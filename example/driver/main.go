// The driver exercises an isolate workflow through the Temporal bindings
// without requiring a running Temporal service.
package main

import (
	"bytes"
	"errors"
	"fmt"
	"isolate"
	"runtime"
	"strings"
	"time"

	"github.com/mfateev/sdk-go-poc/example/clock"
	"github.com/mfateev/sdk-go-poc/example/concurrent"
	"github.com/mfateev/sdk-go-poc/example/order"
	"github.com/mfateev/sdk-go-poc/example/signal"
	"github.com/mfateev/sdk-go-poc/temporalbridge"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/sdk/converter"
	bindings "go.temporal.io/sdk/internalbindings"
	goWorkflow "go.temporal.io/sdk/workflow"
)

type environment struct {
	bindings.WorkflowEnvironment
	activity bindings.ResultHandler
	timer    bindings.ResultHandler
	signal   func(string, *commonpb.Payloads, *commonpb.Header) error
	result   *commonpb.Payloads
	err      error
	info     goWorkflow.Info
	seq      int64
	now      time.Time
}

func (e *environment) RegisterSignalHandler(h func(string, *commonpb.Payloads, *commonpb.Header) error) {
	e.signal = h
}
func (e *environment) GetDataConverter() converter.DataConverter {
	return converter.GetDefaultDataConverter()
}
func (e *environment) WorkflowInfo() *goWorkflow.Info { return &e.info }
func (e *environment) Now() time.Time                 { return e.now }
func (e *environment) GenerateSequence() int64        { e.seq++; return e.seq }
func (e *environment) ExecuteActivity(p bindings.ExecuteActivityParams, cb bindings.ResultHandler) bindings.ActivityID {
	if p.ActivityType.Name != "echo" || p.StartToCloseTimeout != time.Minute {
		panic("unexpected activity")
	}
	e.activity = cb
	return bindings.ActivityID{}
}
func (e *environment) NewTimer(d time.Duration, _ goWorkflow.TimerOptions, cb bindings.ResultHandler) *bindings.TimerID {
	if d != time.Second {
		panic("unexpected timer")
	}
	e.timer = cb
	return nil
}
func (e *environment) Complete(result *commonpb.Payloads, err error) { e.result, e.err = result, err }

func definitionFor(fn any) bindings.WorkflowDefinition {
	handle, ok := isolate.LookupFunction(fn)
	if !ok {
		panic("missing marked function")
	}
	return (temporalbridge.Factory{Function: handle}).NewWorkflowDefinition()
}

func main() {
	d := definitionFor(order.OrderWorkflow)
	e := &environment{now: time.Date(2025, time.January, 2, 0, 0, 0, 0, time.UTC)}
	input, err := e.GetDataConverter().ToPayloads([]byte("hello"))
	if err != nil {
		panic(err)
	}
	d.Execute(e, nil, input)
	d.OnWorkflowTaskStarted(time.Second * 5)
	if e.err != nil {
		panic(e.err)
	}
	if e.activity == nil {
		panic("activity not scheduled")
	}
	if err := e.signal("unused", input, nil); err != nil {
		panic(err)
	}
	d.OnWorkflowTaskStarted(time.Second * 5) // unrelated signal must not fail the parked activity
	e.activity(input, nil)
	d.OnWorkflowTaskStarted(time.Second * 5)
	if e.timer == nil {
		panic("timer not scheduled")
	}
	e.timer(nil, nil)
	e.now = e.now.Add(time.Second)
	d.OnWorkflowTaskStarted(time.Second * 5)
	if e.err != nil {
		panic(e.err)
	}
	var result []byte
	if err := e.GetDataConverter().FromPayloads(e.result, &result); err != nil {
		panic(err)
	}
	if !bytes.Equal(result, []byte("hello")) {
		panic(fmt.Sprintf("result = %q", result))
	}
	d.Close()
	runEcho()
	runTypedEcho()
	runTypedActivity()
	runUnsupportedProto()
	runSignal()
	runClock()
	runConcurrent()
	runDeadlock()
	runDeadline()
	fmt.Println("temporal isolate serial and concurrent paths passed")
}

type concurrentEnvironment struct {
	environment
	activities map[string]bindings.ResultHandler
}

func (e *concurrentEnvironment) ExecuteActivity(p bindings.ExecuteActivityParams, cb bindings.ResultHandler) bindings.ActivityID {
	if p.ActivityType.Name != "echo" || p.StartToCloseTimeout != time.Minute {
		panic("unexpected concurrent activity")
	}
	var input []byte
	if err := e.GetDataConverter().FromPayloads(p.Input, &input); err != nil {
		panic(err)
	}
	e.activities[string(input)] = cb
	return bindings.ActivityID{}
}

func runConcurrent() {
	old := runtime.GOMAXPROCS(1)
	defer runtime.GOMAXPROCS(old)
	var baseline string
	for _, procs := range []int{1, 2, 8} {
		runtime.GOMAXPROCS(procs)
		for repetition := 0; repetition < 10; repetition++ {
			d := definitionFor(concurrent.ConcurrentWorkflow)
			e := &concurrentEnvironment{
				environment: environment{now: time.Date(2025, time.January, 2, 0, 0, 0, 0, time.UTC)},
				activities:  make(map[string]bindings.ResultHandler),
			}
			d.Execute(e, nil, nil)
			d.OnWorkflowTaskStarted(5 * time.Second)
			if e.err != nil {
				panic(e.err)
			}
			if len(e.activities) != 2 || e.timer == nil {
				panic("concurrent commands were missed")
			}
			for _, value := range []string{"two", "one"} {
				payloads, err := e.GetDataConverter().ToPayloads([]byte(value))
				if err != nil {
					panic(err)
				}
				e.activities[value](payloads, nil)
			}
			e.now = e.now.Add(time.Second)
			e.timer(nil, nil)
			d.OnWorkflowTaskStarted(5 * time.Second)
			if e.err != nil {
				panic(e.err)
			}
			var result []byte
			if err := e.GetDataConverter().FromPayloads(e.result, &result); err != nil {
				panic(err)
			}
			stream := string(result)
			if !strings.HasPrefix(stream, "two|") || !strings.Contains(stream, "one") || !strings.Contains(stream, "timer") {
				panic("lost concurrent result: " + stream)
			}
			if baseline == "" {
				baseline = stream
			}
			if stream != baseline {
				panic(fmt.Sprintf("concurrent replay differs at GOMAXPROCS=%d: %q, want %q", procs, stream, baseline))
			}
			d.Close()
		}
	}
}

func runDeadlock() {
	d := definitionFor(concurrent.DeadlockWorkflow)
	defer d.Close()
	e := &environment{now: time.Date(2025, time.January, 2, 0, 0, 0, 0, time.UTC)}
	d.Execute(e, nil, nil)
	d.OnWorkflowTaskStarted(5 * time.Second)
	if e.err == nil || !strings.Contains(e.err.Error(), "deadlocked") {
		panic(fmt.Sprintf("deadlock error = %v", e.err))
	}
}

func runEcho() {
	d := definitionFor(order.EchoWorkflow)
	e := &environment{now: time.Date(2025, time.January, 2, 0, 0, 0, 0, time.UTC)}
	input, err := e.GetDataConverter().ToPayloads([]byte("hello"))
	if err != nil {
		panic(err)
	}
	d.Execute(e, nil, input)
	d.OnWorkflowTaskStarted(5 * time.Second)
	if e.err != nil {
		panic(e.err)
	}
	var result []byte
	if err := e.GetDataConverter().FromPayloads(e.result, &result); err != nil {
		panic(err)
	}
	if !bytes.Equal(result, []byte("registered:hello")) {
		panic(fmt.Sprintf("echo result = %q", result))
	}
	d.Close()
}

func runTypedEcho() {
	d := definitionFor(order.TypedEchoWorkflow)
	e := &environment{now: time.Date(2025, time.January, 2, 0, 0, 0, 0, time.UTC)}
	input, err := e.GetDataConverter().ToPayloads(struct{ Name string }{Name: "world"}, "!")
	if err != nil {
		panic(err)
	}
	d.Execute(e, nil, input)
	d.OnWorkflowTaskStarted(5 * time.Second)
	if e.err != nil {
		panic(e.err)
	}
	var result struct{ Message string }
	if err := e.GetDataConverter().FromPayloads(e.result, &result); err != nil {
		panic(err)
	}
	if result.Message != "hello world!" {
		panic(fmt.Sprintf("typed echo result = %+v", result))
	}
	d.Close()
}

func runUnsupportedProto() {
	d := definitionFor(order.TypedEchoWorkflow)
	e := &environment{now: time.Date(2025, time.January, 2, 0, 0, 0, 0, time.UTC)}
	input, err := e.GetDataConverter().ToPayloads(&commonpb.Payload{Data: []byte("hello")})
	if err != nil {
		panic(err)
	}
	d.Execute(e, nil, input)
	d.OnWorkflowTaskStarted(5 * time.Second)
	if e.err == nil || !strings.Contains(e.err.Error(), "outside the isolate POC subset") || e.result != nil {
		panic(fmt.Sprintf("unsupported proto input result = %v, error = %v", e.result, e.err))
	}
	d.Close()
}

func runClock() {
	d := definitionFor(clock.ClockWorkflow)
	e := &environment{now: time.Date(2025, time.January, 2, 0, 0, 0, 0, time.UTC)}
	d.Execute(e, nil, nil)
	d.OnWorkflowTaskStarted(5 * time.Second)
	if e.err != nil {
		panic(e.err)
	}
	if e.timer == nil {
		panic("clock timer not scheduled")
	}
	e.now = e.now.Add(time.Second)
	e.timer(nil, nil)
	d.OnWorkflowTaskStarted(5 * time.Second)
	if e.err != nil {
		panic(e.err)
	}
	var result []byte
	if err := e.GetDataConverter().FromPayloads(e.result, &result); err != nil {
		panic(err)
	}
	const want = "2025-01-02T00:00:00Z|2025-01-02T00:00:01Z|2025-01-02T00:00:01Z"
	if string(result) != want {
		panic(fmt.Sprintf("clock result = %q, want %q", result, want))
	}
	d.Close()
}

func runSignal() {
	d := definitionFor(signal.SignalWorkflow)
	e := &environment{now: time.Date(2025, time.January, 2, 0, 0, 0, 0, time.UTC)}
	input, err := e.GetDataConverter().ToPayloads([]byte("signal result"))
	if err != nil {
		panic(err)
	}
	d.Execute(e, nil, nil)
	d.OnWorkflowTaskStarted(5 * time.Second)
	if e.signal == nil {
		panic("signal handler not registered")
	}
	if err := e.signal("ready", input, nil); err != nil {
		panic(err)
	}
	d.OnWorkflowTaskStarted(5 * time.Second)
	if e.err != nil {
		panic(e.err)
	}
	var result []byte
	if err := e.GetDataConverter().FromPayloads(e.result, &result); err != nil {
		panic(err)
	}
	if !bytes.Equal(result, []byte("signal result")) {
		panic(fmt.Sprintf("signal result = %q", result))
	}
	d.Close()
}

func runDeadline() {
	d := definitionFor(concurrent.YieldForeverWorkflow)
	defer d.Close()
	e := &environment{now: time.Date(2025, time.January, 2, 0, 0, 0, 0, time.UTC)}
	d.Execute(e, nil, nil)
	d.OnWorkflowTaskStarted(20 * time.Millisecond)
	if e.err == nil || !strings.Contains(e.err.Error(), "deadline") {
		panic(fmt.Sprintf("deadline error = %v", e.err))
	}
}

type typedActivityEnvironment struct {
	environment
	activityName string
}

func (e *typedActivityEnvironment) ExecuteActivity(p bindings.ExecuteActivityParams, cb bindings.ResultHandler) bindings.ActivityID {
	if p.StartToCloseTimeout != time.Minute {
		panic("unexpected activity timeout")
	}
	switch p.ActivityType.Name {
	case "details":
		var name string
		var count int
		if len(p.Input.GetPayloads()) != 2 {
			panic("wrong typed activity arity")
		}
		if err := e.GetDataConverter().FromPayloads(p.Input, &name, &count); err != nil {
			panic(err)
		}
		if name != "world" || count != 3 {
			panic("incorrect typed arguments")
		}
	case "length":
		var value string
		if err := e.GetDataConverter().FromPayloads(p.Input, &value); err != nil {
			panic(err)
		}
		if value != "hello world" {
			panic("incorrect async argument")
		}
	default:
		panic("unexpected typed activity")
	}
	e.activityName, e.activity = p.ActivityType.Name, cb
	return bindings.ActivityID{}
}

func runTypedActivity() {
	for _, scenario := range []string{"success", "failed", "wrong-type"} {
		d := definitionFor(order.TypedActivityWorkflow)
		e := &typedActivityEnvironment{environment: environment{now: time.Date(2025, time.January, 2, 0, 0, 0, 0, time.UTC)}}
		input, err := e.GetDataConverter().ToPayloads("world")
		if err != nil {
			panic(err)
		}
		d.Execute(e, nil, input)
		d.OnWorkflowTaskStarted(5 * time.Second)
		if e.err != nil || e.activityName != "details" {
			panic("typed activity not scheduled")
		}
		result := order.ActivityDetails{Message: "hello world", Count: 3}
		var response any = result
		var cause error
		if scenario == "failed" {
			cause = errors.New("activity failed")
		}
		if scenario == "wrong-type" {
			response = "not a struct"
		}
		payloads, err := e.GetDataConverter().ToPayloads(response)
		if err != nil {
			panic(err)
		}
		e.activity(payloads, cause)
		d.OnWorkflowTaskStarted(5 * time.Second)
		if scenario != "success" {
			if e.err == nil {
				panic("bad activity response accepted")
			}
			if scenario == "failed" && e.err.Error() != "activity failed" {
				panic(e.err)
			}
			if scenario == "wrong-type" && !strings.Contains(e.err.Error(), "decode activity result") {
				panic(e.err)
			}
		} else {
			if e.err != nil || e.activityName != "length" {
				panic("async typed activity not scheduled")
			}
			payloads, err := e.GetDataConverter().ToPayloads(len(result.Message))
			if err != nil {
				panic(err)
			}
			e.activity(payloads, nil)
			d.OnWorkflowTaskStarted(5 * time.Second)
			if e.err != nil {
				panic(e.err)
			}
			var got order.ActivityDetails
			if err := e.GetDataConverter().FromPayloads(e.result, &got); err != nil {
				panic(err)
			}
			if got != result {
				panic(fmt.Sprintf("typed activity result = %+v", got))
			}
		}
		d.Close()
	}
}
