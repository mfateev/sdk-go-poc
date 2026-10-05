// The driver exercises an isolate workflow through the Temporal bindings
// without requiring a running Temporal service.
package main

import (
	"bytes"
	"fmt"
	"isolate"
	"strings"
	"time"

	"github.com/mfateev/sdk-go-poc/example/clock"
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
	runUnsupportedProto()
	runSignal()
	runClock()
	fmt.Println("temporal isolate serial path passed")
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
