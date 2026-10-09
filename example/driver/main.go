// The driver exercises an isolate workflow through the Temporal bindings
// without requiring a running Temporal service.
package main

import (
	"bytes"
	"errors"
	"fmt"
	"isolate"
	"runtime"
	"slices"
	"strings"
	"time"

	"github.com/mfateev/sdk-go-poc/example/cancellation"
	"github.com/mfateev/sdk-go-poc/example/clock"
	"github.com/mfateev/sdk-go-poc/example/concurrent"
	"github.com/mfateev/sdk-go-poc/example/metadata"
	"github.com/mfateev/sdk-go-poc/example/metadata/probe"
	"github.com/mfateev/sdk-go-poc/example/order"
	"github.com/mfateev/sdk-go-poc/example/signal"
	"github.com/mfateev/sdk-go-poc/temporalbridge"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	"go.temporal.io/sdk/converter"
	bindings "go.temporal.io/sdk/internalbindings"
	"go.temporal.io/sdk/temporal"
	goWorkflow "go.temporal.io/sdk/workflow"
	"google.golang.org/protobuf/proto"
)

type environment struct {
	cancel             func()
	canceledActivities int
	bindings.WorkflowEnvironment
	activity  bindings.ResultHandler
	timer     bindings.ResultHandler
	signal    func(string, *commonpb.Payloads, *commonpb.Header) error
	result    *commonpb.Payloads
	completes int
	err       error
	info      goWorkflow.Info
	seq       int64
	now       time.Time
}

func (e *environment) RegisterCancelHandler(handler func()) { e.cancel = handler }
func (e *environment) RequestCancelActivity(_ bindings.ActivityID) {
	e.canceledActivities++
	if e.activity != nil {
		e.activity(nil, temporal.NewCanceledError())
	}
}
func (e *environment) RequestCancelTimer(_ bindings.TimerID) {}

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
func (e *environment) Complete(result *commonpb.Payloads, err error) {
	e.completes++
	e.result, e.err = result, err
}

func definitionFor(fn any) bindings.WorkflowDefinition {
	handle, ok := isolate.LookupFunction(fn)
	if !ok {
		panic("missing marked function")
	}
	return (temporalbridge.Factory{Function: handle, ResolveActivity: func(name string) string {
		if name == "ActivityLength" {
			return "length"
		}
		return name
	}}).NewWorkflowDefinition()
}

func main() {
	runMetadataServices()
	runStructuredFailures()
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
	runInferredActivity()
	runUnsupportedProto()
	runSignal()
	runClock()
	runConcurrent()
	runConcurrentChannelError()
	runRootCancellation()
	runActivityCancellation()
	runChildCancellation()
	runContextStress()
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
			events := strings.Split(stream, "|")
			slices.Sort(events)
			if !slices.Equal(events, []string{"one", "timer", "two"}) {
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

func runConcurrentChannelError() {
	for _, mode := range []string{"activity-error", "decode-error"} {
		d := definitionFor(concurrent.ConcurrentWorkflow)
		e := &concurrentEnvironment{
			environment: environment{now: time.Date(2025, time.January, 2, 0, 0, 0, 0, time.UTC)},
			activities:  make(map[string]bindings.ResultHandler),
		}
		d.Execute(e, nil, nil)
		d.OnWorkflowTaskStarted(5 * time.Second)
		if mode == "activity-error" {
			// Empty error messages must still arrive through FutureResult.Err.
			e.activities["one"](nil, errors.New(""))
		} else {
			payloads, err := e.GetDataConverter().ToPayloads(42)
			if err != nil {
				panic(err)
			}
			e.activities["one"](payloads, nil)
		}
		d.OnWorkflowTaskStarted(5 * time.Second)
		if e.completes != 1 || e.err == nil || mode == "activity-error" && e.err.Error() != "" {
			panic(fmt.Sprintf("channel %s lost failure: completes=%d error=%v", mode, e.completes, e.err))
		}
		// Pending activity/timer callbacks must not revive a completed adapter.
		e.activities["two"](nil, nil)
		e.timer(nil, nil)
		d.OnWorkflowTaskStarted(5 * time.Second)
		d.Close()
		if e.completes != 1 {
			panic("channel error revived completed workflow")
		}
	}
}

func runRootCancellation() {
	d := definitionFor(concurrent.WaitForCancellationWorkflow)
	defer d.Close()
	e := &environment{now: time.Date(2025, time.January, 2, 0, 0, 0, 0, time.UTC)}
	d.Execute(e, nil, nil)
	d.OnWorkflowTaskStarted(5 * time.Second)
	if e.err != nil || e.cancel == nil {
		panic("context wait did not park on host cancellation")
	}
	e.cancel()
	d.OnWorkflowTaskStarted(5 * time.Second)
	if !temporal.IsCanceledError(e.err) {
		panic(fmt.Sprintf("workflow cancellation = %v", e.err))
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
	var failure any
	func() {
		defer func() { failure = recover() }()
		d.OnWorkflowTaskStarted(20 * time.Millisecond)
	}()
	task, ok := failure.(*temporalbridge.WorkflowTaskError)
	if !ok || !strings.Contains(task.Error(), "deadline") || e.completes != 0 {
		panic(fmt.Sprintf("deadline task failure = %v, completes=%d", failure, e.completes))
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
			if scenario == "wrong-type" && !strings.Contains(e.err.Error(), "unable to decode") {
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

// Fake host callbacks prove the function reference schedules work instead of
// calling FormatNumber in the isolate, and that the host alias is applied.
type inferredActivityEnvironment struct{ environment }

func (e *inferredActivityEnvironment) ExecuteActivity(p bindings.ExecuteActivityParams, cb bindings.ResultHandler) bindings.ActivityID {
	if p.ActivityType.Name != "format-number" || p.StartToCloseTimeout != time.Minute || len(p.Input.GetPayloads()) != 1 {
		panic("incorrect inferred activity command")
	}
	var input int
	if err := e.GetDataConverter().FromPayloads(p.Input, &input); err != nil {
		panic(err)
	}
	if input != 5 {
		panic("incorrect inferred activity input")
	}
	e.activity = cb
	return bindings.ActivityID{}
}
func runInferredActivity() {
	for _, scenario := range []string{"success", "failed", "wrong-type"} {
		handle, ok := isolate.LookupFunction(order.InferredActivityWorkflow)
		if !ok {
			panic("missing inferred activity workflow")
		}
		d := (temporalbridge.Factory{Function: handle, ResolveActivity: func(name string) string {
			if name != "FormatNumber" {
				panic("wrong function name")
			}
			return "format-number"
		}}).NewWorkflowDefinition()
		e := &inferredActivityEnvironment{environment: environment{now: time.Date(2025, time.January, 2, 0, 0, 0, 0, time.UTC)}}
		input, err := e.GetDataConverter().ToPayloads(5)
		if err != nil {
			panic(err)
		}
		d.Execute(e, nil, input)
		d.OnWorkflowTaskStarted(5 * time.Second)
		if e.err != nil || e.activity == nil {
			panic("inferred call did not schedule host activity")
		}
		var result any = "host result differs from activity body"
		var cause error
		if scenario == "failed" {
			cause = errors.New("inferred activity failed")
		}
		if scenario == "wrong-type" {
			result = 42
		}
		payloads, err := e.GetDataConverter().ToPayloads(result)
		if err != nil {
			panic(err)
		}
		e.activity(payloads, cause)
		d.OnWorkflowTaskStarted(5 * time.Second)
		if scenario == "success" {
			var got string
			if e.err != nil {
				panic(e.err)
			}
			if err := e.GetDataConverter().FromPayloads(e.result, &got); err != nil {
				panic(err)
			}
			if got != result {
				panic("inferred call used local function result")
			}
		} else if e.err == nil {
			panic("inferred call accepted failed/wrong-type response")
		}
		d.Close()
	}
}

func runActivityCancellation() {
	for _, beforeStart := range []bool{false, true} {
		d := definitionFor(cancellation.ActivityWorkflow)
		e := &cancellationEnvironment{environment: environment{now: time.Date(2025, time.January, 2, 0, 0, 0, 0, time.UTC)}}
		input, err := e.GetDataConverter().ToPayloads("cancel")
		if err != nil {
			panic(err)
		}
		d.Execute(e, nil, input)
		if beforeStart {
			e.cancel()
		}
		d.OnWorkflowTaskStarted(5 * time.Second)
		if !beforeStart {
			if e.activity == nil || e.err != nil {
				panic("cancellable activity not scheduled")
			}
			e.cancel()
			d.OnWorkflowTaskStarted(5 * time.Second)
		}
		if !temporal.IsCanceledError(e.err) {
			panic(fmt.Sprintf("activity workflow cancellation: %v", e.err))
		}
		want := 1
		if beforeStart {
			want = 0
		}
		if e.canceledActivities != want {
			panic(fmt.Sprintf("activity cancel requests=%d, want=%d", e.canceledActivities, want))
		}
		if !beforeStart {
			// A late activity completion must not reply/complete a second time.
			e.activity(input, nil)
			d.OnWorkflowTaskStarted(5 * time.Second)
			if !temporal.IsCanceledError(e.err) {
				panic("late activity response replaced cancellation")
			}
		}
		d.Close()
	}
}

type cancellationEnvironment struct{ environment }

func (e *cancellationEnvironment) ExecuteActivity(p bindings.ExecuteActivityParams, cb bindings.ResultHandler) bindings.ActivityID {
	if p.ActivityType.Name != "WaitActivity" {
		panic("incorrect cancellation activity name")
	}
	e.activity = cb
	return bindings.ActivityID{}
}
func runChildCancellation() {
	for _, scenario := range []struct {
		fn   any
		want string
	}{{cancellation.DeadlineWorkflow, "deadline observed"}, {cancellation.LocalCancelWorkflow, "local cancellation observed"}} {
		d := definitionFor(scenario.fn)
		e := &cancellationEnvironment{environment: environment{now: time.Date(2025, time.January, 2, 0, 0, 0, 0, time.UTC)}}
		d.Execute(e, nil, nil)
		d.OnWorkflowTaskStarted(5 * time.Second)
		if e.activity == nil || e.timer == nil || e.err != nil {
			panic(fmt.Sprintf("child cancellation not scheduled: %v", e.err))
		}
		e.now = e.now.Add(time.Second)
		e.timer(nil, nil)
		d.OnWorkflowTaskStarted(5 * time.Second)
		if e.err != nil {
			panic(e.err)
		}
		var got string
		if err := e.GetDataConverter().FromPayloads(e.result, &got); err != nil {
			panic(err)
		}
		if got != scenario.want || e.canceledActivities != 1 {
			panic(fmt.Sprintf("child cancellation: %q requests=%d", got, e.canceledActivities))
		}
		d.Close()
	}
}
func runContextStress() {
	for _, procs := range []int{1, 2, 8} {
		runtime.GOMAXPROCS(procs)
		for range 5 {
			d := definitionFor(cancellation.ContextStressWorkflow)
			e := &environment{now: time.Date(2025, time.January, 2, 0, 0, 0, 0, time.UTC)}
			d.Execute(e, nil, nil)
			d.OnWorkflowTaskStarted(5 * time.Second)
			if e.err != nil {
				panic(e.err)
			}
			var got int
			if err := e.GetDataConverter().FromPayloads(e.result, &got); err != nil {
				panic(err)
			}
			if got != 64 {
				panic("context callbacks did not all execute")
			}
			d.Close()
		}
	}
}

func runMetadataServices() {
	if err := probe.PrepareCallbackProbe(); err != nil {
		panic(err)
	}
	for _, procs := range []int{1, 2, 8} {
		runtime.GOMAXPROCS(procs)
		for _, mode := range []string{"build", "reject visitor", "reject builder", "reject descriptor", "reject mutation", "reject clone", "private registry", "private message info", "build"} {
			d := definitionFor(metadata.MetadataWorkflow)
			e := &environment{now: time.Date(2025, time.January, 2, 0, 0, 0, 0, time.UTC)}
			input, err := e.GetDataConverter().ToPayloads(mode)
			if err != nil {
				panic(err)
			}
			d.Execute(e, nil, input)
			if strings.HasPrefix(mode, "reject ") {
				var failure any
				func() { defer func() { failure = recover() }(); d.OnWorkflowTaskStarted(5 * time.Second) }()
				fault, ok := failure.(*isolate.EffectError)
				operation := map[string]string{"reject visitor": "RangeMessages", "reject builder": "Builder.Build", "reject descriptor": "metadata callback", "reject mutation": "RegisterMessage", "reject clone": "unaudited protobuf clone type"}[mode]
				if !ok || !strings.Contains(fault.Operation, operation) || !strings.Contains(fault.Stack, "metadata.MetadataWorkflow") || e.completes != 0 || probe.CallbackProbeCount() != 0 {
					panic(fmt.Sprintf("%s task fault: %v completions=%d callbacks=%d", mode, failure, e.completes, probe.CallbackProbeCount()))
				}
				d.Close()
				continue
			}
			if strings.HasPrefix(mode, "private ") {
				var failure any
				func() { defer func() { failure = recover() }(); d.OnWorkflowTaskStarted(5 * time.Second) }()
				taskFailure, ok := failure.(*temporalbridge.WorkflowTaskError)
				var fault *isolate.OwnershipError
				if !ok || !errors.As(taskFailure, &fault) || fault.Reason != "isolate: metadata service requires a process-owned receiver" || !strings.Contains(fault.Stack, "metadata.MetadataWorkflow") || e.completes != 0 {
					panic(fmt.Sprintf("%s ownership task fault: failure=%v completions=%d", mode, failure, e.completes))
				}
				d.Close()
				continue
			}
			d.OnWorkflowTaskStarted(5 * time.Second)
			if e.err != nil {
				panic(e.err)
			}
			var got string
			if err := e.GetDataConverter().FromPayloads(e.result, &got); err != nil {
				panic(err)
			}
			want := "metadata services passed"
			if got != want {
				panic(fmt.Sprintf("metadata workflow: %q, want %q", got, want))
			}
			if probe.CallbackProbeCount() != 0 {
				panic("isolate metadata service invoked host callback")
			}
			d.Close()
		}
	}
}

func runStructuredFailures() {
	dc := converter.GetDefaultDataConverter()
	fc := temporal.NewDefaultFailureConverter(temporal.DefaultFailureConverterOptions{DataConverter: dc})
	details, err := dc.ToPayloads("detail", int64(9007199254740993), []byte("private"))
	if err != nil {
		panic(err)
	}
	heartbeat, err := dc.ToPayloads("heartbeat")
	if err != nil {
		panic(err)
	}
	app := fc.ErrorToFailure(temporal.NewApplicationErrorWithOptions("bad", "invalid", temporal.ApplicationErrorOptions{NonRetryable: true, NextRetryDelay: 3 * time.Second}))
	app.GetApplicationFailureInfo().Details = details
	failure := &failurepb.Failure{Message: "activity failed", FailureInfo: &failurepb.Failure_ActivityFailureInfo{ActivityFailureInfo: &failurepb.ActivityFailureInfo{ActivityId: "activity-id", ActivityType: &commonpb.ActivityType{Name: "echo"}, ScheduledEventId: 3, StartedEventId: 4, RetryState: enumspb.RETRY_STATE_MAXIMUM_ATTEMPTS_REACHED}}, Cause: &failurepb.Failure{Message: "heartbeat timed out", FailureInfo: &failurepb.Failure_TimeoutFailureInfo{TimeoutFailureInfo: &failurepb.TimeoutFailureInfo{TimeoutType: enumspb.TIMEOUT_TYPE_HEARTBEAT, LastHeartbeatDetails: heartbeat}}, Cause: app}}
	for _, procs := range []int{1, 2, 8} {
		runtime.GOMAXPROCS(procs)
		for _, returnFailure := range []bool{false, true} {
			// Two active instances plus host conversion exercise shared type
			// initialization while all message contents remain independently owned.
			var definitions []bindings.WorkflowDefinition
			var environments []*environment
			for range 2 {
				d := definitionFor(metadata.StructuredFailureWorkflow)
				e := &environment{now: time.Date(2025, time.January, 2, 0, 0, 0, 0, time.UTC)}
				input, err := dc.ToPayloads(returnFailure)
				if err != nil {
					panic(err)
				}
				d.Execute(e, nil, input)
				d.OnWorkflowTaskStarted(5 * time.Second)
				definitions, environments = append(definitions, d), append(environments, e)
			}
			for i, d := range definitions {
				e := environments[i]
				e.activity(nil, fc.FailureToError(failure))
				d.OnWorkflowTaskStarted(5 * time.Second)
				var output *failurepb.Failure
				if returnFailure {
					var activity *temporal.ActivityError
					if !errors.As(e.err, &activity) {
						panic(fmt.Sprintf("workflow failure lost SDK type: %v", e.err))
					}
					output = fc.ErrorToFailure(e.err)
				} else {
					if e.err != nil {
						panic(e.err)
					}
					var data []byte
					if err := dc.FromPayloads(e.result, &data); err != nil {
						panic(err)
					}
					output = new(failurepb.Failure)
					if err := proto.Unmarshal(data, output); err != nil {
						panic(err)
					}
				}
				if !proto.Equal(failure, output) {
					panic("structured failure round trip changed protobuf fields")
				}
				d.Close()
			}
		}
	}
}

func (*environment) RegisterUpdateHandler(func(string, string, *commonpb.Payloads, *commonpb.Header, bindings.UpdateCallbacks)) {
}
