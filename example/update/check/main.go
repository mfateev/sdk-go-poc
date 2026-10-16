package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"isolate"
	"runtime"
	"slices"
	"strings"
	"time"

	"github.com/mfateev/sdk-go-poc/example/update"
	"github.com/mfateev/sdk-go-poc/temporalbridge"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/sdk/converter"
	bindings "go.temporal.io/sdk/internalbindings"
	"go.temporal.io/sdk/log"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
	goWorkflow "go.temporal.io/sdk/workflow"
)

type outcome struct {
	accepted, rejected, completed int
	result                        *commonpb.Payloads
	err                           error
}

func (o *outcome) Accept()          { o.accepted++ }
func (o *outcome) Reject(err error) { o.rejected++; o.err = err }
func (o *outcome) Complete(value any, err error) {
	o.completed++
	o.err = err
	if err == nil {
		o.result, o.err = converter.GetDefaultDataConverter().ToPayloads(value)
	}
}

type environment struct {
	bindings.WorkflowEnvironment
	query      func(string, *commonpb.Payloads, *commonpb.Header) (*commonpb.Payloads, error)
	update     func(string, string, *commonpb.Payloads, *commonpb.Header, bindings.UpdateCallbacks)
	signal     func(string, *commonpb.Payloads, *commonpb.Header) error
	cancel     func()
	timers     []bindings.ResultHandler
	activities []bindings.ResultHandler
	sequence   int64
	replaying  bool
	completes  int
	result     *commonpb.Payloads
	err        error
}

func (*environment) RegisterResetHandler(func(bindings.WorkflowResetInfo)) {}
func (e *environment) RegisterCancelHandler(fn func())                     { e.cancel = fn }
func (e *environment) RegisterSignalHandler(fn func(string, *commonpb.Payloads, *commonpb.Header) error) {
	e.signal = fn
}
func (e *environment) RegisterQueryHandler(fn func(string, *commonpb.Payloads, *commonpb.Header) (*commonpb.Payloads, error)) {
	e.query = fn
}
func (e *environment) RegisterUpdateHandler(fn func(string, string, *commonpb.Payloads, *commonpb.Header, bindings.UpdateCallbacks)) {
	e.update = fn
}
func (*environment) WorkflowInfo() *goWorkflow.Info {
	return &goWorkflow.Info{TaskQueueName: "update-check"}
}
func (*environment) Now() time.Time { return time.Date(2025, 1, 2, 0, 0, 0, 0, time.UTC) }
func (*environment) GetDataConverter() converter.DataConverter {
	return converter.GetDefaultDataConverter()
}
func (*environment) GetLogger() log.Logger     { return nil }
func (e *environment) IsReplaying() bool       { return e.replaying }
func (e *environment) GenerateSequence() int64 { e.sequence++; return e.sequence }
func (e *environment) ExecuteActivity(_ bindings.ExecuteActivityParams, callback bindings.ResultHandler) bindings.ActivityID {
	e.activities = append(e.activities, callback)
	return bindings.ActivityID{}
}
func (e *environment) NewTimer(d time.Duration, _ goWorkflow.TimerOptions, callback bindings.ResultHandler) *bindings.TimerID {
	if d != time.Second {
		panic("unexpected update timer")
	}
	e.timers = append(e.timers, callback)
	return nil
}
func (e *environment) Complete(result *commonpb.Payloads, err error) {
	e.completes++
	e.result, e.err = result, err
}
func check(err error) {
	if err != nil {
		panic(err)
	}
}
func (e *environment) send(name, id string, args ...any) *outcome {
	input, err := e.GetDataConverter().ToPayloads(args...)
	check(err)
	o := new(outcome)
	e.update(name, id, input, nil, o)
	return o
}
func (e *environment) state(expected int64) {
	result, err := e.query("state", nil, nil)
	check(err)
	var value int64
	check(e.GetDataConverter().FromPayloads(result, &value))
	if value != expected {
		panic(fmt.Sprintf("state=%d, expected=%d", value, expected))
	}
}
func (e *environment) finished(expected bool) {
	result, err := e.query("finished", nil, nil)
	check(err)
	var value bool
	check(e.GetDataConverter().FromPayloads(result, &value))
	if value != expected {
		panic("AllHandlersFinished mismatch")
	}
}
func result(o *outcome, expected int64) {
	check(o.err)
	if o.accepted != 1 || o.completed != 1 || o.rejected != 0 {
		panic(fmt.Sprintf("update lifecycle=%+v", o))
	}
	var value int64
	check(converter.GetDefaultDataConverter().FromPayloads(o.result, &value))
	if value != expected {
		panic(fmt.Sprintf("update result=%d, expected=%d", value, expected))
	}
}
func main() {
	address := flag.String("address", "", "optional Temporal server")
	devServer := flag.Bool("dev-server", false, "start a local Temporal server for live update and replay checks")
	history := flag.String("history", "", "saved update history to replay")
	flag.Parse()
	for _, procs := range []int{1, 2, 8} {
		runtime.GOMAXPROCS(procs)
		run()
		runWait()
		runOrdering()
		runCancellationOrdering()
	}
	fmt.Println("priority updates, goroutine ordering, read-only validators, cancellation and replay validation skipping passed")
	if *history != "" {
		check(checkHistory(*history))
	}
	if *address != "" {
		check(checkLive(*address))
	}
	if *devServer {
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		server, err := testsuite.StartDevServer(ctx, testsuite.DevServerOptions{})
		check(err)
		defer func() { check(server.Stop()) }()
		check(checkLive(server.FrontendHostPort()))
	}
}

func runCancellationOrdering() {
	h, ok := isolate.LookupFunction(update.CancellationOrderingWorkflow)
	if !ok {
		panic("missing cancellation ordering workflow handle")
	}
	e := new(environment)
	d := (temporalbridge.Factory{Function: h}).NewWorkflowDefinition()
	defer d.Close()
	d.Execute(e, nil, nil)
	d.OnWorkflowTaskStarted(5 * time.Second)
	e.cancel()
	o := e.send("check-cancellation", "canceled")
	payloads, err := e.GetDataConverter().ToPayloads(true)
	check(err)
	check(e.signal("finish", payloads, nil))
	d.OnWorkflowTaskStarted(5 * time.Second)
	check(o.err)
	check(e.err)
	var sawCancellation bool
	check(e.GetDataConverter().FromPayloads(e.result, &sawCancellation))
	if e.completes != 1 || o.accepted != 1 || o.completed != 1 || !sawCancellation {
		panic("priority handler ran before cancellation delivery")
	}
}

func runOrdering() {
	h, ok := isolate.LookupFunction(update.OrderingWorkflow)
	if !ok {
		panic("missing ordering workflow handle")
	}
	for _, mode := range []string{"registration", "validators", "main-timer", "main-activity", "signal", "handler-timer", "handler-activity", "mixed-wakeup", "mixed-channel", "shared-future", "native-timer", "child", "channel"} {
		e := new(environment)
		d := (temporalbridge.Factory{Function: h}).NewWorkflowDefinition()
		input, err := e.GetDataConverter().ToPayloads(mode)
		check(err)
		d.Execute(e, nil, input)
		var outcomes []*outcome
		var expected []string
		switch mode {
		case "registration", "validators":
			outcomes = append(outcomes, e.send("record", "first", "first"))
			expected = []string{"first", "main"}
			if mode == "validators" {
				outcomes = append(outcomes, e.send("record", "second", "second"))
				expected = []string{"first", "second", "main"}
			}
		default:
			d.OnWorkflowTaskStarted(5 * time.Second)
			if mode == "main-timer" {
				// Queue the root's timer wakeup before the update arrives.
				e.timers[0](nil, nil)
			} else if mode == "main-activity" {
				e.activities[0](nil, nil)
			} else if mode != "handler-timer" && mode != "handler-activity" && mode != "mixed-wakeup" && mode != "mixed-channel" && mode != "shared-future" && mode != "native-timer" {
				payloads, err := e.GetDataConverter().ToPayloads(true)
				check(err)
				check(e.signal("finish", payloads, nil))
			}
			outcomes = append(outcomes, e.send("record", "record", "update"))
			expected = []string{"update", "main"}
			if mode == "handler-timer" || mode == "handler-activity" || mode == "mixed-wakeup" || mode == "mixed-channel" || mode == "shared-future" || mode == "native-timer" {
				d.OnWorkflowTaskStarted(5 * time.Second)
				if mode == "mixed-wakeup" || mode == "mixed-channel" || mode == "shared-future" || mode == "native-timer" {
					// The root's direct timer reply precedes the activity reply
					// consumed by the update's SDK future delivery goroutine.
					e.timers[0](nil, nil)
				} else {
					payloads, err := e.GetDataConverter().ToPayloads(true)
					check(err)
					check(e.signal("finish", payloads, nil))
				}
				if mode == "native-timer" {
					e.timers[1](nil, nil)
				} else if mode == "handler-timer" {
					e.timers[0](nil, nil)
				} else {
					e.activities[0](nil, nil)
				}
				expected = []string{"update", "resumed", "main"}
			} else if mode == "child" {
				expected = []string{"update", "main", "child", "resumed"}
			} else if mode == "channel" {
				expected = []string{"update", "main-before-yield", "resumed", "helper", "main-after-yield"}
			}
		}
		d.OnWorkflowTaskStarted(5 * time.Second)
		if e.completes != 1 {
			panic(fmt.Sprintf("%s ordering workflow did not finish", mode))
		}
		check(e.err)
		var actual []string
		check(e.GetDataConverter().FromPayloads(e.result, &actual))
		if !slices.Equal(actual, expected) {
			panic(fmt.Sprintf("%s ordering: got %v, expected %v", mode, actual, expected))
		}
		for _, o := range outcomes {
			check(o.err)
			if o.accepted != 1 || o.completed != 1 || o.rejected != 0 {
				panic(fmt.Sprintf("%s lifecycle: %+v", mode, o))
			}
		}
		d.Close()
	}
}

func runWait() {
	h, ok := isolate.LookupFunction(update.WaitWorkflow)
	if !ok {
		panic("missing waiter workflow handle")
	}
	for _, mode := range []string{"empty", "pending", "canceled"} {
		e := new(environment)
		d := (temporalbridge.Factory{Function: h}).NewWorkflowDefinition()
		d.Execute(e, nil, nil)
		d.OnWorkflowTaskStarted(5 * time.Second)
		var pending *outcome
		if mode != "empty" {
			pending = e.send("delayed", "pending", int64(7))
			d.OnWorkflowTaskStarted(5 * time.Second)
			stack := d.StackTrace()
			if !strings.Contains(stack, "update.WaitWorkflow(") || !strings.Contains(stack, "workflow.executeUpdate(") {
				panic("workflow stack omitted running update handler: " + stack)
			}
			if strings.Contains(stack, "workflow.serveUpdates()") || strings.Contains(stack, "workflow.serveQueries()") {
				panic("workflow stack included SDK services: " + stack)
			}
		}
		finish, err := e.GetDataConverter().ToPayloads(nil)
		check(err)
		check(e.signal("finish", finish, nil))
		d.OnWorkflowTaskStarted(5 * time.Second)
		if mode != "empty" {
			if e.completes != 0 || pending.completed != 0 {
				panic("waiter finished before its update")
			}
			if mode == "canceled" {
				e.cancel()
			} else {
				e.timers[0](nil, nil)
			}
			d.OnWorkflowTaskStarted(5 * time.Second)
		}
		if e.completes != 1 {
			panic("waiter did not complete")
		}
		if mode == "canceled" {
			if !temporal.IsCanceledError(e.err) {
				panic("waiter lost cancellation")
			}
		} else {
			check(e.err)
			var count int64
			check(e.GetDataConverter().FromPayloads(e.result, &count))
			if mode == "pending" {
				result(pending, 7)
				if count != 7 {
					panic("waiter lost update result")
				}
			} else if count != 0 {
				panic("empty waiter result")
			}
		}
		d.Close()
	}
}
func run() {
	h, ok := isolate.LookupFunction(update.UpdateWorkflow)
	if !ok {
		panic("missing workflow handle")
	}
	e := new(environment)
	d := (temporalbridge.Factory{Function: h}).NewWorkflowDefinition()
	defer d.Close()
	input, err := e.GetDataConverter().ToPayloads(int64(10))
	check(err)
	d.Execute(e, nil, input)
	// Updates delivered before initial registration must yield to their own
	// handler registrations, including a name registered after the first one.
	first := e.send("add", "first", int64(2))
	second := e.send("error-only", "second", int64(3))
	d.OnWorkflowTaskStarted(5 * time.Second)
	result(first, 12)
	check(second.err)
	if second.accepted != 1 || second.completed != 1 {
		panic("error-only update did not complete")
	}
	e.state(15)
	e.finished(true)
	for _, mode := range []string{"exit", "goexit", "environment", "mutex", "waitgroup", "cond", "field", "map", "delete", "clear", "slice", "copy", "package", "atomic", "reflect", "callback", "error-callback", "goroutine", "channel", "select", "activity", "child", "timer", "raw", "panic", "error"} {
		o := e.send("bad", "bad-"+mode, mode)
		d.OnWorkflowTaskStarted(5 * time.Second)
		if o.rejected != 1 || o.accepted != 0 || o.completed != 0 || o.err == nil {
			panic("invalid validator accepted: " + mode)
		}
		if mode == "field" || mode == "map" || mode == "panic" {
			var p *temporal.PanicError
			if !errors.As(o.err, &p) {
				panic("validator panic type lost: " + o.err.Error())
			}
		}
		e.state(15)
	}
	negative := e.send("add", "negative", int64(-7))
	d.OnWorkflowTaskStarted(5 * time.Second)
	var app *temporal.ApplicationError
	if negative.rejected != 1 || !errors.As(negative.err, &app) || app.Type() != "Validation" {
		panic("validator error type lost")
	}
	var details int64
	check(app.Details(&details))
	if details != -7 {
		panic("validator details lost")
	}
	missing := e.send("unknown", "unknown", int64(1))
	d.OnWorkflowTaskStarted(5 * time.Second)
	if missing.rejected != 1 || !strings.Contains(missing.err.Error(), "KnownUpdates") {
		panic("unknown update not rejected")
	}
	malformed := e.send("add", "malformed", "wrong type")
	d.OnWorkflowTaskStarted(5 * time.Second)
	if malformed.rejected != 1 {
		panic("malformed args accepted")
	}
	failed := e.send("failure", "failure", int64(0))
	d.OnWorkflowTaskStarted(5 * time.Second)
	app = nil
	if failed.completed != 1 || failed.accepted != 1 || !errors.As(failed.err, &app) || app.Type() != "HandlerFailure" {
		panic("handler failure type lost")
	}
	delayed := e.send("delayed", "delayed", int64(4))
	quick := e.send("add", "quick", int64(5))
	d.OnWorkflowTaskStarted(5 * time.Second)
	result(quick, 20)
	if delayed.accepted != 1 || delayed.completed != 0 {
		panic("delayed update did not suspend")
	}
	e.state(20)
	e.finished(false)
	e.timers[0](nil, nil)
	d.OnWorkflowTaskStarted(5 * time.Second)
	result(delayed, 24)
	e.finished(true)
	large := e.send("add", "large", int64(9007199254740993))
	d.OnWorkflowTaskStarted(5 * time.Second)
	result(large, 9007199254741017)
	e.state(9007199254741017)
	canceled := e.send("cancellation", "cancel")
	d.OnWorkflowTaskStarted(5 * time.Second)
	e.cancel()
	d.OnWorkflowTaskStarted(5 * time.Second)
	if canceled.accepted != 1 || canceled.completed != 1 || !temporal.IsCanceledError(canceled.err) {
		panic("native update context was not canceled")
	}
	e.state(9007199254741017)
	e.finished(true)
	finish, err := e.GetDataConverter().ToPayloads([]byte("finish"))
	check(err)
	check(e.signal("finish", finish, nil))
	d.OnWorkflowTaskStarted(5 * time.Second)
	if e.completes != 1 {
		panic("workflow did not complete")
	}
	check(e.err)
	e.state(9007199254741017)
	// Replay must skip a now-invalid validator for a historically accepted
	// update, while still decoding arguments and running the handler.
	e2 := &environment{replaying: true}
	d2 := (temporalbridge.Factory{Function: h}).NewWorkflowDefinition()
	defer d2.Close()
	d2.Execute(e2, nil, input)
	d2.OnWorkflowTaskStarted(5 * time.Second)
	replayed := e2.send("add", "replay", int64(-2))
	d2.OnWorkflowTaskStarted(5 * time.Second)
	result(replayed, 8)
	e2.state(8)
}
