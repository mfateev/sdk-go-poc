package main

import (
	"errors"
	"flag"
	"fmt"
	"isolate"
	"runtime"
	"time"

	"github.com/mfateev/sdk-go-poc/example/child"
	"github.com/mfateev/sdk-go-poc/temporalbridge"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/sdk/converter"
	bindings "go.temporal.io/sdk/internalbindings"
	"go.temporal.io/sdk/log"
	"go.temporal.io/sdk/temporal"
	goWorkflow "go.temporal.io/sdk/workflow"
)

type environment struct {
	bindings.WorkflowEnvironment
	params                                 bindings.ExecuteWorkflowParams
	resultCallback                         bindings.ResultHandler
	startCallback                          func(bindings.WorkflowExecution, error)
	signalCallback                         bindings.ResultHandler
	cancelHandler                          func()
	schedules, cancels, signals, completes int
	result                                 *commonpb.Payloads
	err                                    error
}

func (*environment) RegisterUpdateHandler(func(string, string, *commonpb.Payloads, *commonpb.Header, bindings.UpdateCallbacks)) {
}
func (*environment) RegisterSignalHandler(func(string, *commonpb.Payloads, *commonpb.Header) error) {}
func (e *environment) RegisterCancelHandler(fn func())                                              { e.cancelHandler = fn }
func (*environment) WorkflowInfo() *goWorkflow.Info {
	return &goWorkflow.Info{Namespace: "default", TaskQueueName: "child-check"}
}
func (*environment) Now() time.Time        { return time.Date(2025, 1, 2, 0, 0, 0, 0, time.UTC) }
func (*environment) GetLogger() log.Logger { return nil }
func (*environment) GetDataConverter() converter.DataConverter {
	return converter.GetDefaultDataConverter()
}
func (*environment) IsReplaying() bool { return false }
func (e *environment) Complete(p *commonpb.Payloads, err error) {
	e.completes++
	e.result, e.err = p, err
}
func (e *environment) ExecuteChildWorkflow(p bindings.ExecuteWorkflowParams, result bindings.ResultHandler, started func(bindings.WorkflowExecution, error)) {
	e.schedules++
	e.params, e.resultCallback, e.startCallback = p, result, started
	if p.WorkflowType.Name != "ChildAlias" && p.WorkflowType.Name != "OrdinaryAlias" {
		panic("alias resolution lost")
	}
	if p.Namespace != "default" || p.TaskQueueName != "child-check" || p.WorkflowRunTimeout != 2*time.Minute || p.RetryPolicy.GetMaximumAttempts() != 1 {
		panic("child options lost")
	}
	var precise int64
	raw := p.Memo["precise"].(converter.RawValue).Payload()
	check(e.GetDataConverter().FromPayload(raw, &precise))
	if precise != 9007199254740993 {
		panic("memo precision lost")
	}
	if len(p.SearchAttributes) != 0 {
		check(e.GetDataConverter().FromPayload(p.SearchAttributes["CustomIntField"].(*commonpb.Payload), &precise))
		if precise != 9007199254740993 {
			panic("visibility precision lost")
		}
	}
}
func (e *environment) RequestCancelChildWorkflow(namespace, id string) {
	if namespace != "default" || id != "child-id" {
		panic("wrong cancellation target")
	}
	e.cancels++
	if !e.params.WaitForCancellation {
		e.resultCallback(nil, temporal.NewCanceledError())
	}
}
func (e *environment) SignalExternalWorkflow(namespace, id, runID, name string, input *commonpb.Payloads, _ any, _ *commonpb.Header, childOnly bool, callback bindings.ResultHandler) {
	if namespace != "default" || id != "child-id" || runID != "" || name != "go" || !childOnly {
		panic("wrong child signal target")
	}
	var data []byte
	check(e.GetDataConverter().FromPayloads(input, &data))
	if string(data) != "go" {
		panic("wrong child signal argument")
	}
	e.signals++
	e.signalCallback = callback
}
func check(err error) {
	if err != nil {
		panic(err)
	}
}

func run(mode string) {
	h, ok := isolate.LookupFunction(child.Parent)
	if !ok {
		panic("missing parent handle")
	}
	factory := temporalbridge.Factory{Function: h, ResolveWorkflow: func(name string) string {
		if name == "OrdinaryChild" {
			return "OrdinaryAlias"
		}
		return "ChildAlias"
	}}
	d := factory.NewWorkflowDefinition()
	defer d.Close()
	e := new(environment)
	input, err := e.GetDataConverter().ToPayloads(mode)
	check(err)
	d.Execute(e, nil, input)
	d.OnWorkflowTaskStarted(5 * time.Second)
	if mode == "already-canceled" {
		if e.schedules != 0 || e.completes != 1 {
			panic("pre-canceled child scheduled")
		}
		return
	}
	if e.schedules != 1 {
		panic("child not scheduled before returning")
	}
	if mode == "ignored" || mode == "evict" {
		if mode == "ignored" && e.completes != 1 {
			panic("ignored future blocked parent")
		}
		d.Close()
		e.startCallback(bindings.WorkflowExecution{ID: "child-id", RunID: "run"}, nil)
		e.resultCallback(nil, nil)
		d.OnWorkflowTaskStarted(time.Second)
		if e.cancels != 0 {
			panic("eviction canceled server child")
		}
		return
	}
	if mode == "start-failure" {
		// Cover the old-history path with no initiation-error callback.
		e.resultCallback(nil, &temporal.ChildWorkflowExecutionAlreadyStartedError{})
		d.OnWorkflowTaskStarted(5 * time.Second)
	} else {
		if mode == "cancel-before-start" && e.cancels != 0 {
			panic("canceled child before initiation")
		}
		e.startCallback(bindings.WorkflowExecution{ID: "child-id", RunID: "run"}, nil)
		d.OnWorkflowTaskStarted(5 * time.Second)
		switch mode {
		case "cancel", "cancel-before-start", "cancel-wait":
			if e.cancels != 1 {
				panic("child cancellation not requested exactly once")
			}
			if mode == "cancel-wait" {
				if e.completes != 0 {
					panic("WaitForCancellation ignored")
				}
				e.resultCallback(nil, temporal.NewCanceledError())
				d.OnWorkflowTaskStarted(5 * time.Second)
			}
		default:
			if mode == "signal" {
				if e.signals != 1 || e.completes != 0 {
					panic("signal future resolved early")
				}
				e.signalCallback(nil, nil)
				d.OnWorkflowTaskStarted(5 * time.Second)
			}
			result, err := e.GetDataConverter().ToPayloads(int64(9007199254740993))
			check(err)
			e.resultCallback(result, nil)
			d.OnWorkflowTaskStarted(5 * time.Second)
		}
	}
	check(e.err)
	if e.completes != 1 {
		panic(fmt.Sprintf("%s completed %d times", mode, e.completes))
	}
	var value int64
	check(e.GetDataConverter().FromPayloads(e.result, &value))
	want := int64(9007199254740993)
	if mode == "cancel" || mode == "cancel-before-start" || mode == "cancel-wait" || mode == "start-failure" {
		want = 1
	}
	if value != want {
		panic(fmt.Sprintf("%s result=%d want=%d", mode, value, want))
	}
	// Duplicate/late callbacks must remain inert after completion.
	e.startCallback(bindings.WorkflowExecution{}, errors.New("late"))
	e.resultCallback(nil, errors.New("late"))
	if e.completes != 1 {
		panic("late child callback revived parent")
	}
}

func main() {
	address := flag.String("address", "", "optional live Temporal address")
	historyDir := flag.String("history-dir", "", "saved child histories")
	flag.Parse()
	for _, procs := range []int{1, 2, 8} {
		runtime.GOMAXPROCS(procs)
		for _, mode := range []string{"success", "options", "name", "ordinary", "signal", "cancel", "cancel-before-start", "cancel-wait", "already-canceled", "start-failure", "ignored", "evict"} {
			run(mode)
		}
	}
	fmt.Println("child futures, initiation, cancellation, signals, options and eviction passed")
	if *historyDir != "" {
		check(replayHistories(*historyDir))
	}
	if *address != "" {
		check(checkLive(*address))
	}
}
