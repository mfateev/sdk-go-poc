// Validate compiled lifecycle handling, SDK replay and optional live history.
package main

import (
	"errors"
	"fmt"
	"isolate"
	"os"
	"strings"
	"time"

	"github.com/mfateev/sdk-go-poc/example/lifecycle"
	"github.com/mfateev/sdk-go-poc/temporalbridge"
	"github.com/mfateev/sdk-go-poc/worker"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/sdk/converter"
	bindings "go.temporal.io/sdk/internalbindings"
	"go.temporal.io/sdk/temporal"
	sdkworker "go.temporal.io/sdk/worker"
	sdkwf "go.temporal.io/sdk/workflow"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type environment struct {
	bindings.WorkflowEnvironment
	cancel          func()
	signal          func(string, *commonpb.Payloads, *commonpb.Header) error
	activity, timer bindings.ResultHandler
	serverCancels   int
	sequence        int64
	completes       int
	result          *commonpb.Payloads
	err             error
}

func (e *environment) GetDataConverter() converter.DataConverter {
	return converter.GetDefaultDataConverter()
}
func (e *environment) RegisterCancelHandler(fn func()) { e.cancel = fn }
func (e *environment) RegisterSignalHandler(fn func(string, *commonpb.Payloads, *commonpb.Header) error) {
	e.signal = fn
}
func (e *environment) WorkflowInfo() *sdkwf.Info { return &sdkwf.Info{TaskQueueName: "lifecycle"} }
func (e *environment) GenerateSequence() int64   { e.sequence++; return e.sequence }
func (e *environment) ExecuteActivity(_ bindings.ExecuteActivityParams, cb bindings.ResultHandler) bindings.ActivityID {
	e.activity = cb
	return bindings.ActivityID{}
}
func (e *environment) NewTimer(_ time.Duration, _ sdkwf.TimerOptions, cb bindings.ResultHandler) *bindings.TimerID {
	e.timer = cb
	return &bindings.TimerID{}
}
func (e *environment) RequestCancelActivity(bindings.ActivityID) { e.serverCancels++ }
func (e *environment) RequestCancelTimer(bindings.TimerID)       { e.serverCancels++ }
func (e *environment) Now() time.Time                            { return time.Date(2025, 1, 2, 0, 0, 0, 0, time.UTC) }
func (e *environment) Complete(p *commonpb.Payloads, err error) {
	e.completes++
	e.result = p
	e.err = err
}

type registrations struct {
	sdkworker.Worker
	factory temporalbridge.Factory
}

func (w *registrations) RegisterWorkflowWithOptions(fn any, _ sdkwf.RegisterOptions) {
	w.factory = fn.(temporalbridge.Factory)
}

func main() {
	checkCachedEviction()
	for _, mode := range []string{"panic", "child", "goexit", "exit", "recover", "children", "error", "cancel"} {
		registration := new(registrations)
		worker.Wrap(registration).RegisterWorkflow(lifecycle.LifecycleWorkflow)
		env := new(environment)
		input, _ := env.GetDataConverter().ToPayloads(mode)
		d := registration.factory.NewWorkflowDefinition()
		d.Execute(env, nil, input)
		var fault any
		func() {
			defer func() { fault = recover() }()
			d.OnWorkflowTaskStarted(5 * time.Second)
			if mode == "cancel" {
				env.cancel()
				d.OnWorkflowTaskStarted(5 * time.Second)
			}
		}()
		d.Close()
		if mode == "panic" || mode == "child" || mode == "goexit" || mode == "exit" {
			task, ok := fault.(*temporalbridge.WorkflowTaskError)
			if !ok || env.completes != 0 {
				panic(fmt.Sprintf("mode=%s task=%v completes=%d", mode, fault, env.completes))
			}
			if mode == "exit" {
				var exited *isolate.ExitError
				if !errors.As(task, &exited) || exited.Code != 17 {
					panic(task)
				}
			}
			if mode == "panic" || mode == "child" {
				var failure *isolate.PanicError
				if !errors.As(task, &failure) || !strings.Contains(failure.Stack, "lifecycle.LifecycleWorkflow") {
					panic(task)
				}
			}
		} else {
			if fault != nil || env.completes != 1 {
				panic(fmt.Sprintf("mode=%s task=%v completes=%d", mode, fault, env.completes))
			}
			if mode == "error" {
				if env.err == nil || !strings.Contains(env.err.Error(), "ordinary workflow error") {
					panic(env.err)
				}
			} else if mode == "cancel" {
				if !temporal.IsCanceledError(env.err) {
					panic(env.err)
				}
			} else {
				var result string
				if env.err != nil || env.GetDataConverter().FromPayloads(env.result, &result) != nil || result != "finished" {
					panic("invalid normal completion")
				}
			}
		}
	}
	for _, mode := range []string{"recover", "children", "panic", "child", "goexit", "exit"} {
		r := worker.NewWorkflowReplayer()
		r.RegisterWorkflow(lifecycle.LifecycleWorkflow)
		err := r.ReplayWorkflowHistory(nil, history(mode))
		if mode == "recover" || mode == "children" {
			if err != nil {
				panic(err)
			}
		} else if err == nil || !strings.Contains(err.Error(), "isolate:") {
			panic(fmt.Sprintf("lost replay task failure: %v", err))
		}
	}
	if len(os.Args) == 3 && os.Args[1] == "live" {
		if err := checkLiveServer(os.Args[2]); err != nil {
			panic(err)
		}
	}
	fmt.Println("compiled lifecycle, cooperative cancellation and SDK replay passed")
}

func checkCachedEviction() {
	registration := new(registrations)
	worker.Wrap(registration).RegisterWorkflow(lifecycle.LifecycleWorkflow)
	env := new(environment)
	input, _ := env.GetDataConverter().ToPayloads("evict")
	d := registration.factory.NewWorkflowDefinition()
	d.Execute(env, nil, input)
	d.OnWorkflowTaskStarted(5 * time.Second)
	if env.activity == nil || env.timer == nil || env.signal == nil || env.completes != 0 {
		panic("eviction fixture did not cache pending work")
	}
	d.Close()
	if err := d.(interface{ CloseError() error }).CloseError(); err != nil {
		panic(err)
	}
	// Keep the definition and all callbacks alive after real instance cleanup.
	env.activity(nil, nil)
	env.timer(nil, nil)
	if err := env.signal("pending", nil, nil); err != nil {
		panic(err)
	}
	env.cancel()
	d.OnWorkflowTaskStarted(time.Second)
	d.Close()
	if env.completes != 0 || env.serverCancels != 0 {
		panic("eviction revived execution or canceled server work")
	}
}

func history(mode string) *historypb.History {
	input, _ := converter.GetDefaultDataConverter().ToPayloads(mode)
	result, _ := converter.GetDefaultDataConverter().ToPayloads("finished")
	stamp := timestamppb.New(time.Date(2025, 1, 2, 0, 0, 0, 0, time.UTC))
	return &historypb.History{Events: []*historypb.HistoryEvent{
		{EventId: 1, EventTime: stamp, EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED, Attributes: &historypb.HistoryEvent_WorkflowExecutionStartedEventAttributes{WorkflowExecutionStartedEventAttributes: &historypb.WorkflowExecutionStartedEventAttributes{WorkflowType: &commonpb.WorkflowType{Name: "LifecycleWorkflow"}, TaskQueue: &taskqueuepb.TaskQueue{Name: "lifecycle"}, Input: input, WorkflowTaskTimeout: durationpb.New(10 * time.Second), OriginalExecutionRunId: "run", FirstExecutionRunId: "run"}}},
		{EventId: 2, EventTime: stamp, EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED, Attributes: &historypb.HistoryEvent_WorkflowTaskScheduledEventAttributes{WorkflowTaskScheduledEventAttributes: &historypb.WorkflowTaskScheduledEventAttributes{TaskQueue: &taskqueuepb.TaskQueue{Name: "lifecycle"}, StartToCloseTimeout: durationpb.New(10 * time.Second), Attempt: 1}}},
		{EventId: 3, EventTime: stamp, EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_STARTED, Attributes: &historypb.HistoryEvent_WorkflowTaskStartedEventAttributes{WorkflowTaskStartedEventAttributes: &historypb.WorkflowTaskStartedEventAttributes{ScheduledEventId: 2}}},
		{EventId: 4, EventTime: stamp, EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED, Attributes: &historypb.HistoryEvent_WorkflowTaskCompletedEventAttributes{WorkflowTaskCompletedEventAttributes: &historypb.WorkflowTaskCompletedEventAttributes{ScheduledEventId: 2, StartedEventId: 3}}},
		{EventId: 5, EventTime: stamp, EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED, Attributes: &historypb.HistoryEvent_WorkflowExecutionCompletedEventAttributes{WorkflowExecutionCompletedEventAttributes: &historypb.WorkflowExecutionCompletedEventAttributes{Result: result, WorkflowTaskCompletedEventId: 4}}},
	}}
}

func (*environment) RegisterUpdateHandler(func(string, string, *commonpb.Payloads, *commonpb.Header, bindings.UpdateCallbacks)) {
}
func (*environment) RegisterQueryHandler(func(string, *commonpb.Payloads, *commonpb.Header) (*commonpb.Payloads, error)) {
}
