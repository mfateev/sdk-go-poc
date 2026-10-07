// Check logging, task failure policy and SDK history replay without a server.
package main

import (
	"fmt"
	"github.com/mfateev/sdk-go-poc/example/effects"
	"github.com/mfateev/sdk-go-poc/temporalbridge"
	"github.com/mfateev/sdk-go-poc/worker"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/sdk/converter"
	bindings "go.temporal.io/sdk/internalbindings"
	sdkworker "go.temporal.io/sdk/worker"
	sdkwf "go.temporal.io/sdk/workflow"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
	"isolate"
	"os"
	"strings"
	"time"
)

type environment struct {
	bindings.WorkflowEnvironment
	replay    bool
	completes int
	result    *commonpb.Payloads
	err       error
}

func (e *environment) GetDataConverter() converter.DataConverter {
	return converter.GetDefaultDataConverter()
}
func (e *environment) RegisterCancelHandler(func()) {}
func (e *environment) RegisterSignalHandler(func(string, *commonpb.Payloads, *commonpb.Header) error) {
}
func (e *environment) Now() time.Time { return time.Date(2025, 1, 2, 0, 0, 0, 0, time.UTC) }
func (e *environment) WorkflowInfo() *sdkwf.Info {
	return &sdkwf.Info{WorkflowExecution: sdkwf.Execution{ID: "id", RunID: "run"}, WorkflowType: sdkwf.Type{Name: "EffectsWorkflow"}}
}
func (e *environment) IsReplaying() bool { return e.replay }
func (e *environment) Complete(p *commonpb.Payloads, err error) {
	e.completes++
	e.result = p
	e.err = err
}

type registrations struct {
	sdkworker.Worker
	factory  temporalbridge.Factory
	ordinary bool
}

func (w *registrations) RegisterWorkflowWithOptions(fn any, _ sdkwf.RegisterOptions) {
	if f, ok := fn.(temporalbridge.Factory); ok {
		w.factory = f
	} else {
		w.ordinary = true
	}
}
func ordinary(sdkwf.Context) error { return nil }
func main() {
	if err := os.WriteFile("effect-sentinel", []byte("host"), 0600); err != nil {
		panic(err)
	}
	defer os.Remove("effect-sentinel")
	for _, replay := range []bool{false, true} {
		for _, mode := range []string{"log", "deny", "metadata"} {
			registration := &registrations{}
			w := worker.Wrap(registration)
			w.RegisterWorkflow(effects.EffectsWorkflow)
			var logs []worker.LogEvent
			if err := worker.SetIsolateLogHandler(w, func(e worker.LogEvent) { logs = append(logs, e) }); err != nil {
				panic(err)
			}
			env := &environment{replay: replay}
			input, err := env.GetDataConverter().ToPayloads(mode)
			if err != nil {
				panic(err)
			}
			d := registration.factory.NewWorkflowDefinition()
			d.Execute(env, nil, input)
			var fault any
			func() { defer func() { fault = recover() }(); d.OnWorkflowTaskStarted(5 * time.Second) }()
			d.Close()
			if len(logs) != 5 {
				panic(fmt.Sprintf("logs=%d mode=%s", len(logs), mode))
			}
			for _, record := range logs {
				if record.Replay != replay || record.WorkflowID != "id" || record.RunID != "run" {
					panic("host log metadata changed")
				}
			}
			if mode != "log" {
				effect, ok := fault.(*isolate.EffectError)
				operation, function := "os.WriteFile", "effects.writeFile"
				if mode == "metadata" {
					operation, function = "unaudited metadata operation", "effects.mutateMetadata"
				}
				if !ok || !strings.Contains(effect.Operation, operation) || !strings.Contains(effect.Stack, function) || env.completes != 0 {
					panic(fmt.Sprintf("task fault=%v completes=%d", fault, env.completes))
				}
			} else {
				if fault != nil || env.completes != 1 || env.err != nil {
					panic(fmt.Sprintf("logging fault=%v completes=%d err=%v", fault, env.completes, env.err))
				}
				var result string
				if err := env.GetDataConverter().FromPayloads(env.result, &result); err != nil || result != "logged" {
					panic("logging changed result")
				}
			}
		}
	}
	registration := &registrations{}
	w := worker.Wrap(registration, worker.Options{WorkflowPanicPolicy: sdkworker.FailWorkflow})
	w.RegisterWorkflow(ordinary)
	if !registration.ordinary {
		panic("ordinary workflow policy changed")
	}
	var rejected bool
	func() { defer func() { rejected = recover() != nil }(); w.RegisterWorkflow(effects.EffectsWorkflow) }()
	if !rejected {
		panic("accepted isolate under FailWorkflow")
	}
	for _, mode := range []string{"log", "deny", "metadata"} {
		r := worker.NewWorkflowReplayer()
		r.RegisterWorkflow(effects.EffectsWorkflow)
		count := 0
		_ = worker.SetIsolateLogHandler(r, func(e worker.LogEvent) {
			if !e.Replay {
				panic("replay record not marked")
			}
			count++
		})
		err := r.ReplayWorkflowHistory(nil, history(mode))
		if mode == "log" {
			if err != nil || count != 5 {
				panic(fmt.Sprintf("SDK logging replay: logs=%d error=%v", count, err))
			}
		} else if err == nil || !strings.Contains(err.Error(), "forbidden operation") || !strings.Contains(err.Error(), map[string]string{"deny": "os.WriteFile", "metadata": "unaudited metadata operation"}[mode]) {
			panic(fmt.Sprintf("SDK effect replay: %v", err))
		}
	}
	data, err := os.ReadFile("effect-sentinel")
	if err != nil || string(data) != "host" {
		panic("forbidden external effect occurred")
	}
	if len(os.Args) == 3 && os.Args[1] == "live" {
		if err := checkLiveServer(os.Args[2]); err != nil {
			panic(err)
		}
	}
	fmt.Println("worker effects, logging and SDK replay passed")
}
func history(mode string) *historypb.History {
	input, _ := converter.GetDefaultDataConverter().ToPayloads(mode)
	result, _ := converter.GetDefaultDataConverter().ToPayloads("logged")
	stamp := timestamppb.New(time.Date(2025, 1, 2, 0, 0, 0, 0, time.UTC))
	return &historypb.History{Events: []*historypb.HistoryEvent{
		{EventId: 1, EventTime: stamp, EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED, Attributes: &historypb.HistoryEvent_WorkflowExecutionStartedEventAttributes{WorkflowExecutionStartedEventAttributes: &historypb.WorkflowExecutionStartedEventAttributes{WorkflowType: &commonpb.WorkflowType{Name: "EffectsWorkflow"}, TaskQueue: &taskqueuepb.TaskQueue{Name: "effects"}, Input: input, WorkflowTaskTimeout: durationpb.New(10 * time.Second), OriginalExecutionRunId: "run", FirstExecutionRunId: "run"}}},
		{EventId: 2, EventTime: stamp, EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED, Attributes: &historypb.HistoryEvent_WorkflowTaskScheduledEventAttributes{WorkflowTaskScheduledEventAttributes: &historypb.WorkflowTaskScheduledEventAttributes{TaskQueue: &taskqueuepb.TaskQueue{Name: "effects"}, StartToCloseTimeout: durationpb.New(10 * time.Second), Attempt: 1}}},
		{EventId: 3, EventTime: stamp, EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_STARTED, Attributes: &historypb.HistoryEvent_WorkflowTaskStartedEventAttributes{WorkflowTaskStartedEventAttributes: &historypb.WorkflowTaskStartedEventAttributes{ScheduledEventId: 2}}},
		{EventId: 4, EventTime: stamp, EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED, Attributes: &historypb.HistoryEvent_WorkflowTaskCompletedEventAttributes{WorkflowTaskCompletedEventAttributes: &historypb.WorkflowTaskCompletedEventAttributes{ScheduledEventId: 2, StartedEventId: 3}}},
		{EventId: 5, EventTime: stamp, EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED, Attributes: &historypb.HistoryEvent_WorkflowExecutionCompletedEventAttributes{WorkflowExecutionCompletedEventAttributes: &historypb.WorkflowExecutionCompletedEventAttributes{Result: result, WorkflowTaskCompletedEventId: 4}}},
	}}
}
