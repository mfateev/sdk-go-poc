package main

import (
	"errors"
	"flag"
	"fmt"
	"isolate"
	"runtime"
	"time"

	"github.com/mfateev/sdk-go-poc/example/apicoverage"
	"github.com/mfateev/sdk-go-poc/temporalbridge"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/converter"
	bindings "go.temporal.io/sdk/internalbindings"
	"go.temporal.io/sdk/log"
	"go.temporal.io/sdk/temporal"
	goWorkflow "go.temporal.io/sdk/workflow"
)

type environment struct {
	bindings.WorkflowEnvironment
	mode                                                                                                                     string
	now                                                                                                                      time.Time
	sequence                                                                                                                 int64
	signal                                                                                                                   func(string, *commonpb.Payloads, *commonpb.Header) error
	creation                                                                                                                 bindings.ResultHandler
	nexusResult                                                                                                              func(*commonpb.Payload, error)
	queued                                                                                                                   []func()
	result                                                                                                                   *commonpb.Payloads
	err                                                                                                                      error
	completed, localSchedules, timers, sessionAdds, sessionRemoves, externalSignals, externalCancels, nexusCancels, abandons int
}

func (*environment) RegisterUpdateHandler(func(string, string, *commonpb.Payloads, *commonpb.Header, bindings.UpdateCallbacks)) {
}
func (*environment) RegisterQueryHandler(func(string, *commonpb.Payloads, *commonpb.Header) (*commonpb.Payloads, error)) {
}
func (*environment) RegisterCancelHandler(func()) {}
func (e *environment) RegisterSignalHandler(fn func(string, *commonpb.Payloads, *commonpb.Header) error) {
	e.signal = fn
}
func (e *environment) WorkflowInfo() *goWorkflow.Info {
	return &goWorkflow.Info{Namespace: "default", TaskQueueName: "coverage", WorkflowExecution: goWorkflow.Execution{ID: "workflow", RunID: "run"}, OriginalRunID: "run"}
}
func (e *environment) Now() time.Time      { return e.now }
func (*environment) GetLogger() log.Logger { return nil }
func (*environment) GetDataConverter() converter.DataConverter {
	return converter.GetDefaultDataConverter()
}
func (*environment) IsReplaying() bool         { return false }
func (e *environment) GenerateSequence() int64 { e.sequence++; return e.sequence }
func (e *environment) Complete(p *commonpb.Payloads, err error) {
	e.completed++
	e.result, e.err = p, err
}
func payload(values ...any) *commonpb.Payloads {
	p, err := converter.GetDefaultDataConverter().ToPayloads(values...)
	must(err)
	return p
}
func (e *environment) ExecuteActivity(p bindings.ExecuteActivityParams, cb bindings.ResultHandler) bindings.ActivityID {
	switch p.ActivityType.Name {
	case "internalSessionCreationActivity":
		var id string
		must(e.GetDataConverter().FromPayloads(p.Input, &id))
		if p.TaskQueueName != "coverage__internal_session_creation" && p.TaskQueueName != "resource@host" {
			panic("session creation queue lost")
		}
		e.creation = cb
		must(e.signal(id, payload(struct{ Taskqueue, HostName, ResourceID string }{"resource@host", "host", "resource"}), nil))
		if e.mode == "session-failure" {
			e.queued = append(e.queued, func() { cb(nil, temporal.NewTimeoutError(enumspb.TIMEOUT_TYPE_HEARTBEAT, nil)) })
		}
	case "internalSessionCompletionActivity":
		if p.TaskQueueName != "resource@host" {
			panic("session completion queue lost")
		}
		cb(nil, nil)
	default:
		if p.TaskQueueName != "resource@host" {
			panic("session user activity queue lost")
		}
		cb(payload(p.TaskQueueName), nil)
	}
	return bindings.ActivityID{}
}
func (e *environment) RequestCancelActivity(bindings.ActivityID) {
	if e.creation != nil {
		e.creation(nil, temporal.NewCanceledError())
	}
}
func (e *environment) AddSession(*goWorkflow.SessionInfo) { e.sessionAdds++ }
func (e *environment) RemoveSession(string)               { e.sessionRemoves++ }
func (e *environment) ExecuteLocalActivity(p bindings.ExecuteLocalActivityParams, cb bindings.LocalActivityResultHandler) bindings.LocalActivityID {
	e.localSchedules++
	if p.InputArgs[1] != apicoverage.Precise || p.ActivityType != "LocalAlias" {
		panic("local arguments or alias lost")
	}
	if e.mode == "local-cancel" {
		e.queued = append(e.queued, func() { cb(&bindings.LocalActivityResultWrapper{Err: temporal.NewCanceledError()}) })
		return bindings.LocalActivityID{}
	}
	if e.mode == "local-retry" && p.Attempt == 1 {
		cb(&bindings.LocalActivityResultWrapper{Err: temporal.NewApplicationError("retry", "LocalRetry"), Attempt: 1, Backoff: 2 * time.Second})
	} else if e.mode == "local-failure" {
		cb(&bindings.LocalActivityResultWrapper{Err: temporal.NewNonRetryableApplicationError("rejected", "LocalFailure", nil, apicoverage.Precise)})
	} else {
		cb(&bindings.LocalActivityResultWrapper{Result: payload(apicoverage.Precise)})
	}
	return bindings.LocalActivityID{}
}
func (*environment) RequestCancelLocalActivity(bindings.LocalActivityID) {}
func (e *environment) NewTimer(duration time.Duration, _ goWorkflow.TimerOptions, cb bindings.ResultHandler) *bindings.TimerID {
	e.timers++
	e.now = e.now.Add(duration)
	cb(nil, nil)
	return nil
}
func (e *environment) SignalExternalWorkflow(ns, id, run, name string, p *commonpb.Payloads, _ any, _ *commonpb.Header, child bool, cb bindings.ResultHandler) {
	if ns != "default" || id != "target" || name != "precise" || child {
		panic("wrong external target")
	}
	var value int64
	must(e.GetDataConverter().FromPayloads(p, &value))
	if value != apicoverage.Precise {
		panic("external argument precision")
	}
	e.externalSignals++
	if e.mode == "external-failure" {
		cb(nil, &temporal.UnknownExternalWorkflowExecutionError{})
	} else {
		cb(nil, nil)
	}
}
func (e *environment) RequestCancelExternalWorkflow(ns, id, run string, cb bindings.ResultHandler) {
	e.externalCancels++
	if e.mode == "external-failure" {
		cb(nil, &temporal.UnknownExternalWorkflowExecutionError{})
	} else {
		cb(nil, nil)
	}
}
func (e *environment) ExecuteNexusOperation(_ bindings.ExecuteNexusOperationParams, cb func(*commonpb.Payload, error), started func(string, error)) int64 {
	e.nexusResult = cb
	if e.mode == "nexus-failure" {
		cb(nil, &temporal.NexusOperationError{Endpoint: "endpoint", Service: "coverage", Operation: "failure"})
		return 1
	}
	if e.mode == "nexus" {
		cb(payload(apicoverage.Precise).Payloads[0], nil)
		started("", nil)
	} else {
		started("token", nil)
		if e.mode == "nexus-async" {
			e.queued = append(e.queued, func() { cb(payload(apicoverage.Precise).Payloads[0], nil) })
		}
	}
	return 1
}
func (e *environment) RequestCancelNexusOperation(int64) {
	e.nexusCancels++
	e.nexusResult(nil, temporal.NewCanceledError())
}
func (e *environment) AbandonNexusOperation(int64) { e.abandons++ }
func must(err error) {
	if err != nil {
		panic(err)
	}
}
func runFake(mode string) {
	h, ok := isolate.LookupFunction(apicoverage.Coverage)
	if !ok {
		panic("workflow handle missing")
	}
	d := (temporalbridge.Factory{Function: h, ResolveActivity: func(name string) string { return "LocalAlias" }, ResolveLocalActivity: func(string) any { return apicoverage.Local }}).NewWorkflowDefinition()
	defer d.Close()
	e := &environment{mode: mode, now: time.Date(2025, 1, 2, 0, 0, 0, 0, time.UTC)}
	d.Execute(e, nil, payload(apicoverage.Input{Mode: mode, Target: "target", Endpoint: "endpoint"}))
	for i := 0; i < 20 && e.completed == 0; i++ {
		d.OnWorkflowTaskStarted(5 * time.Second)
		queued := e.queued
		e.queued = nil
		for _, fn := range queued {
			fn()
		}
	}
	must(e.err)
	if e.completed != 1 {
		panic(fmt.Sprintf("%s did not complete", mode))
	}
	var result string
	must(e.GetDataConverter().FromPayloads(e.result, &result))
	if result != "ok" {
		panic("wrong result")
	}
	if mode == "local-retry" && (e.localSchedules != 2 || e.timers != 1) {
		panic("durable local backoff lost")
	}
	if mode == "session" && (e.sessionAdds != 1 || e.sessionRemoves != 1) {
		panic("session metadata lifecycle lost")
	}
	if mode == "session-recreate" && (e.sessionAdds != 2 || e.sessionRemoves != 2) {
		panic("session recreation lifecycle lost")
	}
	if mode == "nexus-abandon" && (e.abandons != 1 || e.nexusCancels != 0) {
		panic("Nexus abandon policy lost")
	}
	d.Close()
	if e.nexusResult != nil {
		e.nexusResult(nil, errors.New("late"))
	}
	if e.creation != nil {
		e.creation(nil, errors.New("late"))
	}
}
func main() {
	address := flag.String("address", "", "optional Temporal server")
	histories := flag.String("history-dir", "", "saved histories to replay (also live output directory)")
	flag.Parse()
	for _, procs := range []int{1, 2, 8} {
		runtime.GOMAXPROCS(procs)
		for _, mode := range []string{"external", "external-failure", "local", "local-retry", "local-failure", "local-cancel", "session", "session-recreate", "session-failure", "nexus", "nexus-async", "nexus-failure", "nexus-cancel", "nexus-abandon", "nexus-try-cancel", "nexus-wait-requested"} {
			runFake(mode)
		}
	}
	fmt.Println("native API coverage checks passed at GOMAXPROCS 1/2/8")
	if *address != "" {
		must(runLive(*address, *histories))
	} else if *histories != "" {
		for _, procs := range []int{1, 2, 8} {
			runtime.GOMAXPROCS(procs)
			must(replayDirectory(*histories))
		}
	}
}
