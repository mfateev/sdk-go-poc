package main

import (
	"errors"
	"flag"
	"fmt"
	"isolate"
	"runtime"
	"strings"
	"time"

	"github.com/mfateev/sdk-go-poc/example/upstream"
	"github.com/mfateev/sdk-go-poc/temporalbridge"
	"github.com/mfateev/sdk-go-poc/worker"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/sdk/converter"
	bindings "go.temporal.io/sdk/internalbindings"
	"go.temporal.io/sdk/log"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
	goWorker "go.temporal.io/sdk/worker"
	goWorkflow "go.temporal.io/sdk/workflow"
)

type timer struct {
	due      time.Time
	callback bindings.ResultHandler
	canceled bool
}
type environment struct {
	bindings.WorkflowEnvironment
	now       time.Time
	timers    []*timer
	completed int
	sequence  int64
	result    *commonpb.Payloads
	err       error
}

func (*environment) RegisterUpdateHandler(func(string, string, *commonpb.Payloads, *commonpb.Header, bindings.UpdateCallbacks)) {
}
func (*environment) RegisterQueryHandler(func(string, *commonpb.Payloads, *commonpb.Header) (*commonpb.Payloads, error)) {
}
func (*environment) RegisterSignalHandler(func(string, *commonpb.Payloads, *commonpb.Header) error) {}
func (*environment) RegisterResetHandler(func(bindings.WorkflowResetInfo))                          {}
func (*environment) RegisterCancelHandler(func())                                                   {}
func (*environment) WorkflowInfo() *goWorkflow.Info {
	return &goWorkflow.Info{Namespace: "default", TaskQueueName: "upstream", OriginalRunID: "upstream-run"}
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
func (e *environment) ExecuteActivity(p bindings.ExecuteActivityParams, cb bindings.ResultHandler) bindings.ActivityID {
	if p.ActivityType.Name == "fail" {
		cb(nil, temporal.NewApplicationError("failure", "UpstreamFailure"))
	} else if p.ActivityType.Name == "echo" {
		cb(p.Input, nil)
	} else {
		panic("unexpected activity " + p.ActivityType.Name)
	}
	return bindings.ActivityID{}
}
func (e *environment) NewTimer(d time.Duration, _ goWorkflow.TimerOptions, cb bindings.ResultHandler) *bindings.TimerID {
	e.timers = append(e.timers, &timer{due: e.now.Add(d), callback: cb})
	return &bindings.TimerID{}
}
func (e *environment) RequestCancelTimer(bindings.TimerID) {
	// These scenarios cancel only the sole outstanding timer. TimerID's fields
	// are private to the host SDK, so the fake cannot manufacture distinct IDs.
	for _, timer := range e.timers {
		if timer.callback != nil {
			timer.canceled = true
		}
	}
}
func (e *environment) fireNext() bool {
	var next *timer
	for _, timer := range e.timers {
		if !timer.canceled && timer.callback != nil && (next == nil || timer.due.Before(next.due)) {
			next = timer
		}
	}
	if next == nil {
		return false
	}
	e.now = next.due
	cb := next.callback
	next.callback = nil
	cb(nil, nil)
	return true
}
func runPreferredVersion(h isolate.Handle) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	calls := 0
	var got goWorker.PreferredVersionProviderInput
	env.SetWorkerOptions(worker.Options{
		DeadlockDetectionTimeout: 10 * time.Second,
		PreferredVersionProvider: func(input goWorker.PreferredVersionProviderInput) *goWorker.VersionPreference {
			calls++
			got = input
			return &goWorker.VersionPreference{Version: goWorkflow.DefaultVersion}
		},
	})
	env.RegisterWorkflowWithOptions(temporalbridge.Factory{Function: h}, goWorkflow.RegisterOptions{Name: "NativePreferredVersion"})
	env.ExecuteWorkflow("NativePreferredVersion", "preferred-version")
	if !env.IsWorkflowCompleted() || env.GetWorkflowError() != nil {
		panic(fmt.Sprintf("preferred version workflow failed: %v", env.GetWorkflowError()))
	}
	var result string
	if err := env.GetWorkflowResult(&result); err != nil || result != "ok" {
		panic(fmt.Sprintf("preferred version result %q: %v", result, err))
	}
	if calls != 1 || got.ChangeID != "preferred-version" || got.MinSupported != goWorkflow.DefaultVersion || got.MaxSupported != 1 {
		panic(fmt.Sprintf("preferred version provider calls=%d input=%+v", calls, got))
	}
}

func run(scenario string) {
	if strings.HasPrefix(scenario, "environment-") {
		runEnvironment(strings.TrimPrefix(scenario, "environment-"))
		return
	}
	h, ok := isolate.LookupFunction(upstream.NativeWorkflow)
	if !ok {
		panic("upstream workflow handle missing")
	}
	if scenario == "preferred-version" {
		runPreferredVersion(h)
		return
	}
	d := (temporalbridge.Factory{Function: h}).NewWorkflowDefinition()
	defer d.Close()
	e := &environment{now: time.Date(2025, 1, 2, 0, 0, 0, 0, time.UTC)}
	input, err := e.GetDataConverter().ToPayloads(scenario)
	if err != nil {
		panic(err)
	}
	d.Execute(e, nil, input)
	for range 100 {
		d.OnWorkflowTaskStarted(5 * time.Second)
		if e.completed != 0 {
			break
		}
		if !e.fireNext() {
			panic("upstream scenario blocked without host work: " + scenario)
		}
	}
	if scenario == "returned-panic" {
		var returned *temporal.PanicError
		if e.completed != 1 || !errors.As(e.err, &returned) || returned.Error() != "panicError" || returned.StackTrace() != "stackTrace" {
			panic(fmt.Sprintf("returned panic lost: %v", e.err))
		}
		return
	}
	if e.err != nil {
		panic(e.err)
	}
	if e.completed != 1 {
		panic("upstream scenario did not complete once: " + scenario)
	}
	var result string
	if err := e.GetDataConverter().FromPayloads(e.result, &result); err != nil {
		panic(err)
	}
	if result != "ok" {
		panic(errors.New("unexpected upstream scenario result"))
	}
}
func main() {
	scenario := flag.String("scenario", "unbuffered", "upstream scenario")
	flag.Parse()
	for _, procs := range []int{1, 2, 8} {
		runtime.GOMAXPROCS(procs)
		run(*scenario)
	}
	fmt.Println("upstream scenario passed:", *scenario)
}
