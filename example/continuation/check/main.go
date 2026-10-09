// Check compiled continuation, workflow aliases, and SDK version history.
package main

import (
	"errors"
	"flag"
	"fmt"
	"time"

	"github.com/mfateev/sdk-go-poc/example/continuation"
	"github.com/mfateev/sdk-go-poc/temporalbridge"
	"github.com/mfateev/sdk-go-poc/worker"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/sdk/converter"
	bindings "go.temporal.io/sdk/internalbindings"
	sdkworker "go.temporal.io/sdk/worker"
	sdkwf "go.temporal.io/sdk/workflow"
)

type registrations struct {
	sdkworker.Worker
	factory temporalbridge.Factory
}

func (w *registrations) RegisterWorkflowWithOptions(fn any, _ sdkwf.RegisterOptions) {
	w.factory = fn.(temporalbridge.Factory)
}

type environment struct {
	bindings.WorkflowEnvironment
	cancel    func()
	result    *commonpb.Payloads
	err       error
	completes int
	versions  map[string]sdkwf.Version
}

func (*environment) GetDataConverter() converter.DataConverter {
	return converter.GetDefaultDataConverter()
}
func (e *environment) RegisterCancelHandler(fn func())                                              { e.cancel = fn }
func (*environment) RegisterSignalHandler(func(string, *commonpb.Payloads, *commonpb.Header) error) {}
func (*environment) WorkflowInfo() *sdkwf.Info {
	return &sdkwf.Info{TaskQueueName: "continuation", WorkflowRunTimeout: time.Hour, WorkflowTaskTimeout: 10 * time.Second}
}
func (*environment) Now() time.Time    { return time.Date(2025, 1, 2, 0, 0, 0, 0, time.UTC) }
func (*environment) IsReplaying() bool { return true }
func (e *environment) GetVersion(id string, min, max sdkwf.Version) sdkwf.Version {
	if e.versions == nil {
		e.versions = make(map[string]sdkwf.Version)
	}
	v, ok := e.versions[id]
	if !ok {
		v = max
		e.versions[id] = v
	}
	if v < min || v > max {
		panic("unsupported recorded version")
	}
	return v
}
func (e *environment) Complete(result *commonpb.Payloads, err error) {
	e.completes++
	e.result, e.err = result, err
}

func main() {
	address := flag.String("live", "", "optional Temporal dev server address")
	dir := flag.String("history-dir", "", "saved replay fixtures; live mode also exports them here")
	flag.Parse()
	checkFreshRuns()
	checkVersionCalls()
	if *address != "" {
		if err := checkLive(*address, *dir); err != nil {
			panic(err)
		}
	}
	if *dir != "" {
		if err := checkHistories(*dir); err != nil {
			panic(err)
		}
	}
	fmt.Println("compiled fresh-run continuation, aliases and versioning passed")
}

func checkFreshRuns() {
	registration := new(registrations)
	worker.Wrap(registration).RegisterWorkflowWithOptions(continuation.ContinueWorkflow, sdkwf.RegisterOptions{Name: "ContinuedAlias"})
	dc := converter.GetDefaultDataConverter()
	input, err := dc.ToPayloads(3)
	if err != nil {
		panic(err)
	}
	for remaining := 3; remaining >= 0; remaining-- {
		e := new(environment)
		d := registration.factory.NewWorkflowDefinition()
		d.Execute(e, nil, input)
		d.OnWorkflowTaskStarted(5 * time.Second)
		if e.completes != 1 {
			panic("run did not finish exactly once")
		}
		// Even retained callbacks may never revive the retired run.
		e.cancel()
		d.OnWorkflowTaskStarted(time.Second)
		d.Close()
		if err := d.(interface{ CloseError() error }).CloseError(); err != nil {
			panic(err)
		}
		if e.completes != 1 {
			panic("retired run completed again")
		}
		if remaining == 0 {
			var result int
			if e.err != nil || dc.FromPayloads(e.result, &result) != nil || result != 1 {
				panic(fmt.Sprintf("fresh run result %d: %v", result, e.err))
			}
			continue
		}
		var next *sdkwf.ContinueAsNewError
		if !errors.As(e.err, &next) || next.WorkflowType.Name != "ContinuedAlias" || next.TaskQueueName != "continuation" || next.WorkflowRunTimeout != time.Hour || next.WorkflowTaskTimeout != 10*time.Second {
			panic(fmt.Sprintf("lost SDK continuation fields: %+v", e.err))
		}
		var argument int
		if err := dc.FromPayloads(next.Input, &argument); err != nil || argument != remaining-1 {
			panic("wrong continuation input")
		}
		input = next.Input
	}
}

func checkVersionCalls() {
	registration := new(registrations)
	worker.Wrap(registration).RegisterWorkflow(continuation.VersionWorkflow)
	e := new(environment)
	d := registration.factory.NewWorkflowDefinition()
	d.Execute(e, nil, nil)
	d.OnWorkflowTaskStarted(5 * time.Second)
	d.Close()
	var result int
	if e.err != nil || e.completes != 1 || e.GetDataConverter().FromPayloads(e.result, &result) != nil || result != 1 || len(e.versions) != 1 {
		panic(fmt.Sprintf("version cache: result=%d error=%v", result, e.err))
	}
}

func (*environment) RegisterUpdateHandler(func(string, string, *commonpb.Payloads, *commonpb.Header, bindings.UpdateCallbacks)) {
}
