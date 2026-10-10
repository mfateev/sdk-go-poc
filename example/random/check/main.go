// This driver runs real compiled workflows across task suspension, eviction,
// queries and replay-like delivery with a synthetic host environment.
package main

import (
	"bytes"
	"crypto/rand"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"slices"
	"strings"
	"time"

	randomworkflow "github.com/mfateev/sdk-go-poc/example/random"
	"github.com/mfateev/sdk-go-poc/temporalbridge"
	"github.com/mfateev/sdk-go-poc/worker"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
	bindings "go.temporal.io/sdk/internalbindings"
	"go.temporal.io/sdk/log"
	sdkworker "go.temporal.io/sdk/worker"
	sdkwf "go.temporal.io/sdk/workflow"
)

type registrations struct {
	sdkworker.Worker
	factory temporalbridge.Factory
}

func (r *registrations) RegisterWorkflowWithOptions(fn any, _ sdkwf.RegisterOptions) {
	r.factory = fn.(temporalbridge.Factory)
}
func (*registrations) RegisterActivityWithOptions(any, activity.RegisterOptions) {}

type environment struct {
	bindings.WorkflowEnvironment
	info          sdkwf.Info
	replay        bool
	query         func(string, *commonpb.Payloads, *commonpb.Header) (*commonpb.Payloads, error)
	activity      bindings.ResultHandler
	input, result *commonpb.Payloads
	err           error
	completes     int
}

func (*environment) RegisterCancelHandler(func()) {}
func (e *environment) RegisterQueryHandler(fn func(string, *commonpb.Payloads, *commonpb.Header) (*commonpb.Payloads, error)) {
	e.query = fn
}
func (*environment) RegisterUpdateHandler(func(string, string, *commonpb.Payloads, *commonpb.Header, bindings.UpdateCallbacks)) {
}
func (*environment) RegisterSignalHandler(func(string, *commonpb.Payloads, *commonpb.Header) error) {}
func (e *environment) WorkflowInfo() *sdkwf.Info                                                    { return &e.info }
func (*environment) Now() time.Time                                                                 { return time.Date(2025, 1, 2, 0, 0, 0, 0, time.UTC) }
func (*environment) GetDataConverter() converter.DataConverter {
	return converter.GetDefaultDataConverter()
}
func (e *environment) IsReplaying() bool { return e.replay }
func (e *environment) Complete(result *commonpb.Payloads, err error) {
	e.result, e.err = result, err
	e.completes++
}
func (*environment) GetLogger() log.Logger   { return logger{} }
func (*environment) GenerateSequence() int64 { return 1 }
func (e *environment) ExecuteActivity(p bindings.ExecuteActivityParams, callback bindings.ResultHandler) bindings.ActivityID {
	e.input, e.activity = p.Input, callback
	return bindings.ActivityID{}
}

type logger struct{}

func (logger) Debug(string, ...any) {}
func (logger) Info(string, ...any)  {}
func (logger) Warn(string, ...any)  {}
func (logger) Error(string, ...any) {}
func must(err error) {
	if err != nil {
		panic(err)
	}
}

func run(factory temporalbridge.Factory, originalRunID, currentRunID string, queries bool) []string {
	e := &environment{replay: queries, info: sdkwf.Info{Namespace: "default", TaskQueueName: "random", WorkflowType: sdkwf.Type{Name: "RandomWorkflow"}, WorkflowExecution: sdkwf.Execution{ID: "workflow", RunID: currentRunID}, OriginalRunID: originalRunID}}
	d := factory.NewWorkflowDefinition()
	defer d.Close()
	d.Execute(e, nil, nil)
	d.OnWorkflowTaskStarted(5 * time.Second)
	if e.activity == nil || e.completes != 0 {
		panic(fmt.Sprintf("workflow did not suspend at activity: %v", e.err))
	}
	if queries {
		for range 4 {
			_, err := e.query("random", nil, nil)
			must(err)
			_, err = e.query("bad", nil, nil)
			if err == nil || !strings.Contains(err.Error(), "read-only") {
				panic(fmt.Sprintf("random read bypassed query ownership: %v", err))
			}
		}
	}
	e.activity(e.input, nil)
	d.OnWorkflowTaskStarted(5 * time.Second)
	if e.completes != 1 {
		panic("workflow did not complete")
	}
	must(e.err)
	var trace []string
	must(e.GetDataConverter().FromPayloads(e.result, &trace))
	if queries {
		_, err := e.query("random", nil, nil)
		must(err)
	}
	return trace
}

func main() {
	history := flag.String("history", "", "replay a CLI-exported history instead of the synthetic host probe")
	flag.Parse()
	if *history != "" {
		must(replayHistory(*history))
		fmt.Println("saved UUID/random history replay passed")
		return
	}
	r := &registrations{}
	w := worker.Wrap(r)
	w.RegisterWorkflow(randomworkflow.RandomWorkflow)
	w.RegisterActivity(randomworkflow.Echo)
	baseline := run(r.factory, "recorded-run", "live-run", false)
	for range 3 {
		_, _ = rand.Read(make([]byte, 4096)) // Host entropy must not affect a replay.
		if got := run(r.factory, "recorded-run", "replay-run", true); !slices.Equal(got, baseline) {
			panic("queries, eviction or replay changed random trace")
		}
	}
	if got := run(r.factory, "another-recorded-run", "live-run", false); slices.Equal(got, baseline) {
		panic("different runs reused random stream")
	}
	encoded, err := json.Marshal(baseline)
	must(err)
	fmt.Fprintln(os.Stdout, string(encoded))
}

func replayHistory(filename string) error {
	data, err := os.ReadFile(filename)
	if err != nil {
		return err
	}
	history, err := client.HistoryFromJSON(bytes.NewReader(data), client.HistoryJSONOptions{})
	if err != nil {
		return err
	}
	if len(history.Events) == 0 {
		return fmt.Errorf("history is empty")
	}
	completion := history.Events[len(history.Events)-1].GetWorkflowExecutionCompletedEventAttributes()
	if completion == nil {
		return fmt.Errorf("history is not completed")
	}
	var expected []string
	if err := converter.GetDefaultDataConverter().FromPayloads(completion.Result, &expected); err != nil {
		return err
	}
	r := worker.NewWorkflowReplayer()
	r.RegisterWorkflow(randomworkflow.RandomWorkflow)
	r.RegisterActivity(randomworkflow.Echo)
	if err := r.ReplayWorkflowHistory(nil, history); err != nil {
		return err
	}
	getter := r.(interface{ GetWorkflowResult(string, any) error })
	var got []string
	if err := getter.GetWorkflowResult("", &got); err != nil {
		return err
	}
	if !slices.Equal(got, expected) {
		return fmt.Errorf("random trace changed on replay: got %v want %v", got, expected)
	}
	return nil
}
