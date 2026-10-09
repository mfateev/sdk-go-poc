package main

import (
	"flag"
	"fmt"
	"isolate"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	example "github.com/mfateev/sdk-go-poc/example/converter"
	"github.com/mfateev/sdk-go-poc/example/converter/custom"
	"github.com/mfateev/sdk-go-poc/temporalbridge"
	"github.com/mfateev/sdk-go-poc/worker"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/sdk/converter"
	bindings "go.temporal.io/sdk/internalbindings"
	"go.temporal.io/sdk/log"
	"go.temporal.io/sdk/temporal"
	goWorker "go.temporal.io/sdk/worker"
	goWorkflow "go.temporal.io/sdk/workflow"
)

type fakeWorker struct {
	goWorker.Worker
	factory temporalbridge.Factory
}

func (w *fakeWorker) RegisterWorkflowWithOptions(fn any, _ goWorkflow.RegisterOptions) {
	w.factory = fn.(temporalbridge.Factory)
}

type environment struct {
	bindings.WorkflowEnvironment
	dc       converter.DataConverter
	query    func(string, *commonpb.Payloads, *commonpb.Header) (*commonpb.Payloads, error)
	timer    bindings.ResultHandler
	result   *commonpb.Payloads
	err      error
	complete int
}

func (*environment) RegisterUpdateHandler(func(string, string, *commonpb.Payloads, *commonpb.Header, bindings.UpdateCallbacks)) {
}
func (*environment) RegisterCancelHandler(func())                                                   {}
func (*environment) RegisterSignalHandler(func(string, *commonpb.Payloads, *commonpb.Header) error) {}
func (e *environment) RegisterQueryHandler(h func(string, *commonpb.Payloads, *commonpb.Header) (*commonpb.Payloads, error)) {
	e.query = h
}
func (e *environment) GetDataConverter() converter.DataConverter { return e.dc }
func (*environment) WorkflowInfo() *goWorkflow.Info {
	return &goWorkflow.Info{Namespace: "default", TaskQueueName: "converter-check", WorkflowExecution: goWorkflow.Execution{ID: "converter-id"}, WorkflowType: goWorkflow.Type{Name: "Workflow"}}
}
func (*environment) Now() time.Time          { return time.Date(2025, 1, 2, 0, 0, 0, 0, time.UTC) }
func (*environment) GetLogger() log.Logger   { return nil }
func (*environment) IsReplaying() bool       { return false }
func (*environment) GenerateSequence() int64 { return 1 }
func (e *environment) NewTimer(_ time.Duration, _ goWorkflow.TimerOptions, h bindings.ResultHandler) *bindings.TimerID {
	e.timer = h
	return nil
}
func (e *environment) Complete(p *commonpb.Payloads, err error) {
	e.result, e.err = p, err
	e.complete++
}
func (e *environment) ExecuteActivity(p bindings.ExecuteActivityParams, h bindings.ResultHandler) bindings.ActivityID {
	var input int64
	check(p.DataConverter.FromPayloads(p.Input, &input))
	if p.ActivityType.Name == "Echo" {
		result, err := p.DataConverter.ToPayloads(input)
		check(err)
		h(result, nil)
	} else {
		h(nil, temporal.NewNonRetryableApplicationError("failed", "ExampleError", nil, input))
	}
	return bindings.ActivityID{}
}
func check(err error) {
	if err != nil {
		panic(err)
	}
}
func main() {
	address := flag.String("address", "", "optional live Temporal server")
	historyFile := flag.String("history", "", "saved encrypted history to replay")
	historyDir := flag.String("history-dir", "", "directory of saved encrypted histories")
	flag.Parse()
	if *historyFile != "" {
		check(replayFile(*historyFile))
		return
	}
	for _, procs := range []int{1, 2, 8} {
		runtime.GOMAXPROCS(procs)
		run()
	}
	fmt.Println("worker converter configuration, ownership, host codecs and read-only caches passed")
	if *historyDir != "" {
		files, err := filepath.Glob(filepath.Join(*historyDir, "*.json"))
		check(err)
		if len(files) == 0 {
			panic("no encrypted histories found")
		}
		for _, file := range files {
			check(replayFile(file))
		}
	}
	if *address != "" {
		check(checkLive(*address))
	}
}
func run() {
	raw := &fakeWorker{}
	w := worker.Wrap(raw)
	w.RegisterWorkflow(example.Workflow) // Configuration may follow registration.
	config := []byte("configured")
	check(worker.SetIsolateDataConverter(w, worker.DataConverterOptions{Factory: custom.New, Config: config}))
	config[0] = 'x' // Setter must have copied this.
	dc, err := custom.New([]byte("configured"))
	check(err)
	dc = converter.NewCodecDataConverter(dc, converter.NewZlibCodec(converter.ZlibCodecOptions{AlwaysEncode: true}), &encryptedCodec{})
	dc = converter.WithDataConverterSerializationContext(dc, converter.WorkflowSerializationContext{Namespace: "default", WorkflowID: "converter-id"})
	const input int64 = 9007199254740993
	for range 2 {
		e := &environment{dc: dc}
		d := raw.factory.NewWorkflowDefinition()
		payloads, err := dc.ToPayloads(input)
		check(err)
		d.Execute(e, nil, payloads)
		d.OnWorkflowTaskStarted(5 * time.Second)
		if e.query == nil || e.timer == nil || e.complete != 0 {
			panic(fmt.Sprintf("did not suspend: complete=%d err=%v", e.complete, e.err))
		}
		output, err := e.query("state", nil, nil)
		check(err)
		var value int64
		check(dc.FromPayloads(output, &value))
		if value != input {
			panic("query lost value")
		}
		_, err = e.query("bad", nil, nil)
		if err == nil || !strings.Contains(err.Error(), "panic") {
			panic("query mutation not rejected")
		}
		output, err = e.query("state", nil, nil)
		check(err)
		check(dc.FromPayloads(output, &value))
		if value != input {
			panic("query mutated state")
		}
		e.timer(nil, nil)
		d.OnWorkflowTaskStarted(time.Second)
		check(e.err)
		check(dc.FromPayloads(e.result, &value))
		if e.complete != 1 || value != input {
			panic("result lost")
		}
		output, err = e.query("state", nil, nil)
		check(err)
		check(dc.FromPayloads(output, &value))
		if value != input {
			panic("completed query lost")
		}
		d.Close()
	}
	if custom.Initializations != 1 {
		panic("isolate initialized host package")
	}
	// Restoring defaults applies to subsequent definitions without changing registration.
	check(worker.SetIsolateDataConverter(w, worker.DataConverterOptions{}))
	if raw.factory.ResolveDataConverter().Factory.Name() != "" {
		panic("default converter reset failed")
	}
	if _, ok := isolate.LookupFunction(custom.New); !ok {
		panic("converter factory marker lost")
	}
}
