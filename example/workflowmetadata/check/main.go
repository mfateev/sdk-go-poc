package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"reflect"
	"strings"
	"time"
	"unsafe"

	metadata "github.com/mfateev/sdk-go-poc/example/workflowmetadata"
	"github.com/mfateev/sdk-go-poc/internal/searchattrwire"
	"github.com/mfateev/sdk-go-poc/temporalbridge"
	"github.com/mfateev/sdk-go-poc/worker"
	commonpb "go.temporal.io/api/common/v1"
	failurepb "go.temporal.io/api/failure/v1"
	sdkpb "go.temporal.io/api/sdk/v1"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/converter"
	bindings "go.temporal.io/sdk/internalbindings"
	"go.temporal.io/sdk/log"
	"go.temporal.io/sdk/temporal"
	sdkworker "go.temporal.io/sdk/worker"
	sdkwf "go.temporal.io/sdk/workflow"
	"google.golang.org/protobuf/proto"
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
	info      *sdkwf.Info
	query     func(string, *commonpb.Payloads, *commonpb.Header) (*commonpb.Payloads, error)
	signal    func(string, *commonpb.Payloads, *commonpb.Header) error
	update    func(string, string, *commonpb.Payloads, *commonpb.Header, bindings.UpdateCallbacks)
	result    *commonpb.Payloads
	err       error
	completes int
}

func (e *environment) WorkflowInfo() *sdkwf.Info { return e.info }
func (*environment) Now() time.Time              { return time.Unix(1700000000, 0) }
func (*environment) IsReplaying() bool           { return false }
func (*environment) GetLogger() log.Logger       { return nil }
func (*environment) GetDataConverter() converter.DataConverter {
	return converter.GetDefaultDataConverter()
}
func (*environment) RegisterCancelHandler(func()) {}
func (e *environment) RegisterUpdateHandler(fn func(string, string, *commonpb.Payloads, *commonpb.Header, bindings.UpdateCallbacks)) {
	e.update = fn
}
func (e *environment) RegisterQueryHandler(fn func(string, *commonpb.Payloads, *commonpb.Header) (*commonpb.Payloads, error)) {
	e.query = fn
}
func (e *environment) RegisterSignalHandler(fn func(string, *commonpb.Payloads, *commonpb.Header) error) {
	e.signal = fn
}
func (e *environment) Complete(p *commonpb.Payloads, err error) {
	e.result, e.err = p, err
	e.completes++
}
func (e *environment) TypedSearchAttributes() temporal.SearchAttributes {
	fields := make(map[string][]byte)
	for name, p := range e.info.SearchAttributes.GetIndexedFields() {
		if p.Data == nil || len(p.Metadata["type"]) == 0 {
			continue
		}
		fields[name], _ = proto.Marshal(&commonpb.Payloads{Payloads: []*commonpb.Payload{p}})
	}
	raw, _ := json.Marshal(fields)
	attributes, err := searchattrwire.Decode(raw)
	must(err)
	return attributes
}

func (e *environment) UpsertSearchAttributes(values map[string]any) error {
	if len(values) == 0 {
		return fmt.Errorf("search attributes is empty")
	}
	if e.info.SearchAttributes == nil {
		e.info.SearchAttributes = &commonpb.SearchAttributes{IndexedFields: make(map[string]*commonpb.Payload)}
	}
	for name, value := range values {
		p, ok := value.(*commonpb.Payload)
		if !ok {
			panic("search attribute was reserialized")
		}
		if p.Data == nil {
			delete(e.info.SearchAttributes.IndexedFields, name)
		} else {
			e.info.SearchAttributes.IndexedFields[name] = p
		}
	}
	return nil
}
func (e *environment) UpsertMemo(values map[string]any) error {
	if len(values) == 0 {
		return fmt.Errorf("memo is empty")
	}
	if e.info.Memo == nil {
		e.info.Memo = &commonpb.Memo{Fields: make(map[string]*commonpb.Payload)}
	}
	for name, value := range values {
		p, err := e.GetDataConverter().ToPayload(value)
		if err != nil {
			return err
		}
		if p.Data == nil {
			delete(e.info.Memo.Fields, name)
		} else {
			e.info.Memo.Fields[name] = p
		}
	}
	return nil
}
func must(err error) {
	if err != nil {
		panic(err)
	}
}
func setPrivate(info *sdkwf.Info, name string, value any) {
	field := reflect.ValueOf(info).Elem().FieldByName(name)
	reflect.NewAt(field.Type(), unsafe.Pointer(field.UnsafeAddr())).Elem().Set(reflect.ValueOf(value))
}
func check(previous string, e *environment, taskTimeout time.Duration) {
	registrations := &registrations{}
	w := worker.Wrap(registrations)
	w.RegisterWorkflow(metadata.Workflow)
	must(worker.SetIsolateInterceptors(w, worker.InterceptorOptions{Factory: metadata.NewInterceptors}))
	d := registrations.factory.NewWorkflowDefinition()
	defer d.Close()
	input, err := e.GetDataConverter().ToPayloads(previous, false)
	must(err)
	d.Execute(e, nil, input)
	d.OnWorkflowTaskStarted(taskTimeout)
	if e.completes != 0 {
		panic(fmt.Sprintf("premature completion: %v", e.err))
	}
	p, err := e.query("__temporal_workflow_metadata", nil, nil)
	must(err)
	var description sdkpb.WorkflowMetadata
	must(e.GetDataConverter().FromPayloads(p, &description))
	signals := description.Definition.SignalDefinitions
	if len(signals) != 1 || signals[0].Description != "Complete the metadata check" {
		panic("signal descriptions lost")
	}
	checkQuery := func() {
		p, err := e.query("metadata-state", nil, nil)
		must(err)
		var result string
		must(e.GetDataConverter().FromPayloads(p, &result))
		if result != "ok" {
			panic(result)
		}
	}
	checkQuery()
	if _, err := e.query("bad-metadata", nil, nil); err == nil || !strings.Contains(err.Error(), "cannot mutate") {
		panic(fmt.Sprintf("bad query: %v", err))
	}
	checkQuery()

	if _, err := e.query("bad-key", nil, nil); err == nil || !strings.Contains(err.Error(), "unsupported search attribute map key") {
		panic(fmt.Sprintf("custom map key: %v", err))
	}
	checkQuery()
	validation := new(outcome)
	e.update("check", "check", nil, nil, validation)
	d.OnWorkflowTaskStarted(taskTimeout)
	if !validation.accepted || !validation.completed || validation.err != nil {
		panic(fmt.Sprintf("metadata validator: %+v", validation))
	}
	must(e.signal("finish", nil, nil))
	d.OnWorkflowTaskStarted(taskTimeout)
	if e.completes != 1 || e.err != nil {
		panic(fmt.Sprintf("completion: %d %v", e.completes, e.err))
	}
	checkQuery()
}
func newEnvironment(previous string) *environment {
	e := &environment{info: &sdkwf.Info{WorkflowType: sdkwf.Type{Name: "Workflow"}, WorkflowExecution: sdkwf.Execution{ID: "metadata", RunID: "run"}, OriginalRunID: "run", Namespace: "default", TaskQueueName: "metadata"}}
	if previous == "empty" {
		setPrivate(e.info, "lastCompletionResult", &commonpb.Payloads{})
	}
	if previous == "values" {
		p, err := e.GetDataConverter().ToPayloads(metadata.Precise, "previous")
		must(err)
		setPrivate(e.info, "lastCompletionResult", p)
		failure := temporal.NewDefaultFailureConverter(temporal.DefaultFailureConverterOptions{}).ErrorToFailure(temporal.NewNonRetryableApplicationError("previous failure", "Previous", nil, metadata.Precise))
		setPrivate(e.info, "lastFailure", (*failurepb.Failure)(failure))
	}
	return e
}

func main() {
	taskTimeout := flag.Duration("task-timeout", 30*time.Second, "synthetic workflow task deadline")
	live := flag.String("live", "", "Temporal server address")
	history := flag.String("history", "", "saved history to replay")
	output := flag.String("output", "", "path to save live history")
	flag.Parse()
	if *history != "" {
		must(replayFile(*history))
		return
	}
	if *live != "" {
		must(liveCheck(*live, *output))
		return
	}
	for _, previous := range []string{"absent", "empty", "values"} {
		check(previous, newEnvironment(previous), *taskTimeout)
	}
	fmt.Println("metadata hooks, precise previous results/failures, read-only getters and signal descriptions passed")
}

type outcome struct {
	accepted, completed bool
	err                 error
}

func (o *outcome) Accept()                   { o.accepted = true }
func (o *outcome) Reject(err error)          { o.err = err }
func (o *outcome) Complete(_ any, err error) { o.completed = true; o.err = err }
