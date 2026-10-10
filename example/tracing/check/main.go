package main

import (
	"encoding/json"
	"flag"
	"fmt"
	export "github.com/mfateev/sdk-go-poc/contrib/opentelemetry/exporter"
	"github.com/mfateev/sdk-go-poc/example/tracing"
	"github.com/mfateev/sdk-go-poc/temporalbridge"
	"github.com/mfateev/sdk-go-poc/worker"
	"go.opentelemetry.io/otel/trace"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/converter"
	bindings "go.temporal.io/sdk/internalbindings"
	"go.temporal.io/sdk/log"
	sdkworker "go.temporal.io/sdk/worker"
	sdkwf "go.temporal.io/sdk/workflow"
	"os"
	"strings"
	"time"
)

var headers []string

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
	replay             bool
	completes, metrics int
	result             *commonpb.Payloads
	err                error
	query              func(string, *commonpb.Payloads, *commonpb.Header) (*commonpb.Payloads, error)
	update             func(string, string, *commonpb.Payloads, *commonpb.Header, bindings.UpdateCallbacks)
	signal             func(string, *commonpb.Payloads, *commonpb.Header) error
}

func (*environment) RegisterCancelHandler(func()) {}
func (e *environment) RegisterQueryHandler(fn func(string, *commonpb.Payloads, *commonpb.Header) (*commonpb.Payloads, error)) {
	e.query = fn
}
func (e *environment) RegisterUpdateHandler(fn func(string, string, *commonpb.Payloads, *commonpb.Header, bindings.UpdateCallbacks)) {
	e.update = fn
}
func (e *environment) RegisterSignalHandler(fn func(string, *commonpb.Payloads, *commonpb.Header) error) {
	e.signal = fn
}
func (*environment) WorkflowInfo() *sdkwf.Info {
	return &sdkwf.Info{WorkflowType: sdkwf.Type{Name: "Workflow"}, WorkflowExecution: sdkwf.Execution{ID: "workflow", RunID: "run"}, OriginalRunID: "run", Namespace: "default", TaskQueueName: "tracing"}
}
func (*environment) Now() time.Time { return time.Date(2025, 1, 2, 0, 0, 0, 0, time.UTC) }
func (*environment) GetDataConverter() converter.DataConverter {
	return converter.GetDefaultDataConverter()
}
func (e *environment) IsReplaying() bool { return e.replay }
func (e *environment) Complete(result *commonpb.Payloads, err error) {
	e.completes++
	e.result, e.err = result, err
}
func (*environment) GetLogger() log.Logger { return logger{} }

func (*environment) GenerateSequence() int64 { return 1 }
func (e *environment) ExecuteActivity(params bindings.ExecuteActivityParams, callback bindings.ResultHandler) bindings.ActivityID {
	field := params.Header.Fields["_tracer-data"]
	if field == nil {
		field = params.Header.Fields["dd_trace_span"]
	}
	if field == nil {
		panic("activity trace header missing")
	}
	var data map[string]string
	must(e.GetDataConverter().FromPayload(field, &data))
	if !strings.Contains(data["traceparent"], "11111111111111111111111111111111") {
		panic("trace ID changed at activity boundary")
	}
	headers = append(headers, data["traceparent"])
	var input string
	must(e.GetDataConverter().FromPayloads(params.Input, &input))
	result, err := e.GetDataConverter().ToPayloads(input + "-echo")
	must(err)
	callback(result, nil)
	return bindings.ActivityID{}
}

type logger struct{}

func (logger) Debug(string, ...any)               {}
func (logger) Info(string, ...any)                {}
func (logger) Warn(message string, fields ...any) { fmt.Println("diagnostic:", message, fields) }
func (logger) Error(string, ...any)               {}

func must(err error) {
	if err != nil {
		panic(err)
	}
}
func traceHeader() *commonpb.Header {
	payload, err := converter.GetDefaultDataConverter().ToPayload(map[string]string{"traceparent": "00-11111111111111111111111111111111-2222222222222222-01", "baggage": "tenant=acme"})
	must(err)
	return &commonpb.Header{Fields: map[string]*commonpb.Payload{"_tracer-data": payload, "dd_trace_span": payload}}
}
func check(version string, replay bool, taskTimeout time.Duration) string {
	registrations := &registrations{}
	w := worker.Wrap(registrations)
	w.RegisterWorkflow(tracing.Workflow)
	w.RegisterActivity(tracing.Echo)
	must(worker.SetIsolateInterceptors(w, worker.InterceptorOptions{Factory: tracing.NewInterceptors, Config: []byte(version)}))
	spans := []string{}
	queryIDs := map[string]bool{}
	must(worker.RegisterSink(w, tracing.SinkOp, func(event worker.SinkEvent) {
		span, err := export.Decode(event.Payload, nil)
		must(err)
		spans = append(spans, span.Name())
		if !span.SpanContext().IsValid() || span.SpanContext().TraceID().String() != "11111111111111111111111111111111" {
			panic("sink lost original trace IDs")
		}
		if span.Name() == "user-span" && version == "opentracing" {
			if len(span.Events()) != 1 || span.Events()[0].Name != "input" || span.SpanKind() != trace.SpanKindClient {
				panic("OpenTracing logs lost")
			}
		}
		if strings.Contains(span.Name(), "HandleQuery") {
			id := span.SpanContext().SpanID().String()
			if queryIDs[id] {
				panic("different query requests reused a span ID")
			}
			queryIDs[id] = true
		}
	}))
	env := &environment{replay: replay}
	input, err := env.GetDataConverter().ToPayloads("hello")
	must(err)
	d := registrations.factory.NewWorkflowDefinition()
	defer d.Close()
	d.Execute(env, traceHeader(), input)
	d.OnWorkflowTaskStarted(taskTimeout)
	if env.err != nil {
		panic(env.err)
	}
	if version == "opentracing" {
		value, err := env.query("ottrace", nil, traceHeader())
		must(err)
		var tenant string
		must(env.GetDataConverter().FromPayloads(value, &tenant))
		if tenant != "acme" {
			panic("query lost OpenTracing baggage")
		}
		if _, err := env.query("badspan", nil, traceHeader()); err == nil || !strings.Contains(err.Error(), "read-only") {
			panic(fmt.Sprintf("span mutation: %v", err))
		}
	}
	before, err := env.query("trace", nil, traceHeader())
	must(err)
	_, err = env.query("bad", nil, traceHeader())
	if err == nil || !strings.Contains(err.Error(), "read-only") {
		panic(fmt.Sprintf("invalid query: %v", err))
	}
	after, err := env.query("trace", nil, traceHeader())
	must(err)
	if string(before.Payloads[0].Data) != string(after.Payloads[0].Data) {
		panic("query changed root trace")
	}
	signal, err := env.GetDataConverter().ToPayloads([]byte("done"))
	must(err)
	must(env.signal("finish", signal, traceHeader()))
	d.OnWorkflowTaskStarted(taskTimeout)
	if env.err != nil || env.completes != 1 {
		panic(fmt.Sprintf("completion=%d err=%v", env.completes, env.err))
	}
	var result string
	must(env.GetDataConverter().FromPayloads(env.result, &result))
	if result != "hello-echo" {
		panic(result)
	}
	if replay && len(spans) != 0 {
		panic("replay exported spans")
	}
	if !replay && len(spans) < 5 {
		panic(fmt.Sprintf("missing spans %v", spans))
	}
	raw, _ := json.Marshal(headers[len(headers)-1])
	return string(raw)
}
func main() {
	live := flag.String("live", "", "Temporal server address")
	taskTimeout := flag.Duration("task-timeout", 5*time.Second, "synthetic task deadline; allow instrumentation overhead in ownership/race stress checks")
	directory := flag.String("histories", "../testdata", "saved history directory")
	version := flag.String("version", "", "one live tracing integration")
	history := flag.String("history", "", "replay one saved history")
	flag.Parse()
	if *live != "" {
		must(liveTracing(*live, *directory, *version))
		return
	}
	if *history != "" {
		must(replayTracing(*history))
		return
	}

	for _, version := range []string{"v1", "v2", "datadog", "opentracing"} {
		a := check(version, false, *taskTimeout)
		b := check(version, true, *taskTimeout)
		if a != b {
			panic("replay changed tracing IDs or headers")
		}
		fmt.Println(version, a)
	}
	fmt.Fprintln(os.Stdout, "native tracing passed")
}
