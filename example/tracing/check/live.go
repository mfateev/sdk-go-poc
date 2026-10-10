package main

import (
	"context"
	"fmt"
	"github.com/DataDog/dd-trace-go/v2/ddtrace/mocktracer"
	ddtracer "github.com/DataDog/dd-trace-go/v2/ddtrace/tracer"
	ddexport "github.com/mfateev/sdk-go-poc/contrib/datadog/exporter"
	nativeexport "github.com/mfateev/sdk-go-poc/contrib/opentelemetry/exporter"
	"github.com/mfateev/sdk-go-poc/example/tracing"
	"github.com/mfateev/sdk-go-poc/worker"
	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/sdk/client"
	upstreamdd "go.temporal.io/sdk/contrib/datadog/tracing"
	upstreamotel "go.temporal.io/sdk/contrib/opentelemetry"
	upstreamotelv2 "go.temporal.io/sdk/contrib/opentelemetry-v2"
	sdkinterceptor "go.temporal.io/sdk/interceptor"
	sdktracing "go.temporal.io/sdk/interceptor/tracing"
	sdkwf "go.temporal.io/sdk/workflow"
	"google.golang.org/protobuf/encoding/protojson"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

type collector struct {
	mu    sync.Mutex
	spans []sdktrace.ReadOnlySpan
}

func (c *collector) ExportSpans(_ context.Context, spans []sdktrace.ReadOnlySpan) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.spans = append(c.spans, spans...)
	return nil
}
func (*collector) Shutdown(context.Context) error            { return nil }
func ordinary(_ sdkwf.Context, input string) (string, error) { return input, nil }
func configure(w interface {
	RegisterWorkflow(any)
	RegisterActivity(any)
}, version string) error {
	w.RegisterWorkflow(tracing.Workflow)
	w.RegisterWorkflow(ordinary)
	w.RegisterActivity(tracing.Echo)
	return worker.SetIsolateInterceptors(w, worker.InterceptorOptions{Factory: tracing.NewInterceptors, Config: []byte(version)})
}
func replayTracing(file string) error {
	raw, err := os.ReadFile(file)
	if err != nil {
		return err
	}
	history := new(historypb.History)
	if err = protojson.Unmarshal(raw, history); err != nil {
		return err
	}
	version := strings.TrimSuffix(filepath.Base(file), ".json")
	r := worker.NewWorkflowReplayer()
	if err = configure(r, version); err != nil {
		return err
	}
	observations := 0
	if err = worker.RegisterSink(r, tracing.SinkOp, func(worker.SinkEvent) { observations++ }); err != nil {
		return err
	}
	if err = r.ReplayWorkflowHistory(nil, history); err != nil {
		return err
	}
	var result string
	if err = r.(interface{ GetWorkflowResult(string, any) error }).GetWorkflowResult("", &result); err != nil {
		return err
	}
	if result != "hello-echo" || observations != 0 {
		return fmt.Errorf("replay result=%q exported=%d", result, observations)
	}
	fmt.Println("saved tracing history replayed:", version)
	return nil
}
func liveTracing(address, dir string, only string) error {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}
	for _, version := range []string{"v1", "v2", "datadog"} {
		if only != "" && only != version {
			continue
		}
		if err := liveTracingVersion(address, dir, version); err != nil {
			return fmt.Errorf("%s: %w", version, err)
		}
	}
	return nil
}
func liveTracingVersion(address, dir, version string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	queue := fmt.Sprintf("isolate-tracing-%s-%d", version, time.Now().UnixNano())
	collector := new(collector)
	processor := sdktrace.NewBatchSpanProcessor(collector)
	defer processor.Shutdown(context.Background())
	provider := upstreamotelv2.NewReplaySafeTracerProvider(sdktrace.WithSpanProcessor(processor))
	defer provider.Shutdown(context.Background())
	var interceptors []sdkinterceptor.ClientInterceptor
	var ddhost mocktracer.Tracer
	if version == "datadog" {
		ddhost = mocktracer.Start()
		defer ddhost.Stop()
		parent := ddtracer.StartSpan("caller", ddtracer.Tag("manual.keep", true))
		defer parent.Finish()
		ctx = ddtracer.ContextWithSpan(ctx, parent)
		interceptors = []sdkinterceptor.ClientInterceptor{upstreamdd.NewTracingInterceptor(upstreamdd.TracerOptions{})}
	} else if version == "v1" {
		tracing, err := upstreamotel.NewTracingInterceptor(upstreamotel.TracerOptions{Tracer: provider.Tracer("temporal-sdk-go")})
		if err != nil {
			return err
		}
		interceptors = []sdkinterceptor.ClientInterceptor{tracing}
	}
	options := client.Options{HostPort: address, Interceptors: interceptors}
	if version == "v2" {
		previous := otel.GetTracerProvider()
		otel.SetTracerProvider(provider)
		defer otel.SetTracerProvider(previous)
		plugin, err := upstreamotelv2.NewPlugin(upstreamotelv2.PluginOptions{TracerOptions: sdktracing.TracerOptions{AddTemporalSpans: true}})
		if err != nil {
			return err
		}
		options.Plugins = []client.Plugin{plugin}
	}
	c, err := client.Dial(options)
	if err != nil {
		return err
	}
	defer c.Close()
	w := worker.New(c, queue, worker.Options{})
	if err = configure(w, version); err != nil {
		return err
	}
	if version == "datadog" {
		err = ddexport.Register(w, tracing.SinkOp, ddexport.Options{Service: "isolate-poc"})
	} else {
		err = nativeexport.RegisterProcessor(w, tracing.SinkOp, processor, nil)
	}
	if err != nil {
		return err
	}
	if err = w.Start(); err != nil {
		return err
	}
	defer w.Stop()
	run, err := c.ExecuteWorkflow(ctx, client.StartWorkflowOptions{ID: queue, TaskQueue: queue}, "Workflow", "hello")
	if err != nil {
		return err
	}
	update, err := c.UpdateWorkflow(ctx, client.UpdateWorkflowOptions{WorkflowID: run.GetID(), UpdateName: "update", WaitForStage: client.WorkflowUpdateStageCompleted})
	if err != nil {
		return err
	}
	if err = update.Get(ctx, nil); err != nil {
		return err
	}
	before, err := c.QueryWorkflow(ctx, run.GetID(), "", "trace")
	if err != nil {
		return err
	}
	var spanID string
	if err = before.Get(&spanID); err != nil {
		return err
	}
	if _, err = c.QueryWorkflow(ctx, run.GetID(), "", "bad"); err == nil {
		return fmt.Errorf("mutating query accepted")
	}
	after, err := c.QueryWorkflow(ctx, run.GetID(), "", "trace")
	if err != nil {
		return err
	}
	var afterID string
	if err = after.Get(&afterID); err != nil {
		return err
	}
	if afterID != spanID {
		return fmt.Errorf("query changed cached span")
	}
	if err = c.SignalWorkflow(ctx, run.GetID(), "", "finish", []byte("finish")); err != nil {
		return err
	}
	var result string
	if err = run.Get(ctx, &result); err != nil {
		return err
	}
	if result != "hello-echo" {
		return fmt.Errorf("result=%q", result)
	}
	completed, err := c.QueryWorkflow(ctx, run.GetID(), "", "trace")
	if err != nil {
		return err
	}
	if err = completed.Get(&afterID); err != nil {
		return err
	}
	if afterID != spanID {
		return fmt.Errorf("completed query changed span")
	}
	plain, err := c.ExecuteWorkflow(ctx, client.StartWorkflowOptions{TaskQueue: queue}, ordinary, "plain")
	if err != nil {
		return err
	}
	if err = plain.Get(ctx, &result); err != nil {
		return err
	}
	if result != "plain" {
		return fmt.Errorf("ordinary SDK workflow changed")
	}
	history := new(historypb.History)
	it := c.GetWorkflowHistory(ctx, run.GetID(), run.GetRunID(), false, enumspb.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)
	for it.HasNext() {
		event, err := it.Next()
		if err != nil {
			return err
		}
		history.Events = append(history.Events, event)
	}
	raw, err := protojson.MarshalOptions{Indent: "  "}.Marshal(history)
	if err != nil {
		return err
	}
	file := filepath.Join(dir, version+".json")
	if err = os.WriteFile(file, raw, 0644); err != nil {
		return err
	}
	if err = processor.ForceFlush(ctx); err != nil {
		return err
	}
	if version == "datadog" {
		spans := ddhost.FinishedSpans()
		var runID, activityParent uint64
		for _, span := range spans {
			if span.OperationName() == "temporal.RunWorkflow" && span.Tag("resource.name") == "Workflow" {
				runID = span.SpanID()
			}
			if span.OperationName() == "temporal.StartActivity" {
				activityParent = span.SpanID()
			}
		}
		if runID == 0 || activityParent == 0 {
			return fmt.Errorf("missing Datadog workflow/activity spans: %v", spans)
		}
		linked := false
		for _, span := range spans {
			if span.OperationName() == "temporal.RunActivity" && span.ParentID() == activityParent {
				linked = true
			}
		}
		if !linked {
			return fmt.Errorf("Datadog activity lost its native parent")
		}
	} else {
		collector.mu.Lock()
		spans := append([]sdktrace.ReadOnlySpan(nil), collector.spans...)
		collector.mu.Unlock()
		var activityParent trace.SpanContext
		for _, span := range spans {
			if span.Name() == "StartActivity:Echo" {
				activityParent = span.SpanContext()
			}
		}
		linked := false
		for _, span := range spans {
			if span.Name() == "RunActivity:Echo" && span.Parent().SpanID() == activityParent.SpanID() && span.SpanContext().TraceID() == activityParent.TraceID() {
				linked = true
			}
		}
		if !linked {
			return fmt.Errorf("OpenTelemetry activity lost its native parent")
		}
	}
	if err = replayTracing(file); err != nil {
		return err
	}
	fmt.Println("live", version, "headers, spans, queries, updates, ordinary workflow and replay passed")
	return nil
}
