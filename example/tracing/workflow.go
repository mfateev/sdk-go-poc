package tracing

import (
	"context"
	dd "github.com/mfateev/sdk-go-poc/contrib/datadog/tracing"
	otelv1 "github.com/mfateev/sdk-go-poc/contrib/opentelemetry"
	otelv2 "github.com/mfateev/sdk-go-poc/contrib/opentelemetry-v2"
	"github.com/mfateev/sdk-go-poc/interceptor"
	tracingv2 "github.com/mfateev/sdk-go-poc/interceptor/tracing"
	"github.com/mfateev/sdk-go-poc/workflow"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"math"
	"time"
)

const SinkOp uint32 = 0x10200

var state int

//go:isolate
func NewInterceptors(config []byte) ([]interceptor.WorkerInterceptor, error) {
	if string(config) == "datadog" {
		return []interceptor.WorkerInterceptor{dd.NewTracingInterceptor(dd.TracerOptions{SinkOp: SinkOp})}, nil
	}
	if string(config) == "v2" {
		return []interceptor.WorkerInterceptor{otelv2.NewTracingInterceptor(otelv2.TracerOptions{SinkOp: SinkOp, TracerOptions: tracingv2.TracerOptions{AddTemporalSpans: true}})}, nil
	}
	tracer, err := otelv1.NewTracingInterceptor(otelv1.TracerOptions{SinkOp: SinkOp})
	return []interceptor.WorkerInterceptor{tracer}, err
}

//go:isolate
func Workflow(ctx context.Context, input string) (string, error) {
	root := trace.SpanFromContext(ctx)
	if !root.SpanContext().IsValid() {
		panic("missing native tracing context")
	}
	root.SetAttributes(attribute.Int64("large", 9007199254740993), attribute.Float64("infinity", math.Inf(1)))
	if err := workflow.SetQueryHandler(ctx, "trace", func() (string, error) { return root.SpanContext().SpanID().String(), nil }); err != nil {
		return "", err
	}
	if err := workflow.SetQueryHandler(ctx, "bad", func() (int, error) { state++; return state, nil }); err != nil {
		return "", err
	}
	if err := workflow.SetUpdateHandler(ctx, "update", func(ctx context.Context) error {
		trace.SpanFromContext(ctx).AddEvent("update-event")
		state++
		return nil
	}); err != nil {
		return "", err
	}
	workflow.GetLogger(ctx).Info("traced-log")
	customCtx, custom := root.TracerProvider().Tracer("workflow").Start(ctx, "user-span")
	defer custom.End()
	custom.AddEvent("input", trace.WithAttributes(attribute.String("value", input)))
	ctx = workflow.WithActivityOptions(customCtx, workflow.ActivityOptions{StartToCloseTimeout: time.Second})
	var result string
	if err := workflow.ExecuteActivity(ctx, Echo, input).Get(ctx, &result); err != nil {
		return "", err
	}
	if signal := <-workflow.GetSignalChannel(ctx, "finish"); signal.Err != nil {
		return "", signal.Err
	}
	return result, nil
}
func Echo(_ context.Context, input string) (string, error) { return input + "-echo", nil }
