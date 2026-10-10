// Package opentelemetry adapts Temporal v2 tracing to native isolate contexts.
package opentelemetry

import (
	"context"
	otelv1 "github.com/mfateev/sdk-go-poc/contrib/opentelemetry"
	"github.com/mfateev/sdk-go-poc/interceptor"
	"github.com/mfateev/sdk-go-poc/interceptor/tracing"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/baggage"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
	"go.temporal.io/sdk/log"
	"time"
)

type TracerOptions struct {
	TracerOptions     tracing.TracerOptions
	SinkOp            uint32
	Tracer            trace.Tracer
	DisableBaggage    bool
	TextMapPropagator propagation.TextMapPropagator
}
type options struct{ TracerOptions }

func (o *options) Options() tracing.TracerOptions { return o.TracerOptions.TracerOptions }

// NewTracer constructs a private v2 workflow tracer. AddTemporalSpans retains
// its upstream default (false); headers still propagate without Temporal spans.
func NewTracer(opts TracerOptions) tracing.WorkflowTracer {
	if opts.TracerOptions.HeaderKey == "" {
		opts.TracerOptions.HeaderKey = "_tracer-data"
	}
	if opts.TextMapPropagator == nil {
		opts.TextMapPropagator = otelv1.NewTextMapPropagator()
	}
	if opts.Tracer == nil {
		opts.Tracer = otelv1.NewSinkTracerProvider(opts.SinkOp).Tracer("temporal-sdk-go")
	}
	options := &options{TracerOptions: opts}
	bridge := &contextBridge{options: options}
	return &nativeTracer{options: options, contextBridge: bridge, spanCodec: &spanCodec{contextBridge: bridge}}
}
func NewTracingInterceptor(opts TracerOptions) interceptor.WorkerInterceptor {
	return tracing.NewTracingInterceptor(NewTracer(opts))
}

type nativeTracer struct {
	tracing.BaseTracer
	*options
	*contextBridge
	*spanCodec
}

func (t *nativeTracer) CreateSpan(ctx context.Context, opts *tracing.TracerStartSpanOptions) tracing.TracerSpan {
	ctx = t.contextBridge.ContextWithSpan(ctx, opts.Parent)
	kind := trace.SpanKindInternal
	if opts.Direction == tracing.SpanDirectionInbound {
		kind = trace.SpanKindServer
	}
	if opts.Direction == tracing.SpanDirectionOutbound {
		kind = trace.SpanKindClient
	}
	attrs := make([]attribute.KeyValue, 0, len(opts.Tags))
	for k, v := range opts.Tags {
		attrs = append(attrs, attribute.String(k, v))
	}
	_, span := t.options.Tracer.Start(ctx, t.SpanName(opts), trace.WithTimestamp(time.Now()), trace.WithSpanKind(kind), trace.WithAttributes(attrs...))
	out := &tracerSpan{Span: span}
	if !t.DisableBaggage {
		out.Baggage = baggage.FromContext(ctx)
	}
	return out
}
func (t *nativeTracer) GetLogger(logger log.Logger, ref tracing.TracerSpanRef) log.Logger {
	span := asTracerSpan(ref)
	if span == nil || span.Span == nil || !span.SpanContext().IsValid() {
		return logger
	}
	return log.With(logger, "TraceID", span.SpanContext().TraceID(), "SpanID", span.SpanContext().SpanID())
}

// NewSinkTracerProvider also supports spans started directly by application code.
func NewSinkTracerProvider(op uint32) trace.TracerProvider { return otelv1.NewSinkTracerProvider(op) }
