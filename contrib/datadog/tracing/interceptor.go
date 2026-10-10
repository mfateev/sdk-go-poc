// Package tracing provides Datadog-compatible native workflow interception.
package tracing

import (
	"context"
	"encoding/binary"
	"fmt"
	"github.com/mfateev/sdk-go-poc/contrib/opentelemetry"
	"github.com/mfateev/sdk-go-poc/interceptor"
	"github.com/mfateev/sdk-go-poc/internal/ddtracewire"
	"github.com/mfateev/sdk-go-poc/internal/otelcontext"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/baggage"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
	"go.temporal.io/sdk/log"
	"slices"
	"strconv"
	"strings"
)

type TracerOptions struct {
	SinkOp                                                          uint32
	DisableSignalTracing, DisableQueryTracing, DisableUpdateTracing bool
	AllowInvalidParentSpans                                         bool
}
type contextKey struct{}

const HeaderKey = "dd_trace_span"

// NewTracer keeps the SDK tracer contract and Datadog header/name/log formats.
// IDs come from deterministic isolate randomness. Datadog runtime objects and
// OnFinish backend options are configured separately on the host exporter.
func NewTracer(opts TracerOptions) interceptor.Tracer {
	inner, err := opentelemetry.NewTracer(opentelemetry.TracerOptions{SinkOp: opts.SinkOp, Tracer: opentelemetry.NewSinkTracerProvider(opts.SinkOp).Tracer("temporal-sdk-go/datadog"), SpanStarter: func(ctx context.Context, t trace.Tracer, name string, options ...trace.SpanStartOption) trace.Span {
		operation, _, _ := strings.Cut(name, ":")
		_, span := t.Start(ctx, operation, options...)
		return span
	}, DisableSignalTracing: opts.DisableSignalTracing, DisableQueryTracing: opts.DisableQueryTracing, DisableUpdateTracing: opts.DisableUpdateTracing, AllowInvalidParentSpans: opts.AllowInvalidParentSpans, SpanContextKey: contextKey{}, HeaderKey: HeaderKey, TextMapPropagator: datadogPropagator{}})
	if err != nil {
		panic(err)
	}
	return &nativeTracer{Tracer: inner}
}
func NewTracingInterceptor(opts TracerOptions) interceptor.WorkerInterceptor {
	return interceptor.NewTracingInterceptor(NewTracer(opts))
}

type nativeTracer struct{ interceptor.Tracer }

func (t *nativeTracer) UnmarshalSpan(data map[string]string) (interceptor.TracerSpanRef, error) {
	if data["traceparent"] != "" {
		return t.Tracer.UnmarshalSpan(data)
	}
	if data["x-datadog-trace-id"] == "" && data["x-datadog-parent-id"] == "" {
		return nil, nil
	}
	ctx := datadogPropagator{}.Extract(context.Background(), propagation.MapCarrier(data))
	if !trace.SpanContextFromContext(otelcontext.WithFallback(ctx)).IsValid() {
		return nil, fmt.Errorf("invalid Datadog span context")
	}
	carrier := propagation.MapCarrier{}
	datadogPropagator{}.Inject(ctx, carrier)
	return t.Tracer.UnmarshalSpan(carrier)
}
func (t *nativeTracer) SpanName(opts *interceptor.TracerStartSpanOptions) string {
	return "temporal." + opts.Operation
}
func (t *nativeTracer) StartSpan(opts *interceptor.TracerStartSpanOptions) (interceptor.TracerSpan, error) {
	return t.StartSpanWithContext(context.Background(), opts)
}
func (t *nativeTracer) StartSpanWithContext(ctx context.Context, opts *interceptor.TracerStartSpanOptions) (interceptor.TracerSpan, error) {
	copy := *opts
	copy.Operation = "temporal." + opts.Operation
	copy.Name = ""
	span, err := t.Tracer.(interface {
		StartSpanWithContext(context.Context, *interceptor.TracerStartSpanOptions) (interceptor.TracerSpan, error)
	}).StartSpanWithContext(ctx, &copy)
	if err == nil {
		if sc, ok := span.(trace.Span); ok {
			attrs := []attribute.KeyValue{attribute.String("resource.name", opts.Name)}
			for k, v := range opts.Tags {
				attrs = append(attrs, attribute.String("temporal."+strings.TrimPrefix(k, "temporal"), v))
			}
			sc.SetAttributes(attrs...)
		}
	}
	return span, err
}
func (t *nativeTracer) GetLogger(logger log.Logger, span interceptor.TracerSpanRef) log.Logger {
	sc, ok := span.(interface{ SpanContext() trace.SpanContext })
	if !ok {
		return logger
	}
	tid := sc.SpanContext().TraceID()
	sid := sc.SpanContext().SpanID()
	return log.With(logger, "dd.trace_id", binary.BigEndian.Uint64(tid[8:]), "dd.span_id", binary.BigEndian.Uint64(sid[:]))
}

// datadogPropagator accepts legacy decimal Datadog carriers and W3C carriers.
// Sampling priority, origin, propagated tags, trace high bits and baggage survive.
type datadogPropagator struct{}

func (datadogPropagator) Inject(ctx context.Context, carrier propagation.TextMapCarrier) {
	opentelemetry.NewTextMapPropagator().Inject(ctx, carrier)
	for _, member := range baggage.FromContext(ctx).Members() {
		carrier.Set("ot-baggage-"+member.Key(), member.Value())
	}
	sc := trace.SpanContextFromContext(otelcontext.WithFallback(ctx))
	if !sc.IsValid() {
		return
	}
	tid := sc.TraceID()
	sid := sc.SpanID()
	carrier.Set("x-datadog-trace-id", strconv.FormatUint(binary.BigEndian.Uint64(tid[8:]), 10))
	carrier.Set("x-datadog-parent-id", strconv.FormatUint(binary.BigEndian.Uint64(sid[:]), 10))
	meta := ddtracewire.FromContext(sc)
	carrier.Set("x-datadog-sampling-priority", strconv.Itoa(meta.Priority))
	if meta.Origin != "" {
		carrier.Set("x-datadog-origin", meta.Origin)
	}
	if high := binary.BigEndian.Uint64(tid[:8]); high != 0 {
		meta.Tags["_dd.p.tid"] = fmt.Sprintf("%016x", high)
	}
	keys := make([]string, 0, len(meta.Tags))
	for key := range meta.Tags {
		keys = append(keys, key)
	}
	slices.Sort(keys)
	tags := make([]string, 0, len(keys))
	for _, key := range keys {
		tags = append(tags, key+"="+meta.Tags[key])
	}
	if len(tags) != 0 {
		carrier.Set("x-datadog-tags", strings.Join(tags, ","))
	}

}
func (datadogPropagator) Extract(ctx context.Context, carrier propagation.TextMapCarrier) context.Context {
	ctx = opentelemetry.NewTextMapPropagator().Extract(ctx, carrier)
	members := baggage.FromContext(ctx).Members()
	for _, key := range carrier.Keys() {
		if name, ok := strings.CutPrefix(strings.ToLower(key), "ot-baggage-"); ok {
			if member, err := baggage.NewMemberRaw(name, carrier.Get(key)); err == nil {
				members = append(members, member)
			}
		}
	}
	if bag, err := baggage.New(members...); err == nil {
		ctx = baggage.ContextWithBaggage(ctx, bag)
	}
	if carrier.Get("traceparent") != "" {
		return ctx
	}
	lo, err := strconv.ParseUint(carrier.Get("x-datadog-trace-id"), 10, 64)
	if err != nil || lo == 0 {
		return ctx
	}
	sid, err := strconv.ParseUint(carrier.Get("x-datadog-parent-id"), 10, 64)
	if err != nil || sid == 0 {
		return ctx
	}
	var tid trace.TraceID
	binary.BigEndian.PutUint64(tid[8:], lo)
	for _, tag := range strings.Split(carrier.Get("x-datadog-tags"), ",") {
		if hex, ok := strings.CutPrefix(tag, "_dd.p.tid="); ok {
			hi, err := strconv.ParseUint(hex, 16, 64)
			if err == nil {
				binary.BigEndian.PutUint64(tid[:8], hi)
			}
		}
	}
	meta := ddtracewire.Metadata{Origin: carrier.Get("x-datadog-origin"), Tags: make(map[string]string)}
	if p, err := strconv.Atoi(carrier.Get("x-datadog-sampling-priority")); err == nil && p >= -1 && p <= 2 {
		meta.Priority = p
	}
	for _, tag := range strings.Split(carrier.Get("x-datadog-tags"), ",") {
		if key, value, ok := strings.Cut(tag, "="); ok && strings.HasPrefix(key, "_dd.p.") {
			meta.Tags[key] = value
		}
	}
	var spanID trace.SpanID
	binary.BigEndian.PutUint64(spanID[:], sid)
	flags := trace.TraceFlags(0)
	if meta.Priority > 0 {
		flags = trace.FlagsSampled
	}
	return trace.ContextWithRemoteSpanContext(ctx, trace.NewSpanContext(trace.SpanContextConfig{TraceID: tid, SpanID: spanID, TraceFlags: flags, TraceState: meta.TraceState(trace.TraceState{})}))
}
func (datadogPropagator) Fields() []string {
	return []string{"traceparent", "tracestate", "baggage", "x-datadog-trace-id", "x-datadog-parent-id", "x-datadog-sampling-priority", "x-datadog-origin", "x-datadog-tags"}
}
