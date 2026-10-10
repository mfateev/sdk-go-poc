// Adapted from Temporal OpenTracing contrib v0.3.0; see SDK_LICENSE.
// Package opentracing provides OpenTracing utilities.
package opentracing

import (
	"context"
	"errors"
	"fmt"

	"github.com/opentracing/opentracing-go"

	"github.com/mfateev/sdk-go-poc/interceptor"
)

// TracerOptions are options provided to NewInterceptor or NewTracer.
type TracerOptions struct {
	// SinkOp routes completed spans to the host. Used when Tracer is nil.
	SinkOp uint32

	// Tracer is an isolate-owned tracer. Defaults to NewSinkTracer(SinkOp).
	Tracer opentracing.Tracer

	// DisableSignalTracing can be set to disable signal tracing.
	DisableSignalTracing bool

	// DisableQueryTracing can be set to disable query tracing.
	DisableQueryTracing bool

	DisableUpdateTracing    bool
	AllowInvalidParentSpans bool

	// SpanContextKey is the context key used for internal span tracking (not to
	// be confused with the context key OpenTracing uses internally). If not set,
	// this defaults to an internal key (recommended).
	SpanContextKey interface{}

	// HeaderKey is the Temporal header field key used to serialize spans. If
	// empty, this defaults to the one used by all SDKs (recommended).
	HeaderKey string

	// SpanStarter is a callback to create spans. If not set, this creates normal
	// OpenTracing spans calling Tracer.StartSpan.
	SpanStarter func(t opentracing.Tracer, operationName string, opts ...opentracing.StartSpanOption) opentracing.Span
}

type spanContextKey struct{}

const defaultHeaderKey = "_tracer-data"

type tracer struct {
	interceptor.BaseTracer
	options *TracerOptions
}

// NewTracer creates a tracer with the given options. Most callers should use
// NewInterceptor instead.
func NewTracer(options TracerOptions) (interceptor.Tracer, error) {
	if options.Tracer == nil {
		options.Tracer = NewSinkTracer(options.SinkOp)
	}
	if options.SpanContextKey == nil {
		options.SpanContextKey = spanContextKey{}
	}
	if options.HeaderKey == "" {
		options.HeaderKey = defaultHeaderKey
	}

	return &tracer{options: &options}, nil
}

// NewInterceptor creates an interceptor for an isolate interceptor factory.
// Use the upstream adapter separately for ordinary client/activity interception.
func NewInterceptor(options TracerOptions) (interceptor.WorkerInterceptor, error) {
	t, err := NewTracer(options)
	if err != nil {
		return nil, err
	}
	return interceptor.NewTracingInterceptor(t), nil
}

func (t *tracer) Options() interceptor.TracerOptions {
	return interceptor.TracerOptions{
		SpanContextKey:          t.options.SpanContextKey,
		HeaderKey:               t.options.HeaderKey,
		DisableSignalTracing:    t.options.DisableSignalTracing,
		DisableQueryTracing:     t.options.DisableQueryTracing,
		DisableUpdateTracing:    t.options.DisableUpdateTracing,
		AllowInvalidParentSpans: t.options.AllowInvalidParentSpans,
	}
}

func (t *tracer) UnmarshalSpan(m map[string]string) (interceptor.TracerSpanRef, error) {
	ctx, err := t.options.Tracer.Extract(opentracing.TextMap, opentracing.TextMapCarrier(m))
	if errors.Is(err, opentracing.ErrSpanContextNotFound) {
		// If there is no span, return nothing, but don't error out. This is
		// a legitimate place where a span does not exist in the headers
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &tracerSpanRef{SpanContext: ctx}, nil
}

func (t *tracer) MarshalSpan(span interceptor.TracerSpan) (map[string]string, error) {
	data := opentracing.TextMapCarrier{}
	if err := t.options.Tracer.Inject(span.(*tracerSpan).Context(), opentracing.TextMap, data); err != nil {
		return nil, err
	}
	return map[string]string(data), nil
}

func (t *tracer) SpanFromContext(ctx context.Context) interceptor.TracerSpan {
	span := opentracing.SpanFromContext(ctx)
	if span == nil {
		return nil
	}
	return &tracerSpan{Span: span}
}

func (t *tracer) ContextWithSpan(ctx context.Context, span interceptor.TracerSpan) context.Context {
	return opentracing.ContextWithSpan(ctx, span.(*tracerSpan).Span)
}

func (t *tracer) StartSpan(opts *interceptor.TracerStartSpanOptions) (interceptor.TracerSpan, error) {
	return t.StartSpanWithContext(context.Background(), opts)
}

func (t *tracer) StartSpanWithContext(ctx context.Context, opts *interceptor.TracerStartSpanOptions) (interceptor.TracerSpan, error) {
	// Build start options
	startOpts := []opentracing.StartSpanOption{
		opentracing.StartTime(opts.Time),
	}

	// Link parent
	var parent opentracing.SpanContext
	switch optParent := opts.Parent.(type) {
	case nil:
	case *tracerSpan:
		parent = optParent.Context()
	case *tracerSpanRef:
		parent = optParent.SpanContext
	default:
		return nil, fmt.Errorf("unrecognized parent type %T", optParent)
	}
	if parent != nil {
		if opts.DependedOn {
			startOpts = append(startOpts, opentracing.ChildOf(parent))
		} else {
			startOpts = append(startOpts, opentracing.FollowsFrom(parent))
		}
	}

	// Set tags
	if len(opts.Tags) > 0 {
		tags := make(opentracing.Tags, len(opts.Tags))
		for k, v := range opts.Tags {
			tags[k] = v
		}
		startOpts = append(startOpts, tags)
	}

	// A default sink tracer needs the native context's observation scope for
	// query/validator span uniqueness. Custom starters retain the upstream API.
	if backend, ok := t.options.Tracer.(*bridgeTracer); ok && t.options.SpanStarter == nil {
		return &tracerSpan{Span: backend.startSpan(ctx, opts.Operation+":"+opts.Name, startOpts...)}, nil
	}
	// Start
	if t.options.SpanStarter == nil {
		return &tracerSpan{Span: t.options.Tracer.StartSpan(opts.Operation+":"+opts.Name, startOpts...)}, nil
	}
	return &tracerSpan{Span: t.options.SpanStarter(t.options.Tracer, opts.Operation+":"+opts.Name, startOpts...)}, nil
}

type tracerSpanRef struct{ opentracing.SpanContext }

type tracerSpan struct{ opentracing.Span }

func (t *tracerSpan) Finish(opts *interceptor.TracerFinishSpanOptions) {
	if opts.Error != nil {
		// Standard tag that can be bridged to OpenTelemetry
		t.SetTag("error", true)
		t.LogKV("event", "error", "error.object", opts.Error)
	}
	t.Span.Finish()
}
