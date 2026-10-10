// Adapted from Temporal OTel v2 contrib v0.1.0; see SDK_LICENSE.
package opentelemetry

import (
	"context"
	"github.com/mfateev/sdk-go-poc/internal/otelcontext"

	"go.opentelemetry.io/otel/baggage"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"

	"github.com/mfateev/sdk-go-poc/interceptor/tracing"
)

type spanContextKey struct{}

// contextBridge stores spans and optional baggage on context.Context.
type contextBridge struct {
	options *options
}

func (b *contextBridge) SpanFromContext(ctx context.Context) tracing.TracerSpanRef {
	res := &tracerSpan{}

	spanCtx := trace.SpanContextFromContext(otelcontext.WithFallback(ctx))
	if spanCtx.IsValid() {
		res.Span = trace.SpanFromContext(otelcontext.WithFallback(ctx))
	} else {
		res.Span = noop.Span{}
	}

	bag := baggage.FromContext(ctx)
	if !b.options.DisableBaggage && bag.Len() > 0 {
		res.Baggage = bag
	}

	return res
}

func (b *contextBridge) ContextWithSpan(ctx context.Context, ref tracing.TracerSpanRef) context.Context {
	span := asTracerSpan(ref)
	if span != nil && span.Span != nil && span.SpanContext().IsValid() {
		ctx = trace.ContextWithSpan(ctx, span.Span)
	}

	if !b.options.DisableBaggage && span != nil && span.Baggage.Len() > 0 {
		ctx = baggage.ContextWithBaggage(ctx, span.Baggage)
	}

	return ctx
}
