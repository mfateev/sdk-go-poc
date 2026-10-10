package opentelemetry

import (
	"context"
	"github.com/mfateev/sdk-go-poc/internal/otelcontext"
	"go.opentelemetry.io/otel/baggage"
	"go.opentelemetry.io/otel/propagation"
)

// NewTextMapPropagator uses the standard W3C trace context and baggage formats.
// Invalid baggage is discarded without invoking a process-global OTel error
// handler. Custom propagators are allowed if their implementation is isolate-safe.
func NewTextMapPropagator() propagation.TextMapPropagator {
	return privatePropagator{propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, privateBaggage{})}
}

type privateBaggage struct{}

func (privateBaggage) Inject(ctx context.Context, carrier propagation.TextMapCarrier) {
	if text := baggage.FromContext(ctx).String(); text != "" {
		carrier.Set("baggage", text)
	}
}
func (privateBaggage) Extract(ctx context.Context, carrier propagation.TextMapCarrier) context.Context {
	bag, err := baggage.Parse(carrier.Get("baggage"))
	if err != nil {
		return ctx
	}
	return baggage.ContextWithBaggage(ctx, bag)
}
func (privateBaggage) Fields() []string { return []string{"baggage"} }

// Protect the empty-parent path used by the standard TraceContext propagator.
type privatePropagator struct{ propagation.TextMapPropagator }

func (p privatePropagator) Inject(ctx context.Context, carrier propagation.TextMapCarrier) {
	p.TextMapPropagator.Inject(otelcontext.WithFallback(ctx), carrier)
}
func (p privatePropagator) Extract(ctx context.Context, carrier propagation.TextMapCarrier) context.Context {
	return p.TextMapPropagator.Extract(otelcontext.WithFallback(ctx), carrier)
}
