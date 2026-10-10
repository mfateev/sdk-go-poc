// Package otelcontext keeps empty span contexts private inside isolates.
package otelcontext

import (
	"context"
	"go.opentelemetry.io/otel/trace"
)

// WithFallback preserves all existing values and cancellation, supplying a
// private non-recording span when no span is installed. The OTel API otherwise
// returns a process-global no-op receiver. No host globals or context keys are
// inspected; the standard ContextWithSpanContext API constructs the fallback.
func WithFallback(ctx context.Context) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return fallbackContext{Context: ctx, fallback: trace.ContextWithSpanContext(context.Background(), trace.SpanContext{})}
}

type fallbackContext struct {
	context.Context
	fallback context.Context
}

func (c fallbackContext) Value(key any) any {
	if value := c.Context.Value(key); value != nil {
		return value
	}
	return c.fallback.Value(key)
}
