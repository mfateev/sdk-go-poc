// Adapted from Temporal OTel v2 contrib v0.1.0; see SDK_LICENSE.
package opentelemetry

import (
	"go.opentelemetry.io/otel/baggage"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/mfateev/sdk-go-poc/interceptor/tracing"
	"go.temporal.io/sdk/temporal"
)

type tracerSpan struct {
	trace.Span
	baggage.Baggage
}

var _ tracing.TracerSpan = (*tracerSpan)(nil)
var _ tracing.TracerSpanRef = (*tracerSpan)(nil)

func (t *tracerSpan) Finish(opts *tracing.TracerFinishSpanOptions) {
	if !t.Span.IsRecording() {
		return
	}

	if opts.Error != nil {
		t.RecordError(opts.Error)

		// Benign application errors do not mark spans as failed.
		appError, _ := opts.Error.(*temporal.ApplicationError)
		isBenign := appError != nil && appError.Category() == temporal.ApplicationErrorCategoryBenign
		if !isBenign {
			t.SetStatus(codes.Error, opts.Error.Error())
		}
	}
	t.End()
}

func asTracerSpan(ref tracing.TracerSpanRef) *tracerSpan { span, _ := ref.(*tracerSpan); return span }
