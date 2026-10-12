package opentelemetry

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/mfateev/sdk-go-poc/interceptor"
	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
	"go.temporal.io/sdk/temporal"
)

func TestUpstreamSpanKindStatusAndTimestamps(t *testing.T) {
	for _, outbound := range []bool{false, true} {
		for _, cause := range []error{nil, errors.New("unexpected"), temporal.NewApplicationErrorWithOptions("benign", "Benign", temporal.ApplicationErrorOptions{Category: temporal.ApplicationErrorCategoryBenign})} {
			recorder := tracetest.NewSpanRecorder()
			provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
			native, err := NewTracer(TracerOptions{Tracer: provider.Tracer("upstream")})
			if err != nil {
				t.Fatal(err)
			}
			started := time.Date(1969, 7, 20, 20, 17, 0, 0, time.UTC)
			span, err := native.StartSpan(&interceptor.TracerStartSpanOptions{Operation: "Test", Name: "span", Time: started, ToHeader: outbound, FromHeader: !outbound})
			if err != nil {
				t.Fatal(err)
			}
			span.Finish(&interceptor.TracerFinishSpanOptions{Error: cause})
			ended := recorder.Ended()
			if len(ended) != 1 {
				t.Fatalf("ended=%d", len(ended))
			}
			kind, status := trace.SpanKindServer, codes.Unset
			if outbound {
				kind = trace.SpanKindClient
			}
			if cause != nil && cause.Error() == "unexpected" {
				status = codes.Error
			}
			if ended[0].SpanKind() != kind || ended[0].Status().Code != status || ended[0].StartTime() != started {
				t.Fatalf("span kind=%v status=%v start=%v", ended[0].SpanKind(), ended[0].Status(), ended[0].StartTime())
			}
			if err := provider.Shutdown(context.Background()); err != nil {
				t.Fatal(err)
			}
		}
	}
}
func TestUpstreamSpanFromWorkflowContext(t *testing.T) {
	span, ok := SpanFromWorkflowContext(context.Background())
	if ok || span == nil || span.SpanContext().IsValid() {
		t.Fatal("empty context must return private no-op span")
	}
	sc := trace.NewSpanContext(trace.SpanContextConfig{TraceID: trace.TraceID{1}, SpanID: trace.SpanID{2}})
	native := trace.SpanFromContext(trace.ContextWithSpanContext(context.Background(), sc))
	ctx := context.WithValue(context.Background(), spanContextKey{}, &tracerSpan{Span: native})
	got, ok := SpanFromWorkflowContext(ctx)
	if !ok || !got.SpanContext().Equal(native.SpanContext()) {
		t.Fatal("did not extract known native span")
	}
	got, ok = SpanFromWorkflowContext(trace.ContextWithSpan(context.Background(), native))
	if !ok || !got.SpanContext().Equal(sc) {
		t.Fatal("standard context span was not found")
	}
}

type captureLogger struct{ fields []any }

func (*captureLogger) Debug(string, ...any)           {}
func (l *captureLogger) Info(_ string, fields ...any) { l.fields = fields }
func (*captureLogger) Warn(string, ...any)            {}
func (*captureLogger) Error(string, ...any)           {}
func TestUpstreamLogFields(t *testing.T) {
	native, err := NewTracer(TracerOptions{SinkOp: 0x10200})
	if err != nil {
		t.Fatal(err)
	}
	sc := trace.NewSpanContext(trace.SpanContextConfig{TraceID: trace.TraceID{1}, SpanID: trace.SpanID{2}})
	span := &tracerSpan{Span: trace.SpanFromContext(trace.ContextWithSpanContext(context.Background(), sc))}
	logger := new(captureLogger)
	native.GetLogger(logger, span).Info("message")
	if !reflect.DeepEqual(logger.fields, []any{"TraceID", sc.TraceID(), "SpanID", sc.SpanID()}) {
		t.Fatalf("log fields=%v", logger.fields)
	}
}
