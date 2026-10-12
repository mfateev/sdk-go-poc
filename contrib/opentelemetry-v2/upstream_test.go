package opentelemetry

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/mfateev/sdk-go-poc/interceptor/tracing"
	"go.opentelemetry.io/otel/baggage"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
	"go.temporal.io/sdk/temporal"
)

func upstreamSpan() *tracerSpan {
	sc := trace.NewSpanContext(trace.SpanContextConfig{TraceID: trace.TraceID{1}, SpanID: trace.SpanID{2}})
	return &tracerSpan{Span: trace.SpanFromContext(trace.ContextWithSpanContext(context.Background(), sc))}
}

type unknownSpan struct{}

func (unknownSpan) Finish(*tracing.TracerFinishSpanOptions) {}

func TestUpstreamContextBridge(t *testing.T) {
	bridge := &contextBridge{options: &options{}}
	ambient, err := baggage.Parse("key=ambient")
	if err != nil {
		t.Fatal(err)
	}
	current, err := baggage.Parse("key=current")
	if err != nil {
		t.Fatal(err)
	}
	base := baggage.ContextWithBaggage(context.Background(), ambient)
	for _, ref := range []tracing.TracerSpanRef{nil, unknownSpan{}, &tracerSpan{Span: trace.SpanFromContext(context.Background())}} {
		ctx := bridge.ContextWithSpan(base, ref)
		if baggage.FromContext(ctx).Member("key").Value() != "ambient" {
			t.Fatalf("%T removed ambient baggage", ref)
		}
		span := bridge.SpanFromContext(ctx).(*tracerSpan)
		if span.Span == nil || span.SpanContext().IsValid() {
			t.Fatalf("%T did not return a no-op span", ref)
		}
	}
	ctx := bridge.ContextWithSpan(base, &tracerSpan{Baggage: current})
	if baggage.FromContext(ctx).Member("key").Value() != "current" {
		t.Fatal("span baggage did not replace ambient baggage")
	}
	ctx = bridge.ContextWithSpan(context.Background(), &tracerSpan{Baggage: current})
	if baggage.FromContext(ctx).Member("key").Value() != "current" {
		t.Fatal("baggage not enabled by default")
	}
	span := upstreamSpan()
	span.Baggage = current
	ctx = bridge.ContextWithSpan(context.Background(), span)
	for _, ref := range []tracing.TracerSpanRef{nil, unknownSpan{}} {
		stored := bridge.SpanFromContext(bridge.ContextWithSpan(ctx, ref)).(*tracerSpan)
		if !stored.SpanContext().Equal(span.SpanContext()) || stored.Baggage.Member("key").Value() != "current" {
			t.Fatalf("%T replaced stored span", ref)
		}
	}
	if bridge.SpanFromContext(context.Background()).(*tracerSpan).SpanContext().IsValid() {
		t.Fatal("mutated parent context")
	}
	disabled := &contextBridge{options: &options{TracerOptions: TracerOptions{DisableBaggage: true}}}
	if baggage.FromContext(disabled.ContextWithSpan(base, span)).Member("key").Value() != "ambient" {
		t.Fatal("disabled bridge changed ambient baggage")
	}
}

type upstreamPropagator struct {
	sc  trace.SpanContext
	bag baggage.Baggage
}

func (p upstreamPropagator) Inject(ctx context.Context, c propagation.TextMapCarrier) {
	sc := trace.SpanContextFromContext(ctx)
	if sc.IsValid() && sc.TraceID() == p.sc.TraceID() && sc.SpanID() == p.sc.SpanID() {
		c.Set("span", "ok")
	}
	if baggage.FromContext(ctx).Len() > 0 {
		c.Set("baggage", "ok")
	}
}
func (p upstreamPropagator) Extract(ctx context.Context, c propagation.TextMapCarrier) context.Context {
	if c.Get("span") == "ok" {
		ctx = trace.ContextWithRemoteSpanContext(ctx, p.sc)
	}
	if c.Get("baggage") == "ok" {
		ctx = baggage.ContextWithBaggage(ctx, p.bag)
	}
	return ctx
}
func (upstreamPropagator) Fields() []string { return []string{"span", "baggage"} }

func TestUpstreamSpanCodec(t *testing.T) {
	bag, err := baggage.Parse("key=value")
	if err != nil {
		t.Fatal(err)
	}
	valid := upstreamSpan()
	invalid := &tracerSpan{Span: trace.SpanFromContext(trace.ContextWithSpanContext(context.Background(), trace.NewSpanContext(trace.SpanContextConfig{TraceID: trace.TraceID{1}})))}
	for _, tc := range []struct {
		name     string
		span     *tracerSpan
		disabled bool
		headers  map[string]string
	}{
		{"success", valid, false, map[string]string{"span": "ok", "baggage": "ok"}},
		{"invalid", invalid, false, map[string]string{"baggage": "ok"}},
		{"disabled", valid, true, map[string]string{"span": "ok"}},
		{"only-span", valid, false, map[string]string{"span": "ok"}},
		{"only-baggage", &tracerSpan{}, false, map[string]string{"baggage": "ok"}},
		{"empty", &tracerSpan{}, false, map[string]string{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			span := *tc.span
			if tc.name != "only-span" && tc.name != "empty" {
				span.Baggage = bag
			}
			prop := upstreamPropagator{sc: valid.SpanContext(), bag: bag}
			codec := &spanCodec{contextBridge: &contextBridge{options: &options{TracerOptions: TracerOptions{DisableBaggage: tc.disabled, TextMapPropagator: prop}}}}
			headers, err := codec.MarshalSpan(&span)
			if err != nil || !reflect.DeepEqual(headers, tc.headers) {
				t.Fatalf("marshal=%v error=%v want=%v", headers, err, tc.headers)
			}
			ref, err := codec.UnmarshalSpan(headers)
			if err != nil {
				t.Fatal(err)
			}
			decoded := ref.(*tracerSpan)
			if decoded.SpanContext().IsValid() != (headers["span"] == "ok") || decoded.Baggage.Len() != lenBag(tc.headers, tc.disabled) {
				t.Fatalf("unmarshal=%+v", decoded)
			}
		})
	}
	for _, tc := range []struct {
		name            string
		prop            upstreamPropagator
		headers         map[string]string
		disabled, valid bool
		bags            int
	}{
		{"malformed-headers", upstreamPropagator{sc: valid.SpanContext(), bag: bag}, map[string]string{"s_p_a_n": "ok", "b_a_g_g_a_g_e": "ok"}, false, false, 0},
		{"malformed-span", upstreamPropagator{sc: invalid.SpanContext(), bag: bag}, map[string]string{"span": "ok", "baggage": "ok"}, false, false, 1},
		{"baggage-disabled", upstreamPropagator{sc: valid.SpanContext(), bag: bag}, map[string]string{"span": "ok", "baggage": "ok"}, true, true, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			codec := &spanCodec{contextBridge: &contextBridge{options: &options{TracerOptions: TracerOptions{DisableBaggage: tc.disabled, TextMapPropagator: tc.prop}}}}
			ref, err := codec.UnmarshalSpan(tc.headers)
			if err != nil {
				t.Fatal(err)
			}
			span := ref.(*tracerSpan)
			if span.SpanContext().IsValid() != tc.valid || span.Baggage.Len() != tc.bags {
				t.Fatalf("unmarshal=%+v", span)
			}
		})
	}
}
func lenBag(headers map[string]string, disabled bool) int {
	if headers["baggage"] == "ok" && !disabled {
		return 1
	}
	return 0
}

func TestUpstreamTextMapCarrier(t *testing.T) {
	carrier := textMapCarrier{"traceparent": "value"}
	if carrier.Get("TraceParent") != "value" {
		t.Fatal("carrier is not case insensitive")
	}
	carrier.Set("Baggage", "value")
	if carrier["Baggage"] != "value" || carrier["baggage"] != "" || carrier.Get("baggage") != "value" {
		t.Fatal("carrier did not preserve key case")
	}
}

type captureLogger struct{ fields []any }

func (*captureLogger) Debug(string, ...any)           {}
func (l *captureLogger) Info(_ string, fields ...any) { l.fields = fields }
func (*captureLogger) Warn(string, ...any)            {}
func (*captureLogger) Error(string, ...any)           {}
func TestUpstreamGetLogger(t *testing.T) {
	native := NewTracer(TracerOptions{SinkOp: 0x10200}).(*nativeTracer)
	span := upstreamSpan()
	logger := new(captureLogger)
	native.GetLogger(logger, span).Info("message")
	expected := []any{"TraceID", span.SpanContext().TraceID(), "SpanID", span.SpanContext().SpanID()}
	if !reflect.DeepEqual(logger.fields, expected) {
		t.Fatalf("logger fields=%v", logger.fields)
	}
	for _, ref := range []tracing.TracerSpanRef{nil, unknownSpan{}, &tracerSpan{}} {
		logger.fields = nil
		native.GetLogger(logger, ref).Info("message")
		if len(logger.fields) != 0 {
			t.Fatalf("invalid span %T logged trace fields", ref)
		}
	}
}

func TestUpstreamSpanKindAndErrorStatus(t *testing.T) {
	for _, direction := range []tracing.SpanDirection{tracing.SpanDirectionInbound, tracing.SpanDirectionOutbound} {
		for _, cause := range []error{nil, errors.New("unexpected"), temporal.NewApplicationErrorWithOptions("benign", "Benign", temporal.ApplicationErrorOptions{Category: temporal.ApplicationErrorCategoryBenign})} {
			recorder := tracetest.NewSpanRecorder()
			provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
			native := NewTracer(TracerOptions{Tracer: provider.Tracer("upstream")})
			span := native.CreateSpan(context.Background(), &tracing.TracerStartSpanOptions{Operation: "Test", Name: "span", Direction: direction})
			span.Finish(&tracing.TracerFinishSpanOptions{Error: cause})
			ended := recorder.Ended()
			if len(ended) != 1 {
				t.Fatalf("ended=%d", len(ended))
			}
			kind := trace.SpanKindServer
			if direction == tracing.SpanDirectionOutbound {
				kind = trace.SpanKindClient
			}
			status := codes.Unset
			if cause != nil && cause.Error() == "unexpected" {
				status = codes.Error
			}
			if ended[0].SpanKind() != kind || ended[0].Status().Code != status {
				t.Fatalf("kind=%v status=%v", ended[0].SpanKind(), ended[0].Status())
			}
			if err := provider.Shutdown(context.Background()); err != nil {
				t.Fatal(err)
			}
		}
	}
}
