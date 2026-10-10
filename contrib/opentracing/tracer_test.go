package opentracing

import (
	"bytes"
	"context"
	"errors"
	"math"
	"net/http"
	"sync"
	"testing"
	"time"

	ot "github.com/opentracing/opentracing-go"
	otext "github.com/opentracing/opentracing-go/ext"
	otlog "github.com/opentracing/opentracing-go/log"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

type collector struct{ spans []sdktrace.ReadOnlySpan }

func (c *collector) ExportSpans(_ context.Context, spans []sdktrace.ReadOnlySpan) error {
	c.spans = append(c.spans, spans...)
	return nil
}
func (*collector) Shutdown(context.Context) error { return nil }

func testTracer(t *testing.T) (ot.Tracer, *collector) {
	t.Helper()
	c := new(collector)
	p := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(sdktrace.NewSimpleSpanProcessor(c)))
	t.Cleanup(func() {
		if err := p.Shutdown(context.Background()); err != nil {
			t.Error(err)
		}
	})
	return NewBridgeTracer(p, nil), c
}

func TestPropagationFormats(t *testing.T) {
	tracer, _ := testTracer(t)
	root := tracer.StartSpan("root")
	defer root.Finish()
	root.SetBaggageItem("tenant", "space and punctuation,;=")
	expected := root.Context().(spanContext)
	for _, format := range []ot.BuiltinFormat{ot.TextMap, ot.HTTPHeaders, ot.Binary} {
		var carrier any
		switch format {
		case ot.TextMap:
			carrier = ot.TextMapCarrier{}
		case ot.HTTPHeaders:
			carrier = ot.HTTPHeadersCarrier(http.Header{})
		case ot.Binary:
			carrier = new(bytes.Buffer)
		}
		if err := tracer.Inject(root.Context(), format, carrier); err != nil {
			t.Fatal(err)
		}
		extracted, err := tracer.Extract(format, carrier)
		if err != nil {
			t.Fatal(err)
		}
		actual := extracted.(spanContext)
		if actual.sc.TraceID() != expected.sc.TraceID() || actual.sc.SpanID() != expected.sc.SpanID() || actual.sc.TraceFlags() != expected.sc.TraceFlags() || !actual.sc.IsRemote() || actual.bag.Member("tenant").Value() != root.BaggageItem("tenant") {
			t.Fatalf("format=%v context=%v", format, actual)
		}
	}
	if _, err := tracer.Extract(ot.TextMap, ot.TextMapCarrier{"application": "unrelated"}); !errors.Is(err, ot.ErrSpanContextNotFound) {
		t.Fatal(err)
	}
	if _, err := tracer.Extract(ot.TextMap, ot.TextMapCarrier{"traceparent": "broken"}); !errors.Is(err, ot.ErrSpanContextCorrupted) {
		t.Fatal(err)
	}
	if _, err := tracer.Extract(ot.Binary, bytes.NewReader([]byte{0xff, 0xff, 0xff, 0xff})); !errors.Is(err, ot.ErrSpanContextCorrupted) {
		t.Fatal(err)
	}
	if _, err := tracer.Extract(ot.Binary, bytes.NewReader([]byte{0, 0, 0, 3, '{'})); !errors.Is(err, ot.ErrSpanContextCorrupted) {
		t.Fatal(err)
	}
	if _, err := tracer.Extract(ot.Binary, bytes.NewReader(nil)); !errors.Is(err, ot.ErrSpanContextNotFound) {
		t.Fatal(err)
	}
	if _, err := tracer.Extract(ot.TextMap, "bad carrier"); !errors.Is(err, ot.ErrInvalidCarrier) {
		t.Fatal(err)
	}
	if err := tracer.Inject(root.Context(), []string{"unknown"}, ot.TextMapCarrier{}); !errors.Is(err, ot.ErrUnsupportedFormat) {
		t.Fatal(err)
	}
}

func TestSpanDataAndContext(t *testing.T) {
	tracer, c := testTracer(t)
	start := time.Unix(1700000000, 123)
	root := tracer.StartSpan("root", ot.StartTime(start))
	root.SetBaggageItem("tenant", "original")
	old := root.Context()
	root.SetBaggageItem("tenant", "new")
	if old.(spanContext).bag.Member("tenant").Value() != "original" {
		t.Fatal("context snapshot changed")
	}
	ctx, cancel := context.WithCancel(context.Background())
	ctx = ot.ContextWithSpan(ctx, root)
	if trace.SpanFromContext(ctx).SpanContext().SpanID() != root.Context().(spanContext).sc.SpanID() {
		t.Fatal("mixed API context lost")
	}
	cancel()
	if ctx.Err() != context.Canceled {
		t.Fatal("context cancellation lost")
	}
	child := tracer.StartSpan("child", ot.FollowsFrom(old), ot.ChildOf(root.Context()), ot.StartTime(start), otext.SpanKindRPCClient)
	child.SetOperationName("renamed").SetTag("large", int64(9007199254740993)).SetTag("unsigned", uint64(math.MaxUint64)).SetTag("infinity", math.Inf(1)).SetTag("error", true).SetTag("ignored", []int{1})
	child.LogFields(otlog.Event("input"), otlog.Lazy(func(e otlog.Encoder) { e.EmitString("lazy", "evaluated") }), otlog.Object("object", struct{ N int }{3}))
	child.LogKV("event", "error", "error.object", errors.New("boom"))
	child.FinishWithOptions(ot.FinishOptions{FinishTime: start.Add(time.Second), LogRecords: []ot.LogRecord{{Timestamp: start.Add(time.Millisecond), Fields: []otlog.Field{otlog.Event("bulk")}}}})
	child.Finish() // must not duplicate the observation
	root.Finish()
	if len(c.spans) != 2 {
		t.Fatalf("spans=%d", len(c.spans))
	}
	s := c.spans[0]
	if s.Name() != "renamed" || s.Parent().SpanID() != root.Context().(spanContext).sc.SpanID() || s.SpanContext().TraceID() != root.Context().(spanContext).sc.TraceID() || s.SpanKind() != trace.SpanKindClient || s.StartTime() != start || s.EndTime() != start.Add(time.Second) || s.Status().Code != codes.Error {
		t.Fatalf("span=%v", s)
	}
	if len(s.Links()) != 1 || s.Links()[0].Attributes[0].Value.AsString() != "follows_from" || len(s.Events()) != 3 || s.Events()[2].Name != "bulk" {
		t.Fatalf("links=%v events=%v", s.Links(), s.Events())
	}
	attrs := map[attribute.Key]attribute.Value{}
	for _, attr := range s.Attributes() {
		attrs[attr.Key] = attr.Value
	}
	if attrs["large"].AsInt64() != 9007199254740993 || attrs["unsigned"].AsString() != "18446744073709551615" || !math.IsInf(attrs["infinity"].AsFloat64(), 1) {
		t.Fatalf("attrs=%v", attrs)
	}
	if _, ok := attrs["ignored"]; ok {
		t.Fatal("unsupported tag retained")
	}
	if child.BaggageItem("tenant") != "new" {
		t.Fatal("child baggage lost")
	}
}

func TestConcurrentContextSnapshots(t *testing.T) {
	tracer, _ := testTracer(t)
	span := tracer.StartSpan("concurrent")
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for n := 0; n < 100; n++ {
				span.SetBaggageItem("tenant", "private")
				span.Context().ForeachBaggageItem(func(k, v string) bool { return true })
				span.BaggageItem("tenant")
				span.SetTag("counter", n)
			}
		}()
	}
	wg.Wait()
	span.Finish()
}
