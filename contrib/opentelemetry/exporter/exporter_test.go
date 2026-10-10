package exporter

import (
	"encoding/json"
	"github.com/mfateev/sdk-go-poc/internal/otelwire"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
	"math"
	"testing"
)

func TestDecodeRetainsSpanData(t *testing.T) {
	tid, _ := trace.TraceIDFromHex("12345678901234567890123456789012")
	sid, _ := trace.SpanIDFromHex("1234567890123456")
	parent, _ := trace.SpanIDFromHex("6543210987654321")
	state, _ := trace.ParseTraceState("vendor=value")
	sc := trace.NewSpanContext(trace.SpanContextConfig{TraceID: tid, SpanID: sid, TraceFlags: trace.FlagsSampled, TraceState: state})
	p := trace.NewSpanContext(trace.SpanContextConfig{TraceID: tid, SpanID: parent, Remote: true})
	attrs := otelwire.Attributes([]attribute.KeyValue{attribute.Int64("large", 9007199254740993), attribute.Float64("nan", math.Float64frombits(0x7ff8000000000005)), attribute.KeyValue{Key: "nested", Value: attribute.SliceValue(attribute.ByteSliceValue([]byte{0, 255}), attribute.Float64SliceValue([]float64{math.Inf(1), math.Inf(-1)}))}})
	record := otelwire.Span{Version: 1, Name: "test", Context: otelwire.FromContext(sc), Parent: otelwire.FromContext(p), Kind: trace.SpanKindClient, Start: 100, End: 200, Attributes: attrs, Events: []otelwire.Event{{Name: "event", Time: 150, Attributes: attrs}}, Links: []otelwire.Link{{Context: otelwire.FromContext(p), Attributes: attrs}}, Status: int(codes.Error), StatusDescription: "failure", ScopeName: "scope", ScopeVersion: "1", SchemaURL: "schema"}
	raw, err := json.Marshal(record)
	if err != nil {
		t.Fatal(err)
	}
	got, err := Decode(raw, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !got.SpanContext().Equal(sc) || !got.Parent().Equal(p) || got.StartTime().UnixNano() != 100 || got.EndTime().UnixNano() != 200 || got.SpanKind() != trace.SpanKindClient || got.Status().Code != codes.Error || got.InstrumentationScope().Name != "scope" || len(got.Events()) != 1 || len(got.Links()) != 1 {
		t.Fatalf("span changed: %v", got)
	}
	values := got.Attributes()
	if values[0].Value.AsInt64() != 9007199254740993 || math.Float64bits(values[1].Value.AsFloat64()) != 0x7ff8000000000005 {
		t.Fatal("attribute precision lost")
	}
	nested := values[2].Value.AsSlice()
	if nested[0].AsByteSlice()[1] != 255 || !math.IsInf(nested[1].AsFloat64Slice()[0], 1) {
		t.Fatal("nested values lost")
	}
}
func TestDecodeRejectsInvalidSpan(t *testing.T) {
	for _, raw := range []string{"{}", "null", "{", "{\"Version\":2}", "{\"Version\":1,\"Context\":{\"TraceID\":\"invalid\",\"SpanID\":\"1234567890123456\"}}"} {
		if _, err := Decode([]byte(raw), nil); err == nil {
			t.Fatalf("accepted %s", raw)
		}
	}
}
