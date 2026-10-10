package tracing

import (
	"context"
	"github.com/mfateev/sdk-go-poc/interceptor"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
	"testing"
)

func TestLegacyDatadogHeaders(t *testing.T) {
	native := NewTracer(TracerOptions{SinkOp: 0x10200})
	parent, err := native.UnmarshalSpan(map[string]string{"x-datadog-trace-id": "153", "x-datadog-parent-id": "119", "x-datadog-tags": "_dd.p.dm=-4,_dd.p.tid=1234567890123456", "x-datadog-sampling-priority": "2", "x-datadog-origin": "synthetics", "ot-baggage-tenant": "example"})
	if err != nil || parent == nil {
		t.Fatalf("parent=%v err=%v", parent, err)
	}
	span, err := native.StartSpan(&interceptor.TracerStartSpanOptions{Operation: "RunWorkflow", Name: "Work", Parent: parent})
	if err != nil {
		t.Fatal(err)
	}
	carrier, err := native.MarshalSpan(span)
	if err != nil {
		t.Fatal(err)
	}
	if carrier["ot-baggage-tenant"] != "example" || carrier["x-datadog-sampling-priority"] != "2" || carrier["x-datadog-origin"] != "synthetics" || carrier["x-datadog-trace-id"] != "153" || carrier["x-datadog-tags"] != "_dd.p.dm=-4,_dd.p.tid=1234567890123456" {
		t.Fatalf("carrier=%v", carrier)
	}
	ctx := datadogPropagator{}.Extract(context.Background(), propagation.MapCarrier(carrier))
	if trace.SpanContextFromContext(ctx).TraceID().String() != "12345678901234560000000000000099" {
		t.Fatal("128-bit trace lost")
	}
	if _, err = native.UnmarshalSpan(map[string]string{"x-datadog-trace-id": "invalid", "x-datadog-parent-id": "7"}); err == nil {
		t.Fatal("malformed context accepted")
	}
}
