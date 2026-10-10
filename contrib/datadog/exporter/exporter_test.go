package exporter

import (
	"encoding/json"
	"github.com/DataDog/dd-trace-go/v2/ddtrace/mocktracer"
	"github.com/DataDog/dd-trace-go/v2/ddtrace/tracer"
	"github.com/mfateev/sdk-go-poc/internal/ddtracewire"
	"github.com/mfateev/sdk-go-poc/internal/otelwire"
	"github.com/mfateev/sdk-go-poc/worker"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
	"strings"
	"testing"
)

func TestExportExactIDsAndParent(t *testing.T) {
	host := mocktracer.Start()
	defer host.Stop()
	tid, _ := trace.TraceIDFromHex("12345678901234560000000000000099")
	sid, _ := trace.SpanIDFromHex("0000000000000088")
	pid, _ := trace.SpanIDFromHex("0000000000000077")
	for _, parent := range []trace.SpanID{pid, {}} {
		sc := trace.NewSpanContext(trace.SpanContextConfig{TraceID: tid, SpanID: sid, TraceFlags: trace.FlagsSampled})
		p := trace.NewSpanContext(trace.SpanContextConfig{TraceID: tid, SpanID: parent})
		record := otelwire.Span{Version: 1, Name: "temporal.RunWorkflow", Context: otelwire.FromContext(sc), Parent: otelwire.FromContext(p), Start: 100, End: 200, Status: int(codes.Error), StatusDescription: "failed", Attributes: otelwire.Attributes([]attribute.KeyValue{attribute.String("resource.name", "Work"), attribute.String("temporal.WorkflowID", "workflow")})}
		raw, err := json.Marshal(record)
		if err != nil {
			t.Fatal(err)
		}
		NewSinkHandler(Options{Service: "test"})(worker.SinkEvent{Payload: raw})
		finished := host.FinishedSpans()
		if len(finished) != 1 {
			t.Fatalf("finished spans=%d", len(finished))
		}
		got := finished[0]
		wantParent := uint64(0)
		if parent.IsValid() {
			wantParent = 0x77
		}
		if got.SpanID() != 0x88 || got.ParentID() != wantParent || got.Context().TraceIDBytes() != [16]byte(tid) || got.OperationName() != "temporal.RunWorkflow" || got.Tag("resource.name") != "Work" || got.StartTime().UnixNano() != 100 || got.FinishTime().UnixNano() != 200 {
			t.Fatalf("export changed IDs, parent or data: %v", got)
		}
		host.Reset()
	}
}

func TestPropagatedDatadogMetadata(t *testing.T) {
	tid, _ := trace.TraceIDFromHex("12345678901234560000000000000099")
	meta := ddtracewire.Metadata{Priority: 2, Origin: "synthetics", Tags: map[string]string{"_dd.p.dm": "-4", "_dd.p.tid": "1234567890123456"}}
	ctx := tracer.FromGenericCtx(spanContext{traceID: tid, spanID: 0x77, sampled: true, metadata: meta})
	carrier := tracer.TextMapCarrier{}
	if err := tracer.NewPropagator(&tracer.PropagatorConfig{MaxTagsHeaderLen: 512}).Inject(ctx, carrier); err != nil {
		t.Fatal(err)
	}
	if carrier["x-datadog-sampling-priority"] != "2" || carrier["x-datadog-origin"] != "synthetics" {
		t.Fatalf("metadata lost: %v", carrier)
	}
	if !strings.Contains(carrier["x-datadog-tags"], "_dd.p.dm=-4") {
		t.Fatalf("tags lost: %v", carrier)
	}
}
