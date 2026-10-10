// Package exporter exports copied native workflow spans using the host Datadog tracer.
package exporter

import (
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/DataDog/dd-trace-go/v2/ddtrace/tracer"
	otelexport "github.com/mfateev/sdk-go-poc/contrib/opentelemetry/exporter"
	"github.com/mfateev/sdk-go-poc/internal/ddtracewire"
	"github.com/mfateev/sdk-go-poc/worker"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
	"strings"
)

type Options struct {
	Service string
	// OnFinish runs exclusively on the host. It can add Datadog finish options
	// without importing the Datadog tracer or its global state into workflow code.
	OnFinish func(error) []tracer.FinishOption
	// SpanStarter may be supplied for testing or a configured host tracer.
	SpanStarter func(string, ...tracer.StartSpanOption) *tracer.Span
}

func Register(w any, op uint32, opts Options) error {
	return worker.RegisterSink(w, op, NewSinkHandler(opts), worker.SinkOptions{Name: "datadog"})
}

// NewSinkHandler provides the host handler for alternative worker sink wiring.
func NewSinkHandler(opts Options) worker.SinkHandler {
	starter := opts.SpanStarter
	if starter == nil {
		starter = tracer.StartSpan
	}
	return func(event worker.SinkEvent) {
		span, err := otelexport.Decode(event.Payload, nil)
		if err != nil {
			panic(fmt.Sprintf("datadog: invalid span: %v", err))
		}
		sc := span.SpanContext()
		tid := sc.TraceID()
		sid := sc.SpanID()
		parent := span.Parent().SpanID()
		// FromGenericCtx accepts an exact copied trace with a zero parent for a
		// root. WithSpanID supplies the deterministic ID rather than generating one.
		parentCtx := tracer.FromGenericCtx(spanContext{traceID: tid, spanID: binary.BigEndian.Uint64(parent[:]), sampled: sc.IsSampled(), metadata: ddtracewire.FromContext(sc)})
		options := []tracer.StartSpanOption{tracer.WithSpanID(binary.BigEndian.Uint64(sid[:])), tracer.ChildOf(parentCtx), tracer.StartTime(span.StartTime()), tracer.ServiceName(opts.Service)}
		for _, kv := range span.Attributes() {
			key := string(kv.Key)
			if key == "resource.name" {
				options = append(options, tracer.ResourceName(kv.Value.AsString()))
				continue
			}
			if strings.HasPrefix(key, "temporal") && !strings.HasPrefix(key, "temporal.") {
				continue
			}
			options = append(options, tracer.Tag(key, kv.Value.AsInterface()))
		}
		started := starter(span.Name(), options...)
		if started == nil {
			return
		}
		var cause error
		if span.Status().Code == codes.Error {
			cause = errors.New(span.Status().Description)
		}
		finish := []tracer.FinishOption{tracer.FinishTime(span.EndTime())}
		if opts.OnFinish != nil {
			finish = append(finish, opts.OnFinish(cause)...)
		} else if cause != nil {
			finish = append(finish, tracer.WithError(cause))
		}
		started.Finish(finish...)
	}
}

type spanContext struct {
	traceID  trace.TraceID
	spanID   uint64
	sampled  bool
	metadata ddtracewire.Metadata
}

func (c spanContext) SpanID() uint64                               { return c.spanID }
func (c spanContext) TraceID() string                              { return c.traceID.String() }
func (c spanContext) TraceIDBytes() [16]byte                       { return c.traceID }
func (c spanContext) TraceIDLower() uint64                         { return binary.BigEndian.Uint64(c.traceID[8:]) }
func (c spanContext) ForeachBaggageItem(func(string, string) bool) {}

// Datadog's public generic context adapter recognizes these methods to retain
// the propagated sampling decision without invoking a sampler inside isolates.
func (c spanContext) SamplingDecision() uint32 {
	if c.sampled {
		return 2
	}
	return 1
}
func (c spanContext) Priority() *float64 {
	value := float64(c.metadata.Priority)
	return &value
}

func (c spanContext) Origin() string                     { return c.metadata.Origin }
func (c spanContext) PropagatingTags() map[string]string { return c.metadata.Tags }
func (c spanContext) Tags() map[string]string            { return nil }
