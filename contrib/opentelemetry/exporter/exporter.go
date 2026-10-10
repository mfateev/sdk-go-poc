// Package exporter bridges native workflow spans to an ordinary host exporter.
package exporter

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/mfateev/sdk-go-poc/internal/otelwire"
	"github.com/mfateev/sdk-go-poc/worker"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/sdk/instrumentation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
	"time"
)

// Register creates a bounded host batch processor and registers the matching
// byte sink. Its returned shutdown function flushes observations after workers
// stop. Workflow DataConverters/codecs never see tracing bytes. A provider
// configured for ordinary client/activity tracing can use the same exporter
// only if that exporter supports concurrent use and shared shutdown ownership.
func Register(w any, op uint32, exporter sdktrace.SpanExporter, resources *resource.Resource, opts ...sdktrace.BatchSpanProcessorOption) (func(context.Context) error, error) {
	if exporter == nil {
		return nil, fmt.Errorf("tracing: nil span exporter")
	}
	processor := sdktrace.NewBatchSpanProcessor(exporter, opts...)
	if err := RegisterProcessor(w, op, processor, resources); err != nil {
		_ = processor.Shutdown(context.Background())
		return nil, err
	}
	return processor.Shutdown, nil
}

// RegisterProcessor lets an application share an existing host processor.
// It remains responsible for flushing and shutting down that processor.
func RegisterProcessor(w any, op uint32, processor sdktrace.SpanProcessor, resources *resource.Resource) error {
	if processor == nil {
		return fmt.Errorf("tracing: nil span processor")
	}
	return worker.RegisterSink(w, op, func(event worker.SinkEvent) {
		span, err := Decode(event.Payload, resources)
		if err != nil {
			panic(fmt.Sprintf("tracing: invalid span: %v", err))
		}
		processor.OnEnd(span)
	}, worker.SinkOptions{Name: "opentelemetry"})
}

// Decode reconstructs immutable host span data with the original trace/span IDs.
func Decode(raw []byte, resources *resource.Resource) (sdktrace.ReadOnlySpan, error) {
	var record otelwire.Span
	if err := json.Unmarshal(raw, &record); err != nil {
		return nil, err
	}
	if record.Version != 1 {
		return nil, fmt.Errorf("unsupported span format %d", record.Version)
	}
	sc, err := record.Context.Decode()
	if err != nil {
		return nil, err
	}
	if !sc.IsValid() {
		return nil, fmt.Errorf("invalid span context")
	}
	parent, err := record.Parent.Decode()
	if err != nil {
		return nil, err
	}
	attrs, err := otelwire.DecodeAttributes(record.Attributes)
	if err != nil {
		return nil, err
	}
	scopeAttrs, err := otelwire.DecodeAttributes(record.ScopeAttributes)
	if err != nil {
		return nil, err
	}
	out := &spanSnapshot{record: record, sc: sc, parent: parent, attrs: attrs, resources: resources, scope: instrumentation.Scope{Name: record.ScopeName, Version: record.ScopeVersion, SchemaURL: record.SchemaURL, Attributes: attribute.NewSet(scopeAttrs...)}}
	for _, event := range record.Events {
		attrs, err := otelwire.DecodeAttributes(event.Attributes)
		if err != nil {
			return nil, err
		}
		out.events = append(out.events, sdktrace.Event{Name: event.Name, Time: time.Unix(0, event.Time), Attributes: attrs})
	}
	for _, link := range record.Links {
		sc, err := link.Context.Decode()
		if err != nil {
			return nil, err
		}
		attrs, err := otelwire.DecodeAttributes(link.Attributes)
		if err != nil {
			return nil, err
		}
		out.links = append(out.links, sdktrace.Link{SpanContext: sc, Attributes: attrs})
	}
	return out, nil
}

type spanSnapshot struct {
	sdktrace.ReadOnlySpan // carries the SDK's sealed method; all public methods below
	record                otelwire.Span
	sc, parent            trace.SpanContext
	attrs                 []attribute.KeyValue
	events                []sdktrace.Event
	links                 []sdktrace.Link
	scope                 instrumentation.Scope
	resources             *resource.Resource
}

func (s *spanSnapshot) Name() string                     { return s.record.Name }
func (s *spanSnapshot) SpanContext() trace.SpanContext   { return s.sc }
func (s *spanSnapshot) Parent() trace.SpanContext        { return s.parent }
func (s *spanSnapshot) SpanKind() trace.SpanKind         { return s.record.Kind }
func (s *spanSnapshot) StartTime() time.Time             { return time.Unix(0, s.record.Start) }
func (s *spanSnapshot) EndTime() time.Time               { return time.Unix(0, s.record.End) }
func (s *spanSnapshot) Attributes() []attribute.KeyValue { return s.attrs }
func (s *spanSnapshot) Events() []sdktrace.Event         { return s.events }
func (s *spanSnapshot) Links() []sdktrace.Link           { return s.links }
func (s *spanSnapshot) Status() sdktrace.Status {
	return sdktrace.Status{Code: codes.Code(s.record.Status), Description: s.record.StatusDescription}
}
func (s *spanSnapshot) InstrumentationScope() instrumentation.Scope     { return s.scope }
func (s *spanSnapshot) InstrumentationLibrary() instrumentation.Library { return s.scope }
func (s *spanSnapshot) Resource() *resource.Resource                    { return s.resources }
func (*spanSnapshot) DroppedAttributes() int                            { return 0 }
func (*spanSnapshot) DroppedEvents() int                                { return 0 }
func (*spanSnapshot) DroppedLinks() int                                 { return 0 }
func (*spanSnapshot) ChildSpanCount() int                               { return 0 }
