package opentelemetry

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"github.com/mfateev/sdk-go-poc/internal/otelcontext"
	"github.com/mfateev/sdk-go-poc/internal/otelwire"
	"github.com/mfateev/sdk-go-poc/internal/tracecontext"
	"github.com/mfateev/sdk-go-poc/workflow"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/embedded"
	"reflect"
	"sync"
	"time"
)

// NewSinkTracerProvider creates a private OTel provider. IDs use the isolate's
// deterministic crypto/rand stream, time uses its clock, and End emits copied
// bytes. Configure the exporter separately on the host. There is no global
// provider, background exporter, environment lookup, or remote sampling here.
// New roots are sampled; children inherit the parent's flags and trace state.
func NewSinkTracerProvider(op uint32) trace.TracerProvider {
	return &sinkProvider{sink: workflow.NewSink(op)}
}

type sinkProvider struct {
	embedded.TracerProvider
	sink workflow.Sink
}

func (p *sinkProvider) Tracer(name string, opts ...trace.TracerOption) trace.Tracer {
	config := trace.NewTracerConfig(opts...)
	return &sinkTracer{provider: p, name: name, config: config}
}

type sinkTracer struct {
	embedded.Tracer
	provider *sinkProvider
	name     string
	config   trace.TracerConfig
}

func (t *sinkTracer) Start(ctx context.Context, name string, opts ...trace.SpanStartOption) (context.Context, trace.Span) {
	config := trace.NewSpanStartConfig(opts...)
	parent := trace.SpanContextFromContext(otelcontext.WithFallback(ctx))
	if config.NewRoot() {
		parent = trace.SpanContext{}
	}
	traceID := parent.TraceID()
	flags := parent.TraceFlags()
	if !parent.IsValid() {
		for !traceID.IsValid() {
			_, _ = rand.Read(traceID[:])
		}
		flags = trace.FlagsSampled
	}
	var spanID trace.SpanID
	for !spanID.IsValid() {
		_, _ = rand.Read(spanID[:])
		if scope, _ := ctx.Value(tracecontext.ScopeKey{}).(string); scope != "" {
			digest := sha256.Sum256(append(spanID[:], scope...))
			copy(spanID[:], digest[:8])
		}
	}
	sc := trace.NewSpanContext(trace.SpanContextConfig{TraceID: traceID, SpanID: spanID, TraceFlags: flags, TraceState: parent.TraceState()})
	start := config.Timestamp()
	if start.IsZero() {
		start = time.Now()
	}
	scopeAttributes := t.config.InstrumentationAttributes()
	span := &sinkSpan{provider: t.provider, sc: sc, record: otelwire.Span{Version: 1, Name: name, Context: otelwire.FromContext(sc), Parent: otelwire.FromContext(parent), Kind: trace.ValidateSpanKind(config.SpanKind()), Start: start.UnixNano(), ScopeName: t.name, ScopeVersion: t.config.InstrumentationVersion(), SchemaURL: t.config.SchemaURL(), ScopeAttributes: otelwire.Attributes(scopeAttributes.ToSlice())}}
	span.SetAttributes(config.Attributes()...)
	for _, link := range config.Links() {
		span.AddLink(link)
	}
	return trace.ContextWithSpan(ctx, span), span
}

type sinkSpan struct {
	embedded.Span
	provider *sinkProvider
	sc       trace.SpanContext // immutable; safe to read from query scratch scopes
	mu       sync.Mutex
	ended    bool
	record   otelwire.Span
}

func (s *sinkSpan) SpanContext() trace.SpanContext       { return s.sc }
func (s *sinkSpan) TracerProvider() trace.TracerProvider { return s.provider }
func (s *sinkSpan) IsRecording() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return !s.ended && s.sc.IsSampled()
}
func (s *sinkSpan) End(opts ...trace.SpanEndOption) {
	config := trace.NewSpanEndConfig(opts...)
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ended {
		return
	}
	s.ended = true
	if !s.sc.IsSampled() {
		return
	}
	end := config.Timestamp()
	if end.IsZero() {
		end = time.Now()
	}
	s.record.End = end.UnixNano()
	raw, err := json.Marshal(s.record)
	if err == nil {
		s.provider.sink.Emit(raw)
	}
}
func (s *sinkSpan) SetName(name string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.ended {
		s.record.Name = name
	}
}
func (s *sinkSpan) SetStatus(code codes.Code, description string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ended || code < codes.Code(s.record.Status) {
		return
	}
	s.record.Status = int(code)
	s.record.StatusDescription = ""
	if code == codes.Error {
		s.record.StatusDescription = description
	}
}
func (s *sinkSpan) SetAttributes(kvs ...attribute.KeyValue) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ended {
		return
	}
	for _, item := range otelwire.Attributes(kvs) {
		found := false
		for i, old := range s.record.Attributes {
			if old.Key == item.Key {
				s.record.Attributes[i] = item
				found = true
				break
			}
		}
		if !found {
			s.record.Attributes = append(s.record.Attributes, item)
		}
	}
}
func (s *sinkSpan) AddLink(link trace.Link) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.ended {
		s.record.Links = append(s.record.Links, otelwire.Link{Context: otelwire.FromContext(link.SpanContext), Attributes: otelwire.Attributes(link.Attributes)})
	}
}
func (s *sinkSpan) AddEvent(name string, opts ...trace.EventOption) {
	config := trace.NewEventConfig(opts...)
	at := config.Timestamp()
	if at.IsZero() {
		at = time.Now()
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.ended {
		s.record.Events = append(s.record.Events, otelwire.Event{Name: name, Time: at.UnixNano(), Attributes: otelwire.Attributes(config.Attributes())})
	}
}
func (s *sinkSpan) RecordError(err error, opts ...trace.EventOption) {
	if err == nil {
		return
	}
	attrs := []attribute.KeyValue{attribute.String("exception.type", reflect.TypeOf(err).String()), attribute.String("exception.message", fmt.Sprint(err))}
	s.AddEvent("exception", append(opts, trace.WithAttributes(attrs...))...)
}
