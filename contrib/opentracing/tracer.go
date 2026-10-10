package opentracing

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	nativeotel "github.com/mfateev/sdk-go-poc/contrib/opentelemetry"
	"github.com/mfateev/sdk-go-poc/internal/otelcontext"
	"github.com/mfateev/sdk-go-poc/internal/tracecontext"
	ot "github.com/opentracing/opentracing-go"
	otlog "github.com/opentracing/opentracing-go/log"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/baggage"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

// NewSinkTracer creates an isolate-owned OpenTracing backend. IDs and time use
// the deterministic isolate runtime. Finished spans use the same copied-byte
// format and host exporter as contrib/opentelemetry; no exporter runs here.
// TextMap/HTTPHeaders use W3C trace context and baggage, not Jaeger/B3 headers.
func NewSinkTracer(op uint32) ot.Tracer {
	return NewBridgeTracer(nativeotel.NewSinkTracerProvider(op), nil)
}

// NewBridgeTracer adapts an explicit OTel provider to OpenTracing. Use a normal
// SDK provider on the host for upstream Temporal client/activity interception.
// Inside isolates, the provider and optional propagator must be isolate-owned
// and effect-safe. Nil propagation selects private W3C context and baggage.
// No process-global tracer/provider is read or installed.
func NewBridgeTracer(provider trace.TracerProvider, propagator propagation.TextMapPropagator) ot.Tracer {
	if propagator == nil {
		propagator = nativeotel.NewTextMapPropagator()
	}
	return &bridgeTracer{tracer: provider.Tracer("temporal-sdk-go/opentracing"), propagator: propagator}
}

type bridgeTracer struct {
	tracer     trace.Tracer
	propagator propagation.TextMapPropagator
}

// Contexts are immutable snapshots: baggage updates affect only future children.
type spanContext struct {
	sc    trace.SpanContext
	bag   baggage.Baggage
	scope string // local observation token, never injected into headers
}

func (c spanContext) ForeachBaggageItem(fn func(string, string) bool) {
	for _, member := range c.bag.Members() {
		if !fn(member.Key(), member.Value()) {
			return
		}
	}
}

func (t *bridgeTracer) StartSpan(name string, opts ...ot.StartSpanOption) ot.Span {
	return t.startSpan(context.Background(), name, opts...)
}

func (t *bridgeTracer) startSpan(ctx context.Context, name string, opts ...ot.StartSpanOption) ot.Span {
	config := ot.StartSpanOptions{}
	for _, opt := range opts {
		opt.Apply(&config)
	}
	// Prefer the first ChildOf reference, otherwise the first FollowsFrom.
	parentIndex := -1
	for i, ref := range config.References {
		if _, ok := ref.ReferencedContext.(spanContext); !ok {
			continue
		}
		if parentIndex == -1 || ref.Type == ot.ChildOfRef {
			parentIndex = i
			if ref.Type == ot.ChildOfRef {
				break
			}
		}
	}
	parent := spanContext{}
	if parentIndex >= 0 {
		parent = config.References[parentIndex].ReferencedContext.(spanContext)
	}
	scope, _ := ctx.Value(tracecontext.ScopeKey{}).(string)
	if scope == "" && parent.scope != "" {
		scope = parent.scope
		ctx = context.WithValue(ctx, tracecontext.ScopeKey{}, scope)
	}
	// References, rather than an incidental OTel span in ctx, determine the parent.
	ctx = trace.ContextWithSpanContext(ctx, parent.sc)
	startOpts := []trace.SpanStartOption{trace.WithTimestamp(config.StartTime)}
	for i, ref := range config.References {
		if c, ok := ref.ReferencedContext.(spanContext); ok && (i != parentIndex || ref.Type == ot.FollowsFromRef) {
			kind := "child_of"
			if ref.Type == ot.FollowsFromRef {
				kind = "follows_from"
			}
			startOpts = append(startOpts, trace.WithLinks(trace.Link{SpanContext: c.sc, Attributes: []attribute.KeyValue{attribute.String("opentracing.reference.type", kind)}}))
		}
	}
	if kind, ok := tagAttribute("span.kind", config.Tags["span.kind"]); ok && kind.Value.Type() == attribute.STRING {
		switch kind.Value.AsString() {
		case "client":
			startOpts = append(startOpts, trace.WithSpanKind(trace.SpanKindClient))
		case "server":
			startOpts = append(startOpts, trace.WithSpanKind(trace.SpanKindServer))
		case "producer":
			startOpts = append(startOpts, trace.WithSpanKind(trace.SpanKindProducer))
		case "consumer":
			startOpts = append(startOpts, trace.WithSpanKind(trace.SpanKindConsumer))
		}
	}
	_, native := t.tracer.Start(ctx, name, startOpts...)
	s := &bridgeSpan{tracer: t, span: native}
	s.context.Store(&spanContext{sc: native.SpanContext(), bag: parent.bag, scope: scope})
	for k, v := range config.Tags {
		s.SetTag(k, v)
	}
	return s
}

// ContextWithSpanHook also exposes the underlying OTel span, allowing mixed API
// instrumentation and preserving the native context's cancellation and values.
func (t *bridgeTracer) ContextWithSpanHook(ctx context.Context, span ot.Span) context.Context {
	if s, ok := span.(*bridgeSpan); ok {
		ctx = trace.ContextWithSpan(ctx, s.span)
		ctx = baggage.ContextWithBaggage(ctx, s.Context().(spanContext).bag)
	}
	return ctx
}

func (t *bridgeTracer) Inject(raw ot.SpanContext, format any, carrier any) error {
	c, ok := raw.(spanContext)
	if !ok || !c.sc.IsValid() {
		return ot.ErrInvalidSpanContext
	}
	f, ok := format.(ot.BuiltinFormat)
	if !ok {
		return ot.ErrUnsupportedFormat
	}
	ctx := trace.ContextWithSpanContext(context.Background(), c.sc)
	ctx = baggage.ContextWithBaggage(ctx, c.bag)
	data := propagation.MapCarrier{}
	t.propagator.Inject(ctx, data)
	switch f {
	case ot.TextMap, ot.HTTPHeaders:
		writer, ok := carrier.(ot.TextMapWriter)
		if !ok {
			return ot.ErrInvalidCarrier
		}
		for k, v := range data {
			writer.Set(k, v)
		}
		return nil
	case ot.Binary:
		writer, ok := carrier.(io.Writer)
		if !ok {
			return ot.ErrInvalidCarrier
		}
		raw, err := json.Marshal(data)
		if err != nil {
			return err
		}
		if len(raw) > maxBinaryCarrier {
			return ot.ErrInvalidCarrier
		}
		framed := make([]byte, 4+len(raw))
		binary.BigEndian.PutUint32(framed, uint32(len(raw)))
		copy(framed[4:], raw)
		n, err := writer.Write(framed)
		if err == nil && n != len(framed) {
			err = io.ErrShortWrite
		}
		return err
	default:
		return ot.ErrUnsupportedFormat
	}
}

const maxBinaryCarrier = 64 << 10

func (t *bridgeTracer) Extract(format any, carrier any) (ot.SpanContext, error) {
	f, ok := format.(ot.BuiltinFormat)
	if !ok {
		return nil, ot.ErrUnsupportedFormat
	}
	data := propagation.MapCarrier{}
	switch f {
	case ot.TextMap, ot.HTTPHeaders:
		reader, ok := carrier.(ot.TextMapReader)
		if !ok {
			return nil, ot.ErrInvalidCarrier
		}
		if err := reader.ForeachKey(func(k, v string) error {
			if f == ot.HTTPHeaders {
				k = strings.ToLower(k)
			}
			data[k] = v
			return nil
		}); err != nil {
			return nil, err
		}
	case ot.Binary:
		reader, ok := carrier.(io.Reader)
		if !ok {
			return nil, ot.ErrInvalidCarrier
		}
		var header [4]byte
		if _, err := io.ReadFull(reader, header[:]); err == io.EOF {
			return nil, ot.ErrSpanContextNotFound
		} else if err != nil {
			return nil, ot.ErrSpanContextCorrupted
		}
		size := binary.BigEndian.Uint32(header[:])
		if size > maxBinaryCarrier {
			return nil, ot.ErrSpanContextCorrupted
		}
		raw := make([]byte, int(size))
		if _, err := io.ReadFull(reader, raw); err != nil {
			return nil, ot.ErrSpanContextCorrupted
		}
		if err := json.Unmarshal(raw, &data); err != nil {
			return nil, ot.ErrSpanContextCorrupted
		}
	default:
		return nil, ot.ErrUnsupportedFormat
	}
	ctx := t.propagator.Extract(context.Background(), data)
	sc := trace.SpanContextFromContext(otelcontext.WithFallback(ctx))
	if !sc.IsValid() {
		// Unrelated carrier entries do not constitute a malformed span. For
		// custom propagators, Fields() declares which entries identify tracing.
		for _, key := range t.propagator.Fields() {
			if _, present := data[key]; key != "baggage" && present {
				return nil, ot.ErrSpanContextCorrupted
			}
		}
		return nil, ot.ErrSpanContextNotFound
	}
	return spanContext{sc: sc, bag: baggage.FromContext(ctx)}, nil
}

type bridgeSpan struct {
	tracer  *bridgeTracer
	span    trace.Span
	mu      sync.Mutex
	context atomic.Pointer[spanContext]
}

func (s *bridgeSpan) Context() ot.SpanContext {
	return *s.context.Load()
}
func (s *bridgeSpan) Tracer() ot.Tracer { return s.tracer }
func (s *bridgeSpan) Finish()           { s.span.End() }
func (s *bridgeSpan) FinishWithOptions(opts ot.FinishOptions) {
	for _, record := range opts.LogRecords {
		s.log(record.Timestamp, record.Fields...)
	}
	for _, data := range opts.BulkLogData {
		record := data.ToLogRecord()
		s.log(record.Timestamp, record.Fields...)
	}
	end := opts.FinishTime
	if end.IsZero() {
		end = time.Now()
	}
	s.span.End(trace.WithTimestamp(end))
}
func (s *bridgeSpan) SetOperationName(name string) ot.Span { s.span.SetName(name); return s }
func (s *bridgeSpan) SetTag(key string, value any) ot.Span {
	if attr, ok := tagAttribute(key, value); ok {
		s.span.SetAttributes(attr)
		if key == "error" && ((attr.Value.Type() == attribute.BOOL && attr.Value.AsBool()) || (attr.Value.Type() == attribute.STRING && attr.Value.AsString() == "true")) {
			s.span.SetStatus(codes.Error, "")
		}
	}
	return s
}
func (s *bridgeSpan) SetBaggageItem(key, value string) ot.Span {
	member, err := baggage.NewMember(key, value)
	if err != nil {
		return s
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	old := s.context.Load()
	if next, err := old.bag.SetMember(member); err == nil {
		s.context.Store(&spanContext{sc: old.sc, bag: next, scope: old.scope})
	}
	return s
}
func (s *bridgeSpan) BaggageItem(key string) string {
	return s.context.Load().bag.Member(key).Value()
}
func (s *bridgeSpan) LogFields(fields ...otlog.Field) { s.log(time.Now(), fields...) }
func (s *bridgeSpan) LogKV(kvs ...any) {
	fields, err := otlog.InterleavedKVToFields(kvs...)
	if err != nil {
		s.LogFields(otlog.Error(err))
		return
	}
	s.LogFields(fields...)
}
func (s *bridgeSpan) LogEvent(event string) { s.LogFields(otlog.Event(event)) }
func (s *bridgeSpan) LogEventWithPayload(event string, payload any) {
	s.LogFields(otlog.Event(event), otlog.Object("payload", payload))
}
func (s *bridgeSpan) Log(data ot.LogData) {
	record := data.ToLogRecord()
	s.log(record.Timestamp, record.Fields...)
}
func (s *bridgeSpan) log(at time.Time, fields ...otlog.Field) {
	encoder := &logEncoder{}
	for _, field := range fields {
		field.Marshal(encoder)
	}
	name := "log"
	for _, attr := range encoder.attrs {
		if attr.Key == "event" && attr.Value.Type() == attribute.STRING {
			name = attr.Value.AsString()
		}
	}
	s.span.AddEvent(name, trace.WithTimestamp(at), trace.WithAttributes(encoder.attrs...))
}

// Unsupported tags are ignored as required by OpenTracing. Log objects are
// formatted immediately, so observations retain no application object pointers.
// OTel integers are signed; larger uint64 values use exact decimal strings.
func tagAttribute(key string, value any) (attribute.KeyValue, bool) {
	switch v := value.(type) {
	case string:
		return attribute.String(key, v), true
	case bool:
		return attribute.Bool(key, v), true
	case int:
		return attribute.Int64(key, int64(v)), true
	case int8:
		return attribute.Int64(key, int64(v)), true
	case int16:
		return attribute.Int64(key, int64(v)), true
	case int32:
		return attribute.Int64(key, int64(v)), true
	case int64:
		return attribute.Int64(key, v), true
	case uint:
		return unsignedAttribute(key, uint64(v)), true
	case uint8:
		return unsignedAttribute(key, uint64(v)), true
	case uint16:
		return unsignedAttribute(key, uint64(v)), true
	case uint32:
		return unsignedAttribute(key, uint64(v)), true
	case uint64:
		return unsignedAttribute(key, v), true
	case float32:
		return attribute.Float64(key, float64(v)), true
	case float64:
		return attribute.Float64(key, v), true
	default:
		// The ext package uses defined scalar types (notably SpanKindEnum).
		// Preserve those and application-defined scalar tags too.
		rv := reflect.ValueOf(value)
		switch rv.Kind() {
		case reflect.String:
			return attribute.String(key, rv.String()), true
		case reflect.Bool:
			return attribute.Bool(key, rv.Bool()), true
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			return attribute.Int64(key, rv.Int()), true
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			return unsignedAttribute(key, rv.Uint()), true
		case reflect.Float32, reflect.Float64:
			return attribute.Float64(key, rv.Float()), true
		default:
			return attribute.KeyValue{}, false
		}
	}
}
func unsignedAttribute(key string, v uint64) attribute.KeyValue {
	if v > math.MaxInt64 {
		return attribute.String(key, fmt.Sprint(v))
	}
	return attribute.Int64(key, int64(v))
}

type logEncoder struct{ attrs []attribute.KeyValue }

func (e *logEncoder) add(key string, v any) {
	if attr, ok := tagAttribute(key, v); ok {
		e.attrs = append(e.attrs, attr)
	}
}
func (e *logEncoder) EmitString(k, v string)             { e.add(k, v) }
func (e *logEncoder) EmitBool(k string, v bool)          { e.add(k, v) }
func (e *logEncoder) EmitInt(k string, v int)            { e.add(k, v) }
func (e *logEncoder) EmitInt32(k string, v int32)        { e.add(k, v) }
func (e *logEncoder) EmitInt64(k string, v int64)        { e.add(k, v) }
func (e *logEncoder) EmitUint32(k string, v uint32)      { e.add(k, v) }
func (e *logEncoder) EmitUint64(k string, v uint64)      { e.add(k, v) }
func (e *logEncoder) EmitFloat32(k string, v float32)    { e.add(k, v) }
func (e *logEncoder) EmitFloat64(k string, v float64)    { e.add(k, v) }
func (e *logEncoder) EmitObject(k string, v any)         { e.add(k, fmt.Sprint(v)) }
func (e *logEncoder) EmitLazyLogger(fn otlog.LazyLogger) { fn(e) }

var _ ot.Tracer = (*bridgeTracer)(nil)
var _ ot.TracerContextWithSpanExtension = (*bridgeTracer)(nil)
var _ ot.Span = (*bridgeSpan)(nil)
var _ otlog.Encoder = (*logEncoder)(nil)
