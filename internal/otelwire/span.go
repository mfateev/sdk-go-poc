// Package otelwire copies tracing observations without a DataConverter.
package otelwire

import (
	"fmt"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"math"
)

type Context struct {
	TraceID, SpanID, TraceState string
	Flags                       byte
	Remote                      bool
}

func FromContext(c trace.SpanContext) Context {
	if !c.IsValid() {
		return Context{}
	}
	return Context{TraceID: c.TraceID().String(), SpanID: c.SpanID().String(), TraceState: c.TraceState().String(), Flags: byte(c.TraceFlags()), Remote: c.IsRemote()}
}
func (c Context) Decode() (trace.SpanContext, error) {
	if c.TraceID == "" && c.SpanID == "" {
		return trace.SpanContext{}, nil
	}
	tid, err := trace.TraceIDFromHex(c.TraceID)
	if err != nil {
		return trace.SpanContext{}, err
	}
	sid, err := trace.SpanIDFromHex(c.SpanID)
	if err != nil {
		return trace.SpanContext{}, err
	}
	state, err := trace.ParseTraceState(c.TraceState)
	if err != nil {
		return trace.SpanContext{}, err
	}
	return trace.NewSpanContext(trace.SpanContextConfig{TraceID: tid, SpanID: sid, TraceFlags: trace.TraceFlags(c.Flags), TraceState: state, Remote: c.Remote}), nil
}

// Value retains integer precision and floating-point bits, including NaN/Inf.
type Value struct {
	Type      attribute.Type
	Bool      bool
	Int       int64
	FloatBits uint64
	String    string
	Bools     []bool
	Ints      []int64
	Floats    []uint64
	Strings   []string
	Bytes     []byte
	Values    []Value
}

func FromValue(v attribute.Value) Value {
	out := Value{Type: v.Type()}
	switch v.Type() {
	case attribute.BOOL:
		out.Bool = v.AsBool()
	case attribute.INT64:
		out.Int = v.AsInt64()
	case attribute.FLOAT64:
		out.FloatBits = math.Float64bits(v.AsFloat64())
	case attribute.STRING:
		out.String = v.AsString()
	case attribute.BOOLSLICE:
		out.Bools = v.AsBoolSlice()
	case attribute.INT64SLICE:
		out.Ints = v.AsInt64Slice()
	case attribute.FLOAT64SLICE:
		for _, f := range v.AsFloat64Slice() {
			out.Floats = append(out.Floats, math.Float64bits(f))
		}
	case attribute.STRINGSLICE:
		out.Strings = v.AsStringSlice()
	case attribute.BYTESLICE:
		out.Bytes = v.AsByteSlice()
	case attribute.SLICE:
		for _, item := range v.AsSlice() {
			out.Values = append(out.Values, FromValue(item))
		}
	}
	return out
}
func (v Value) Decode() (attribute.Value, error) {
	switch v.Type {
	case attribute.EMPTY:
		return attribute.Value{}, nil
	case attribute.BOOL:
		return attribute.BoolValue(v.Bool), nil
	case attribute.INT64:
		return attribute.Int64Value(v.Int), nil
	case attribute.FLOAT64:
		return attribute.Float64Value(math.Float64frombits(v.FloatBits)), nil
	case attribute.STRING:
		return attribute.StringValue(v.String), nil
	case attribute.BOOLSLICE:
		return attribute.BoolSliceValue(v.Bools), nil
	case attribute.INT64SLICE:
		return attribute.Int64SliceValue(v.Ints), nil
	case attribute.FLOAT64SLICE:
		f := make([]float64, len(v.Floats))
		for i, b := range v.Floats {
			f[i] = math.Float64frombits(b)
		}
		return attribute.Float64SliceValue(f), nil
	case attribute.STRINGSLICE:
		return attribute.StringSliceValue(v.Strings), nil
	case attribute.BYTESLICE:
		return attribute.ByteSliceValue(v.Bytes), nil
	case attribute.SLICE:
		values := make([]attribute.Value, len(v.Values))
		for i, item := range v.Values {
			var err error
			values[i], err = item.Decode()
			if err != nil {
				return attribute.Value{}, err
			}
		}
		return attribute.SliceValue(values...), nil
	default:
		return attribute.Value{}, fmt.Errorf("invalid attribute type %d", v.Type)
	}
}

type Attribute struct {
	Key   string
	Value Value
}

func Attributes(kvs []attribute.KeyValue) []Attribute {
	out := make([]Attribute, len(kvs))
	for i, kv := range kvs {
		out[i] = Attribute{Key: string(kv.Key), Value: FromValue(kv.Value)}
	}
	return out
}
func DecodeAttributes(items []Attribute) ([]attribute.KeyValue, error) {
	out := make([]attribute.KeyValue, len(items))
	for i, item := range items {
		value, err := item.Value.Decode()
		if err != nil {
			return nil, err
		}
		out[i] = attribute.KeyValue{Key: attribute.Key(item.Key), Value: value}
	}
	return out, nil
}

type Event struct {
	Name       string
	Time       int64
	Attributes []Attribute
}
type Link struct {
	Context    Context
	Attributes []Attribute
}
type Span struct {
	Version                            int
	Name                               string
	Context, Parent                    Context
	Kind                               trace.SpanKind
	Start, End                         int64
	Attributes                         []Attribute
	Events                             []Event
	Links                              []Link
	Status                             int
	StatusDescription                  string
	ScopeName, ScopeVersion, SchemaURL string
	ScopeAttributes                    []Attribute
}
