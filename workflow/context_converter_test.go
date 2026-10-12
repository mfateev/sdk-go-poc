package workflow

import (
	"bytes"
	"context"
	"errors"
	"testing"

	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/sdk/converter"
)

type maskKey struct{}
type contextMaskConverter struct {
	converter.DataConverter
	mask string
}

func (dc *contextMaskConverter) ToPayload(value any) (*commonpb.Payload, error) {
	p, err := dc.DataConverter.ToPayload(value)
	if err == nil && dc.mask != "" {
		p.Data = bytes.ReplaceAll(p.Data, []byte(dc.mask), []byte("?"))
	}
	return p, err
}
func (dc *contextMaskConverter) ToPayloads(values ...any) (*commonpb.Payloads, error) {
	p := &commonpb.Payloads{}
	for _, value := range values {
		one, err := dc.ToPayload(value)
		if err != nil {
			return nil, err
		}
		p.Payloads = append(p.Payloads, one)
	}
	return p, nil
}
func (dc *contextMaskConverter) WithContext(ctx context.Context) converter.DataConverter {
	mask, _ := ctx.Value(maskKey{}).(string)
	return &contextMaskConverter{DataConverter: dc.DataConverter, mask: mask}
}
func (dc *contextMaskConverter) WithWorkflowContext(ctx context.Context) converter.DataConverter {
	return dc.WithContext(ctx)
}

func TestUpstreamNativeContextAwareDataConverter(t *testing.T) {
	base := &contextMaskConverter{DataConverter: newDefaultDataConverter()}
	var _ ContextAware = base
	ctx := context.WithValue(context.Background(), maskKey{}, "e")
	for _, tc := range []struct {
		name string
		dc   converter.DataConverter
		want string
	}{
		{"default", base, `"test"`},
		{"activity-context", WithContext(ctx, base), `"t?st"`},
		{"workflow-context", WithWorkflowContext(ctx, base), `"t?st"`},
		{"per-context", GetDataConverter(WithDataConverter(ctx, base)), `"t?st"`},
		{"parent-preserved", GetDataConverter(WithDataConverter(context.Background(), base)), `"test"`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p, err := tc.dc.ToPayload("test")
			if err != nil || tc.dc.ToString(p) != tc.want {
				t.Fatalf("payload %v error %v want %s", p, err, tc.want)
			}
		})
	}
	if base.mask != "" {
		t.Fatal("context binding mutated parent")
	}
}

type taggedResultConverter struct {
	converter.DataConverter
	tag string
}

func (dc taggedResultConverter) FromPayloads(p *commonpb.Payloads, pointers ...any) error {
	if err := dc.DataConverter.FromPayloads(p, pointers...); err != nil {
		return err
	}
	if len(pointers) == 1 {
		*pointers[0].(*string) += dc.tag
	}
	return nil
}
func TestPerContextFutureRetainsScheduledConverter(t *testing.T) {
	parent := context.Background()
	first := WithDataConverter(parent, taggedResultConverter{newDefaultDataConverter(), "-first"})
	second := WithDataConverter(parent, taggedResultConverter{newDefaultDataConverter(), "-second"})
	payload, err := encodeActivityArgs([]any{"value"})
	if err != nil {
		t.Fatal(err)
	}
	for _, ctx := range []context.Context{first, second} {
		f := &activityFuture{done: make(chan struct{}), payload: payload, dataConverter: GetDataConverter(ctx), converterContext: ctx}
		close(f.done)
		want := "value-first"
		if ctx == second {
			want = "value-second"
		}
		for _, waiting := range []context.Context{parent, first, second} {
			var value string
			if err := f.Get(waiting, &value); err != nil || value != want {
				t.Fatalf("result=%s error=%v want=%s", value, err, want)
			}
		}
		value := receiveFutureResult(t, f.ToChannel()).Value
		var result string
		if err := value.Get(&result); err != nil || result != want {
			t.Fatalf("channel result=%s %v", result, err)
		}
	}
}

func TestPerContextConvertersDoNotChangeSiblingOrParent(t *testing.T) {
	parent := context.Background()
	base := &contextMaskConverter{DataConverter: newDefaultDataConverter()}
	first := WithDataConverter(context.WithValue(parent, maskKey{}, "e"), base)
	second := WithDataConverter(context.WithValue(parent, maskKey{}, "t"), base)
	for _, tc := range []struct {
		ctx  context.Context
		want string
	}{{first, "t?st"}, {second, "?es?"}, {parent, "test"}} {
		p, err := encodeContextArgs(tc.ctx, []any{"test"})
		if err != nil {
			t.Fatal(err)
		}
		decoded, err := decodePayloads(p)
		if err != nil {
			t.Fatal(err)
		}
		var got string
		if err := newDefaultDataConverter().FromPayloads(decoded, &got); err != nil || got != tc.want {
			t.Fatalf("value=%s %v want=%s", got, err, tc.want)
		}
	}
}

type nativeContextOnlyConverter struct{ converter.DataConverter }

func (dc nativeContextOnlyConverter) WithContext(ctx context.Context) converter.DataConverter {
	return (&contextMaskConverter{DataConverter: dc.DataConverter}).WithContext(ctx)
}

func TestNativeContextOnlyConverter(t *testing.T) {
	ctx := context.WithValue(context.Background(), maskKey{}, "e")
	dc := GetDataConverter(WithDataConverter(ctx, nativeContextOnlyConverter{newDefaultDataConverter()}))
	p, err := dc.ToPayload("test")
	if err != nil || dc.ToString(p) != `"t?st"` {
		t.Fatalf("context-only conversion: %v %v", p, err)
	}
}

type contextJSONPayloadConverter struct{ converter.PayloadConverter }

func (contextJSONPayloadConverter) Encoding() string { return "json/test-context" }
func (dc contextJSONPayloadConverter) ToPayload(value any) (*commonpb.Payload, error) {
	p, err := dc.PayloadConverter.ToPayload(value)
	if err == nil {
		p.Metadata["encoding"] = []byte(dc.Encoding())
	}
	return p, err
}

func TestPerContextCustomEncodingAndContinuation(t *testing.T) {
	base := context.Background()
	dc := converter.NewCompositeDataConverter(contextJSONPayloadConverter{converter.NewJSONPayloadConverter()})
	ctx := WithDataConverter(context.WithValue(base, maskKey{}, "derived"), dc)
	previous := instancePropagators
	t.Cleanup(func() { instancePropagators = previous })
	instancePropagators = []ContextPropagator{testPropagator{inject: func(ctx context.Context, h HeaderWriter) error {
		p, err := newDefaultDataConverter().ToPayload(ctx.Value(maskKey{}))
		h.Set("key", p)
		return err
	}}}
	e := NewContinueAsNewError(ctx, "Next", "value").(*ContinueAsNewError)
	if string(e.Input.Payloads[0].Metadata["encoding"]) != "json/test-context" {
		t.Fatalf("continuation encoding: %v", e.Input)
	}
	var header string
	if err := newDefaultDataConverter().FromPayload(e.Header.Fields["key"], &header); err != nil || header != "derived" {
		t.Fatalf("continuation header: %s %v", header, err)
	}
	payload, err := encodeContextArgs(ctx, []any{"value"})
	if err != nil {
		t.Fatal(err)
	}
	f := &activityFuture{done: make(chan struct{}), payload: payload, converterContext: ctx, dataConverter: GetDataConverter(ctx)}
	close(f.done)
	var result string
	if err := f.Get(base, &result); err != nil || result != "value" {
		t.Fatalf("custom encoding without worker factory: %s %v", result, err)
	}
	if base.Value(dataConverterKey{}) != nil || base.Value(maskKey{}) != nil {
		t.Fatal("derived converter or header changed parent")
	}
}

type nilContextConverter struct{ converter.DataConverter }

func (nilContextConverter) WithWorkflowContext(context.Context) converter.DataConverter {
	return (*contextMaskConverter)(nil)
}
func TestPerContextConverterRejectsNil(t *testing.T) {
	for _, call := range []func(){
		func() { WithDataConverter(nil, newDefaultDataConverter()) },
		func() { WithDataConverter(context.Background(), nil) },
		func() { WithDataConverter(context.Background(), (*contextMaskConverter)(nil)) },
		func() { WithWorkflowContext(context.Background(), nilContextConverter{}) },
	} {
		func() {
			defer func() {
				if recover() == nil {
					t.Error("nil converter/context accepted")
				}
			}()
			call()
		}()
	}
}

type testPropagator struct {
	inject  func(context.Context, HeaderWriter) error
	extract func(context.Context, HeaderReader) (context.Context, error)
}

func (p testPropagator) Inject(ctx context.Context, h HeaderWriter) error {
	if p.inject != nil {
		return p.inject(ctx, h)
	}
	return nil
}
func (p testPropagator) Extract(ctx context.Context, h HeaderReader) (context.Context, error) {
	if p.extract != nil {
		return p.extract(ctx, h)
	}
	return ctx, nil
}
func TestContextPropagatorOrderIsolationAndErrors(t *testing.T) {
	previous := instancePropagators
	t.Cleanup(func() { instancePropagators = previous })
	p, err := newDefaultDataConverter().ToPayload("root")
	if err != nil {
		t.Fatal(err)
	}
	original := propagationHeader{"key": p}
	ctx := context.WithValue(context.Background(), interceptorHeaderKey{}, map[string]*commonpb.Payload(original))
	ctx = context.WithValue(ctx, maskKey{}, "derived")
	instancePropagators = []ContextPropagator{
		testPropagator{inject: func(ctx context.Context, h HeaderWriter) error {
			p, err := newDefaultDataConverter().ToPayload(ctx.Value(maskKey{}))
			h.Set("key", p)
			return err
		}},
		testPropagator{extract: func(ctx context.Context, h HeaderReader) (context.Context, error) {
			p, ok := h.Get("key")
			if !ok {
				return nil, errors.New("missing key")
			}
			var value string
			if err := newDefaultDataConverter().FromPayload(p, &value); err != nil {
				return nil, err
			}
			return context.WithValue(ctx, maskKey{}, value), nil
		}},
		testPropagator{extract: func(ctx context.Context, h HeaderReader) (context.Context, error) {
			if ctx.Value(maskKey{}) != "derived" {
				return nil, errors.New("wrong extraction order")
			}
			return ctx, nil
		}},
	}
	header, err := outgoingHeader(ctx)
	if err != nil {
		t.Fatal(err)
	}
	got, err := withIncomingHeader(context.Background(), header)
	if err != nil || got.Value(maskKey{}) != "derived" {
		t.Fatalf("context %v %v", got, err)
	}
	var value string
	if err := newDefaultDataConverter().FromPayload(original["key"], &value); err != nil || value != "root" {
		t.Fatalf("parent header mutated: %s %v", value, err)
	}
	cause := errors.New("propagator failure")
	instancePropagators = []ContextPropagator{testPropagator{inject: func(context.Context, HeaderWriter) error { return cause }, extract: func(context.Context, HeaderReader) (context.Context, error) { return nil, cause }}}
	if _, err := outgoingHeader(ctx); !errors.Is(err, cause) {
		t.Fatalf("inject error %v", err)
	}
	if _, err := withIncomingHeader(ctx, nil); !errors.Is(err, cause) {
		t.Fatalf("extract error %v", err)
	}
	instancePropagators = []ContextPropagator{testPropagator{extract: func(context.Context, HeaderReader) (context.Context, error) { return nil, nil }}}
	if _, err := withIncomingHeader(ctx, nil); err == nil {
		t.Fatal("nil extracted context accepted")
	}
}
