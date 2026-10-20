package workflow

import (
	"context"
	"testing"

	"github.com/mfateev/sdk-go-poc/internal/payloadwire"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/sdk/converter"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// Custom implementations use their own reflection within the current isolate.
type customTestProto struct{ commonpb.WorkflowType }

func (m *customTestProto) ProtoReflect() protoreflect.Message { return m.WorkflowType.ProtoReflect() }

func TestTypedHandlerUsesDefaultTemporalConverter(t *testing.T) {
	type input struct{ Name string }
	type output struct{ Message string }
	RegisterTyped2("test-typed-handler", func(_ context.Context, in input, suffix string) (output, error) {
		return output{Message: in.Name + suffix}, nil
	})
	t.Cleanup(func() { delete(typedHandlers, "test-typed-handler") })
	handler := typedHandlers["test-typed-handler"]
	in, err := converter.GetDefaultDataConverter().ToPayloads(input{Name: "hello"}, "!")
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := proto.Marshal(in)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := payloadwire.Decode(encoded)
	if err != nil {
		t.Fatal(err)
	}
	result, err := handler(context.Background(), decoded)
	if err != nil {
		t.Fatal(err)
	}
	var got output
	if err := converter.GetDefaultDataConverter().FromPayloads(result, &got); err != nil {
		t.Fatal(err)
	}
	if got.Message != "hello!" {
		t.Fatalf("result = %+v", got)
	}
	if _, err := handler(context.Background(), new(commonpb.Payloads)); err == nil {
		t.Fatal("missing arguments should fail")
	}
}

func TestTypedHandlerAcceptsCustomProtoResult(t *testing.T) {
	value := customTestProto{WorkflowType: commonpb.WorkflowType{Name: "custom"}}
	for _, v := range []any{&value, value} {
		result, err := encodeTypedResult(v, nil)
		if err != nil {
			t.Fatal(err)
		}
		var got commonpb.WorkflowType
		if err := converter.GetDefaultDataConverter().FromPayloads(result, &got); err != nil || got.Name != "custom" {
			t.Fatalf("custom result: %v %v", &got, err)
		}
	}
}
func TestTypedHandlerAcceptsCustomProtoArgument(t *testing.T) {
	RegisterTyped("test-proto-argument", func(_ context.Context, in *customTestProto) (string, error) { return in.Name, nil })
	t.Cleanup(func() { delete(typedHandlers, "test-proto-argument") })
	input, err := converter.GetDefaultDataConverter().ToPayloads(&commonpb.WorkflowType{Name: "custom"})
	if err != nil {
		t.Fatal(err)
	}
	result, err := typedHandlers["test-proto-argument"](context.Background(), input)
	if err != nil {
		t.Fatal(err)
	}
	var got string
	if err := converter.GetDefaultDataConverter().FromPayloads(result, &got); err != nil || got != "custom" {
		t.Fatalf("custom argument: %q %v", got, err)
	}
}

func TestTypedActivityArgumentsAndResults(t *testing.T) {
	type record struct {
		Name  string
		Count int
	}
	data, err := encodeActivityArgs([]any{"hello", 3, record{Name: "world", Count: 7}})
	if err != nil {
		t.Fatal(err)
	}
	payloads := data
	var name string
	var count int
	var in record
	if err := converter.GetDefaultDataConverter().FromPayloads(payloads, &name, &count, &in); err != nil {
		t.Fatal(err)
	}
	if name != "hello" || count != 3 || in != (record{Name: "world", Count: 7}) {
		t.Fatalf("arguments = %q, %d, %+v", name, count, in)
	}
	encoded, err := converter.GetDefaultDataConverter().ToPayloads(in)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := proto.Marshal(encoded)
	if err != nil {
		t.Fatal(err)
	}
	got, err := decodeActivityResult[record](raw)
	if err != nil || got != in {
		t.Fatalf("result = %+v, %v", got, err)
	}
	empty, err := encodeActivityArgs(nil)
	if err != nil || len(empty.GetPayloads()) != 0 {
		t.Fatalf("zero arguments = %x, %v", empty, err)
	}
	if _, err := decodeActivityResult[struct{}](nil); err != nil {
		t.Fatal(err)
	}
}

func TestTypedActivityFailuresReturnZero(t *testing.T) {
	if err := ExecuteActivity(context.Background(), "").Get(context.Background(), nil); err == nil {
		t.Fatal("accepted empty name")
	}
	payloads, err := converter.GetDefaultDataConverter().ToPayloads("not an integer")
	if err != nil {
		t.Fatal(err)
	}
	raw, err := proto.Marshal(payloads)
	if err != nil {
		t.Fatal(err)
	}
	if got, err := decodeActivityResult[int](raw); err == nil || got != 0 {
		t.Fatalf("mismatched result = %d, %v", got, err)
	}
	if _, err := decodeActivityResult[string]([]byte{0xff}); err == nil {
		t.Fatal("malformed protobuf accepted")
	}
	payloads, err = converter.GetDefaultDataConverter().ToPayloads("one", "two")
	if err != nil {
		t.Fatal(err)
	}
	raw, err = proto.Marshal(payloads)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := decodeActivityResult[string](raw); err == nil {
		t.Fatal("multiple result payloads accepted")
	}
	for _, value := range []any{&customTestProto{}, customTestProto{}} {
		if _, err := encodeActivityArgs([]any{value}); err != nil {
			t.Fatal(err)
		}
	}
	f := &activityFuture{done: make(chan struct{}), payload: &commonpb.Payloads{Payloads: []*commonpb.Payload{nil}}}
	close(f.done)
	if err := f.Get(context.Background(), new(*customTestProto)); err == nil {
		t.Fatalf("protobuf activity result: %v", err)
	}
	if _, err := encodeActivityArgs([]any{make(chan int)}); err == nil {
		t.Fatal("unsupported JSON argument accepted")
	}
}
