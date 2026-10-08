package workflow

import (
	"context"
	"strings"
	"testing"

	"github.com/mfateev/sdk-go-poc/internal/payloadwire"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/sdk/converter"
	"google.golang.org/protobuf/proto"
)

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

func TestTypedHandlerRejectsProtoResult(t *testing.T) {
	if _, err := encodeTypedResult(&commonpb.Payload{}, nil); err == nil {
		t.Fatal("protobuf result should be rejected")
	}
	if _, err := encodeTypedResult(commonpb.Payload{}, nil); err == nil {
		t.Fatal("protobuf value result should be rejected")
	}
}

func TestTypedHandlerRejectsProtoArgumentBeforeConversion(t *testing.T) {
	RegisterTyped("test-proto-argument", func(context.Context, *commonpb.Payload) (string, error) {
		t.Fatal("protobuf handler must not run")
		return "", nil
	})
	t.Cleanup(func() { delete(typedHandlers, "test-proto-argument") })
	payloads := &commonpb.Payloads{Payloads: []*commonpb.Payload{{
		Metadata: map[string][]byte{converter.MetadataEncoding: []byte(converter.MetadataEncodingJSON)},
		Data:     []byte(`{}`),
	}}}
	_, err := typedHandlers["test-proto-argument"](context.Background(), payloads)
	if err == nil || !strings.Contains(err.Error(), "protobuf values") {
		t.Fatalf("protobuf argument error = %v", err)
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
	var payloads commonpb.Payloads
	if err := proto.Unmarshal(data, &payloads); err != nil {
		t.Fatal(err)
	}
	var name string
	var count int
	var in record
	if err := converter.GetDefaultDataConverter().FromPayloads(&payloads, &name, &count, &in); err != nil {
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
	if err != nil || len(empty) != 0 {
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
	for _, value := range []any{&commonpb.Payload{}, commonpb.Payload{}} {
		if _, err := encodeActivityArgs([]any{value}); err == nil {
			t.Fatal("protobuf activity argument accepted")
		}
	}
	f := &activityFuture{done: make(chan struct{}), payload: []byte{1}}
	close(f.done)
	if err := f.Get(context.Background(), new(*commonpb.Payload)); err == nil || !strings.Contains(err.Error(), "protobuf values") {
		t.Fatalf("protobuf activity result: %v", err)
	}
	if _, err := encodeActivityArgs([]any{make(chan int)}); err == nil {
		t.Fatal("unsupported JSON argument accepted")
	}
}
