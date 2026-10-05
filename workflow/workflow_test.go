package workflow

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/mfateev/sdk-go-poc/internal/payloadwire"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/sdk/converter"
	"google.golang.org/protobuf/proto"
)

func TestExecuteActivityAsyncReturnsOneErrorAndCloses(t *testing.T) {
	results := ExecuteActivityAsyncByName[[]byte](context.Background(), "", time.Second)
	select {
	case result, ok := <-results:
		if !ok || result.Err == nil || result.Result != nil {
			t.Fatalf("unexpected activity result: %+v, open: %t", result, ok)
		}
	case <-time.After(time.Second):
		t.Fatal("activity result did not arrive")
	}
	if _, ok := <-results; ok {
		t.Fatal("activity result channel did not close")
	}
}

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
	if got, err := ExecuteActivityByName[string](context.Background(), "", time.Second); err == nil || got != "" {
		t.Fatalf("invalid call = %q, %v", got, err)
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
	if _, err := ExecuteActivityByName[*commonpb.Payload](context.Background(), "unsupported", time.Second); err == nil || !strings.Contains(err.Error(), "protobuf values") {
		t.Fatalf("protobuf activity result: %v", err)
	}
	if _, err := encodeActivityArgs([]any{make(chan int)}); err == nil {
		t.Fatal("unsupported JSON argument accepted")
	}
}

func TestInferredActivityRejectsNilAndNeverInvokesFunction(t *testing.T) {
	var fn func(context.Context, int) (string, error)
	var got string
	var err error
	// Assignment to string also checks inferred result type at compile time.
	got, err = ExecuteActivity(context.Background(), fn, time.Second, 5)
	if got != "" || err == nil {
		t.Fatalf("nil activity: %q, %v", got, err)
	}
	fn = func(context.Context, int) (string, error) { panic("activity executed in workflow") }
	got, err = ExecuteActivity(context.Background(), fn, 0, 5)
	if got != "" || err == nil {
		t.Fatalf("invalid timeout: %q, %v", got, err)
	}
	results := ExecuteActivityAsync(context.Background(), fn, 0, 5)
	result := <-results
	got = result.Result
	if result.Err == nil || got != "" {
		t.Fatalf("async invalid timeout: %+v", result)
	}
	if _, open := <-results; open {
		t.Fatal("async result channel stayed open")
	}
	var contextFn func(context.Context, int) (string, error)
	got, err = ExecuteActivity(context.Background(), contextFn, time.Second, 5)
	if got != "" || err == nil {
		t.Fatalf("nil context activity: %q, %v", got, err)
	}
	contextResult := <-ExecuteActivityAsync(context.Background(), contextFn, time.Second, 5)
	if contextResult.Err == nil {
		t.Fatal("async nil context activity accepted")
	}
}

func TestActivityReferenceSignatureVariants(t *testing.T) {
	var noInput func(context.Context) (string, error)
	var errorOnly func(context.Context, string) error
	var got string
	got, err := ExecuteActivityNoInput(context.Background(), noInput, time.Second)
	if got != "" || err == nil {
		t.Fatalf("nil no-input reference: %q, %v", got, err)
	}
	if err := ExecuteActivityError(context.Background(), errorOnly, time.Second, "input"); err == nil {
		t.Fatal("accepted nil error-only activity")
	}
	noInput = func(context.Context) (string, error) { panic("must run on host") }
	errorOnly = func(context.Context, string) error { panic("must run on host") }
	if _, err := ExecuteActivityNoInput(context.Background(), noInput, 0); err == nil {
		t.Fatal("accepted invalid timeout")
	}
	if err := ExecuteActivityError(context.Background(), errorOnly, 0, "input"); err == nil {
		t.Fatal("accepted invalid timeout")
	}
}
