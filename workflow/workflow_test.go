package workflow

import (
	"strings"
	"testing"
	"time"

	"github.com/mfateev/sdk-go-poc/internal/payloadwire"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/sdk/converter"
	"google.golang.org/protobuf/proto"
)

func TestExecuteActivityAsyncReturnsOneErrorAndCloses(t *testing.T) {
	results := ExecuteActivityAsync("", nil, time.Second)
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
	RegisterTyped2("test-typed-handler", func(in input, suffix string) (output, error) {
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
	result, err := handler(decoded)
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
	if _, err := handler(new(commonpb.Payloads)); err == nil {
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
	RegisterTyped("test-proto-argument", func(*commonpb.Payload) (string, error) {
		t.Fatal("protobuf handler must not run")
		return "", nil
	})
	t.Cleanup(func() { delete(typedHandlers, "test-proto-argument") })
	payloads := &commonpb.Payloads{Payloads: []*commonpb.Payload{{
		Metadata: map[string][]byte{converter.MetadataEncoding: []byte(converter.MetadataEncodingJSON)},
		Data:     []byte(`{}`),
	}}}
	_, err := typedHandlers["test-proto-argument"](payloads)
	if err == nil || !strings.Contains(err.Error(), "protobuf values") {
		t.Fatalf("protobuf argument error = %v", err)
	}
}
