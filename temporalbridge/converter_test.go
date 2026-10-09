package temporalbridge

import (
	"errors"
	"fmt"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/mfateev/sdk-go-poc/internal/failurecodec"
	"github.com/mfateev/sdk-go-poc/internal/failurewire"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/temporal"
	"google.golang.org/protobuf/proto"
)

func TestRemoteCodecRawTransportAndFailureDetails(t *testing.T) {
	var calls atomic.Int64
	codec := &countedCodec{calls: &calls, PayloadCodec: converter.NewZlibCodec(converter.ZlibCodecOptions{AlwaysEncode: true})}
	server := httptest.NewServer(converter.NewPayloadCodecHTTPHandler(codec))
	defer server.Close()
	dc := converter.NewCodecDataConverter(converter.GetDefaultDataConverter(), converter.NewRemotePayloadCodec(converter.RemotePayloadCodecOptions{Endpoint: server.URL}))
	input, err := converter.GetDefaultDataConverter().ToPayloads(int64(9007199254740993), "string")
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := encodeTransport(input, dc)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := decodeTransport(encoded, dc)
	if err != nil || !proto.Equal(input, decoded) {
		t.Fatalf("raw transport: %v", err)
	}
	if calls.Load() != 4 {
		t.Fatalf("codec calls=%d", calls.Load())
	}
	cause := temporal.NewNonRetryableApplicationError("secret message", "ExampleError", nil, int64(9007199254740993), "secret detail")
	fc := temporal.NewDefaultFailureConverter(temporal.DefaultFailureConverterOptions{DataConverter: dc, EncodeCommonAttributes: true})
	original := fc.ErrorToFailure(cause)
	raw, err := failurewire.Encode(original)
	if err != nil {
		t.Fatal(err)
	}
	if original.Message != "Encoded failure" || original.EncodedAttributes == nil {
		t.Fatal("common attributes were not encoded")
	}
	// A failure-holder has already encoded details; transport must decode exactly once.
	stored, err := failurewire.Decode(raw)
	if err != nil {
		t.Fatal(err)
	}
	holder := fc.FailureToError(stored)
	plain, err := encodeInboundFailure(holder, dc)
	if err != nil {
		t.Fatal(err)
	}
	inside, err := failurecodec.Decode(plain, converter.GetDefaultDataConverter())
	if err != nil {
		t.Fatal(err)
	}
	var application *temporal.ApplicationError
	if !errors.As(inside, &application) || application.Message() != "secret message" || !application.NonRetryable() {
		t.Fatalf("application error=%v", inside)
	}
	var number int64
	var message string
	if err := application.Details(&number, &message); err != nil || number != 9007199254740993 || message != "secret detail" {
		t.Fatalf("details=%d %s %v", number, message, err)
	}
	outbound, err := decodeOutboundFailure(plain, dc)
	if err != nil {
		t.Fatal(err)
	}
	roundTrip := fc.ErrorToFailure(outbound)
	if !proto.Equal(original, roundTrip) {
		t.Fatal("failure holders were encoded twice or attributes changed")
	}
}

type countedCodec struct {
	converter.PayloadCodec
	calls *atomic.Int64
}

func (c *countedCodec) Encode(p []*commonpb.Payload) ([]*commonpb.Payload, error) {
	c.calls.Add(1)
	return c.PayloadCodec.Encode(p)
}
func (c *countedCodec) Decode(p []*commonpb.Payload) ([]*commonpb.Payload, error) {
	c.calls.Add(1)
	return c.PayloadCodec.Decode(p)
}

type explodingCodec struct{}

func (*explodingCodec) Encode(p []*commonpb.Payload) ([]*commonpb.Payload, error) {
	return append(p, p...), nil
}
func (*explodingCodec) Decode(p []*commonpb.Payload) ([]*commonpb.Payload, error) {
	return append(p, p...), nil
}
func TestCardinalityChangingCodecFailsTask(t *testing.T) {
	dc := converter.NewCodecDataConverter(converter.GetDefaultDataConverter(), &explodingCodec{})
	p, err := converter.GetDefaultDataConverter().ToPayloads(1)
	if err != nil {
		t.Fatal(err)
	}
	for _, run := range []func(){func() { _, _ = encodeTransport(p, dc) }, func() { _, _ = decodeTransport(p, dc) }} {
		func() {
			defer func() {
				v := recover()
				if _, ok := v.(*WorkflowTaskError); !ok {
					t.Fatalf("wrong panic: %v", v)
				}
			}()
			run()
			t.Fatal("cardinality change accepted")
		}()
	}
}

func TestConfiguredSerializerDoesNotRunOnHostRawBoundary(t *testing.T) {
	dc := converter.NewCompositeDataConverter(&rejectValuesConverter{})
	p := &commonpb.Payloads{Payloads: []*commonpb.Payload{{Metadata: map[string][]byte{converter.MetadataEncoding: []byte("application/custom")}, Data: []byte("opaque")}}}
	encoded, err := encodeTransport(p, dc)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := decodeTransport(encoded, dc)
	if err != nil || !proto.Equal(p, decoded) {
		t.Fatalf("host decoded application values: %v", err)
	}
}

type rejectValuesConverter struct{}

func (*rejectValuesConverter) Encoding() string { return "application/custom" }
func (*rejectValuesConverter) ToPayload(any) (*commonpb.Payload, error) {
	return nil, fmt.Errorf("host value serialization forbidden")
}
func (*rejectValuesConverter) FromPayload(*commonpb.Payload, any) error {
	return fmt.Errorf("host value deserialization forbidden")
}
func (*rejectValuesConverter) ToString(*commonpb.Payload) string { return "opaque" }
