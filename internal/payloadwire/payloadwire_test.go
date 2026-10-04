package payloadwire

import (
	"testing"

	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/sdk/converter"
	"google.golang.org/protobuf/proto"
)

func TestInteroperatesWithProtobufPayloads(t *testing.T) {
	original, err := converter.GetDefaultDataConverter().ToPayloads(
		struct{ Name string }{Name: "hello"}, []byte{0, 1, 255},
	)
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := proto.Marshal(original)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := Decode(encoded)
	if err != nil {
		t.Fatal(err)
	}
	if !proto.Equal(decoded, original) {
		t.Fatalf("decoded payloads differ: got %v, want %v", decoded, original)
	}
	encoded, err = Encode(decoded)
	if err != nil {
		t.Fatal(err)
	}
	var again commonpb.Payloads
	if err := proto.Unmarshal(encoded, &again); err != nil {
		t.Fatal(err)
	}
	if !proto.Equal(&again, original) {
		t.Fatalf("encoded payloads differ: got %v, want %v", &again, original)
	}
}

func TestRejectsExternalPayloads(t *testing.T) {
	payloads := &commonpb.Payloads{Payloads: []*commonpb.Payload{{
		ExternalPayloads: []*commonpb.Payload_ExternalPayloadDetails{{}},
	}}}
	encoded, err := proto.Marshal(payloads)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Decode(encoded); err == nil {
		t.Fatal("external payload should be rejected")
	}
	if _, err := Encode(payloads); err == nil {
		t.Fatal("external payload should be rejected")
	}
}
