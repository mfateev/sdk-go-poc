package headerwire

import (
	"bytes"
	commonpb "go.temporal.io/api/common/v1"
	"testing"
)

func TestOpaqueHeaderCopy(t *testing.T) {
	input := &commonpb.Header{Fields: map[string]*commonpb.Payload{"trace": {Metadata: map[string][]byte{"encoding": []byte("custom/encrypted")}, Data: []byte{0, 255, 7}}}}
	encoded, err := Encode(input)
	if err != nil {
		t.Fatal(err)
	}
	input.Fields["trace"].Data[0] = 9
	decoded, err := Decode(encoded)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(decoded.Fields["trace"].Data, []byte{0, 255, 7}) || string(decoded.Fields["trace"].Metadata["encoding"]) != "custom/encrypted" {
		t.Fatalf("header changed: %v", decoded)
	}
	decoded.Fields["trace"].Data[1] = 1
	again, err := Decode(encoded)
	if err != nil {
		t.Fatal(err)
	}
	if again.Fields["trace"].Data[1] != 255 {
		t.Fatal("decoded payload aliases wire bytes")
	}
}
func TestRejectInvalidHeader(t *testing.T) {
	if _, err := Encode(&commonpb.Header{Fields: map[string]*commonpb.Payload{"nil": nil}}); err == nil {
		t.Fatal("nil payload accepted")
	}
	for _, data := range [][]byte{{0xff}, nil} {
		if _, err := Decode(map[string][]byte{"bad": data}); err == nil {
			t.Fatal("invalid envelope accepted")
		}
	}
}
