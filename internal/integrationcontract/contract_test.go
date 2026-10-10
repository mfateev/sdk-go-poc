package integrationcontract

import (
	"bytes"
	"encoding/binary"
	"isolate"
	"strings"
	"testing"
)

func TestContractValidationBeforeBody(t *testing.T) {
	body := []byte(`{"payload":"not decoded by the handshake"}`)
	plain, err := Unpack(Pack(body))
	if err != nil || !bytes.Equal(plain, body) {
		t.Fatalf("body=%q err=%v", plain, err)
	}
	for _, raw := range [][]byte{nil, []byte(`{"old":"startup"}`), []byte("ISOL")} {
		if body, err := Unpack(raw); err == nil || body != nil {
			t.Fatal("accepted missing/truncated header")
		}
	}
	for i, name := range []string{"protocol", "metadata", "API", "determinism"} {
		for _, version := range []uint32{0, 2} {
			raw := Pack(body)
			binary.LittleEndian.PutUint32(raw[4+i*4:], version)
			if body, err := Unpack(raw); err == nil || body != nil || !strings.Contains(err.Error(), name) {
				t.Fatalf("%s/%d: %q %v", name, version, body, err)
			}
		}
	}
}

func TestSDKRuntimeContractIsExplicit(t *testing.T) {
	if err := ValidateRuntime(isolate.CurrentContract()); err != nil {
		t.Fatal(err)
	}
	for _, changed := range []isolate.Contract{{}, {Metadata: 2, API: 1, Determinism: 1}, {Metadata: 1, API: 2, Determinism: 1}, {Metadata: 1, API: 1, Determinism: 2}} {
		if err := ValidateRuntime(changed); err == nil {
			t.Fatalf("accepted %+v", changed)
		}
	}
}
