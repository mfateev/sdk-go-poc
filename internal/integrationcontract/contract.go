// Package integrationcontract versions the private host/workflow byte protocol.
// A fixed header is validated before JSON, converters, or user code run. It is
// transport metadata, not a history event or application payload.
package integrationcontract

import (
	"encoding/binary"
	"fmt"
	"isolate"
)

const (
	ProtocolVersion    uint32 = 1
	MetadataVersion    uint32 = 1
	APIVersion         uint32 = 1
	DeterminismVersion uint32 = 1
	headerSize                = 20
)

// ValidateRuntime rejects a toolchain whose contracts this SDK has not audited.
// Expected versions intentionally do not derive from the linked runtime.
func ValidateRuntime(actual isolate.Contract) error {
	want := isolate.Contract{Metadata: MetadataVersion, API: APIVersion, Determinism: DeterminismVersion}
	if actual != want {
		return fmt.Errorf("isolate integration: incompatible runtime contract %+v, SDK requires %+v; rebuild with a supported toolchain", actual, want)
	}
	return nil
}

// Pack prepends the contract header without invoking application serialization.
func Pack(body []byte) []byte {
	raw := make([]byte, headerSize+len(body))
	copy(raw, "ISOL")
	for i, version := range [...]uint32{ProtocolVersion, MetadataVersion, APIVersion, DeterminismVersion} {
		binary.LittleEndian.PutUint32(raw[4+i*4:], version)
	}
	copy(raw[headerSize:], body)
	return raw
}

// Unpack validates all contracts before exposing the body to any decoder.
func Unpack(raw []byte) ([]byte, error) {
	if len(raw) < headerSize || string(raw[:4]) != "ISOL" {
		return nil, fmt.Errorf("isolate integration: missing protocol header; rebuild host and workflow SDK together")
	}
	for i, check := range [...]struct {
		name    string
		version uint32
	}{
		{"protocol", ProtocolVersion}, {"metadata", MetadataVersion}, {"API", APIVersion}, {"determinism", DeterminismVersion},
	} {
		if version := binary.LittleEndian.Uint32(raw[4+i*4:]); version != check.version {
			return nil, fmt.Errorf("isolate integration: incompatible %s version %d, requires %d; rebuild host and workflow SDK together", check.name, version, check.version)
		}
	}
	return raw[headerSize:], nil
}
