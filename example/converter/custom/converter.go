// Package custom is intentionally not imported by the workflow package. Its
// state enters the isolate only through the worker-configured support handle.
package custom

import (
	"encoding/json"
	"fmt"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/sdk/converter"
)

var Initializations int

func init() { Initializations++ }

//go:isolate
func New(config []byte) (converter.DataConverter, error) {
	if Initializations != 1 {
		return nil, fmt.Errorf("converter package not initialized independently")
	}
	if string(config) == "fail" {
		return nil, fmt.Errorf("invalid converter configuration")
	}
	return converter.NewCompositeDataConverter(converter.NewNilPayloadConverter(), converter.NewByteSlicePayloadConverter(),
		&payloadConverter{prefix: string(config), cache: make(map[string]int)}), nil
}

type payloadConverter struct {
	prefix string
	cache  map[string]int
}

func (*payloadConverter) Encoding() string { return "json/custom-isolate" }
func (p *payloadConverter) ToPayload(value any) (*commonpb.Payload, error) {
	p.cache["encoded"]++ // Must be writable in normal and read-only scratch owners.
	data, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	return &commonpb.Payload{Metadata: map[string][]byte{converter.MetadataEncoding: []byte(p.Encoding()), "prefix": []byte(p.prefix)}, Data: data}, nil
}
func (p *payloadConverter) FromPayload(value *commonpb.Payload, target any) error {
	p.cache["decoded"]++
	if string(value.Metadata["prefix"]) != p.prefix {
		return fmt.Errorf("converter configuration mismatch")
	}
	return json.Unmarshal(value.Data, target)
}
func (*payloadConverter) ToString(p *commonpb.Payload) string { return string(p.Data) }
