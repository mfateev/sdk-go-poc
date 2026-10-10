// Package tracingheader implements the SDK JSON trace-header format directly.
// Worker DataConverters and payload codecs do not encode observation records.
package tracingheader

import (
	"encoding/json"
	"fmt"
	commonpb "go.temporal.io/api/common/v1"
)

func Encode(data map[string]string) (*commonpb.Payload, error) {
	raw, err := json.Marshal(data)
	if err != nil {
		return nil, err
	}
	return &commonpb.Payload{Metadata: map[string][]byte{"encoding": []byte("json/plain")}, Data: raw}, nil
}
func Decode(payload *commonpb.Payload, out *map[string]string) error {
	if payload == nil || string(payload.Metadata["encoding"]) != "json/plain" {
		return fmt.Errorf("tracing: expected json/plain header")
	}
	return json.Unmarshal(payload.Data, out)
}
