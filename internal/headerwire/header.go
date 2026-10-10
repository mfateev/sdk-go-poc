// Package headerwire transports opaque Temporal headers without a DataConverter.
package headerwire

import (
	"fmt"
	"github.com/mfateev/sdk-go-poc/internal/payloadwire"
	commonpb "go.temporal.io/api/common/v1"
)

func Encode(header *commonpb.Header) (map[string][]byte, error) {
	if header == nil {
		return nil, nil
	}
	out := make(map[string][]byte, len(header.Fields))
	for key, payload := range header.Fields {
		if payload == nil {
			return nil, fmt.Errorf("nil header payload %q", key)
		}
		raw, err := payloadwire.Encode(&commonpb.Payloads{Payloads: []*commonpb.Payload{payload}})
		if err != nil {
			return nil, err
		}
		out[key] = raw
	}
	return out, nil
}
func Decode(fields map[string][]byte) (*commonpb.Header, error) {
	if fields == nil {
		return nil, nil
	}
	out := &commonpb.Header{Fields: make(map[string]*commonpb.Payload, len(fields))}
	for key, raw := range fields {
		payloads, err := payloadwire.Decode(raw)
		if err != nil {
			return nil, err
		}
		if len(payloads.Payloads) != 1 {
			return nil, fmt.Errorf("invalid header payload %q", key)
		}
		out.Fields[key] = payloads.Payloads[0]
	}
	return out, nil
}
