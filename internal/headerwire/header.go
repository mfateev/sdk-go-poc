// Package headerwire snapshots opaque headers without invoking application converters.
package headerwire

import (
	"fmt"

	"github.com/mfateev/sdk-go-poc/protobufcopy"
	commonpb "go.temporal.io/api/common/v1"
)

func copyHeader(header *commonpb.Header) (*commonpb.Header, error) {
	if header == nil {
		return nil, nil
	}
	for key, payload := range header.Fields {
		if payload == nil {
			return nil, fmt.Errorf("nil header payload %q", key)
		}
	}
	out := protobufcopy.Clone(header)
	if out.Fields == nil {
		out.Fields = make(map[string]*commonpb.Payload)
	}
	return out, nil
}

// Encode constructs a private submessage; only the containing transport is marshaled.
func Encode(header *commonpb.Header) (*commonpb.Header, error) { return copyHeader(header) }
func Decode(header *commonpb.Header) (*commonpb.Header, error) { return copyHeader(header) }
