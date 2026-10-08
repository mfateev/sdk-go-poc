// Package payloadwire encodes the small Temporal Payloads envelope used at the
// isolate boundary. It uses protobuf wire encoding without invoking protobuf's
// process-wide reflective decoder from an isolate.
package payloadwire

import (
	"fmt"
	"slices"

	commonpb "go.temporal.io/api/common/v1"
	"google.golang.org/protobuf/encoding/protowire"
)

// Decode parses ordinary Payloads containing metadata and data. External
// payload references and unknown fields are outside the trusted POC subset.
func Decode(data []byte) (*commonpb.Payloads, error) {
	result := new(commonpb.Payloads)
	for len(data) != 0 {
		number, wireType, n := protowire.ConsumeTag(data)
		if n < 0 {
			return nil, fmt.Errorf("payloads: invalid protobuf wire (code %d)", n)
		}
		data = data[n:]
		if number != 1 || wireType != protowire.BytesType {
			return nil, fmt.Errorf("payloads: unsupported field %d", number)
		}
		value, n := protowire.ConsumeBytes(data)
		if n < 0 {
			return nil, fmt.Errorf("payloads: invalid protobuf wire (code %d)", n)
		}
		data = data[n:]
		payload, err := decodePayload(value)
		if err != nil {
			return nil, err
		}
		result.Payloads = append(result.Payloads, payload)
	}
	return result, nil
}

func decodePayload(data []byte) (*commonpb.Payload, error) {
	result := new(commonpb.Payload)
	for len(data) != 0 {
		number, wireType, n := protowire.ConsumeTag(data)
		if n < 0 {
			return nil, fmt.Errorf("payload: invalid protobuf wire (code %d)", n)
		}
		data = data[n:]
		if wireType != protowire.BytesType {
			return nil, fmt.Errorf("payload: invalid wire type for field %d", number)
		}
		value, n := protowire.ConsumeBytes(data)
		if n < 0 {
			return nil, fmt.Errorf("payload: invalid protobuf wire (code %d)", n)
		}
		data = data[n:]
		switch number {
		case 1:
			key, metadata, err := decodeMetadata(value)
			if err != nil {
				return nil, err
			}
			if result.Metadata == nil {
				result.Metadata = make(map[string][]byte)
			}
			result.Metadata[key] = metadata
		case 2:
			result.Data = slices.Clone(value)
		default:
			return nil, fmt.Errorf("payload: unsupported field %d", number)
		}
	}
	return result, nil
}

func decodeMetadata(data []byte) (string, []byte, error) {
	var key string
	var value []byte
	for len(data) != 0 {
		number, wireType, n := protowire.ConsumeTag(data)
		if n < 0 {
			return "", nil, fmt.Errorf("payload metadata: invalid protobuf wire (code %d)", n)
		}
		data = data[n:]
		if wireType != protowire.BytesType || number < 1 || number > 2 {
			return "", nil, fmt.Errorf("payload metadata: unsupported field %d", number)
		}
		field, n := protowire.ConsumeBytes(data)
		if n < 0 {
			return "", nil, fmt.Errorf("payload metadata: invalid protobuf wire (code %d)", n)
		}
		data = data[n:]
		if number == 1 {
			key = string(field)
		} else {
			value = slices.Clone(field)
		}
	}
	return key, value, nil
}

// Encode returns protobuf wire bytes for ordinary Payloads. Metadata keys are
// sorted so the internal transport is stable across map iteration orders.
func Encode(payloads *commonpb.Payloads) ([]byte, error) {
	if payloads == nil {
		return nil, nil
	}
	var result []byte
	for _, payload := range payloads.Payloads {
		if payload == nil || len(payload.ExternalPayloads) != 0 {
			return nil, fmt.Errorf("payloads: nil or external payload is unsupported")
		}
		var value []byte
		keys := make([]string, 0, len(payload.Metadata))
		for key := range payload.Metadata {
			keys = append(keys, key)
		}
		slices.Sort(keys)
		for _, key := range keys {
			var entry []byte
			entry = protowire.AppendTag(entry, 1, protowire.BytesType)
			entry = protowire.AppendString(entry, key)
			entry = protowire.AppendTag(entry, 2, protowire.BytesType)
			entry = protowire.AppendBytes(entry, payload.Metadata[key])
			value = protowire.AppendTag(value, 1, protowire.BytesType)
			value = protowire.AppendBytes(value, entry)
		}
		if len(payload.Data) != 0 {
			value = protowire.AppendTag(value, 2, protowire.BytesType)
			value = protowire.AppendBytes(value, payload.Data)
		}
		result = protowire.AppendTag(result, 1, protowire.BytesType)
		result = protowire.AppendBytes(result, value)
	}
	return result, nil
}
