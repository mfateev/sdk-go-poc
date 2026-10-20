// Package payloadwire encodes raw Temporal Payloads responses with the ordinary
// private protobuf runtime. Structured SDK transport embeds these as submessages.
package payloadwire

import (
	"fmt"
	commonpb "go.temporal.io/api/common/v1"
	"google.golang.org/protobuf/proto"
)

func Validate(payloads *commonpb.Payloads) error {
	for _, payload := range payloads.GetPayloads() {
		if payload == nil || len(payload.ExternalPayloads) != 0 {
			return fmt.Errorf("payloads: nil or external payload is unsupported")
		}
	}
	return nil
}

func Decode(data []byte) (*commonpb.Payloads, error) {
	result := new(commonpb.Payloads)
	if err := proto.Unmarshal(data, result); err != nil {
		return nil, err
	}
	if err := Validate(result); err != nil {
		return nil, err
	}
	return result, nil
}

func Encode(payloads *commonpb.Payloads) ([]byte, error) {
	if payloads == nil {
		return nil, nil
	}
	if err := Validate(payloads); err != nil {
		return nil, err
	}
	return proto.MarshalOptions{Deterministic: true}.Marshal(payloads)
}
