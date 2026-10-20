// Package failurewire encodes raw Temporal Failure responses with the ordinary
// private protobuf runtime. Structured SDK transport embeds these as submessages.
package failurewire

import (
	"fmt"

	"github.com/mfateev/sdk-go-poc/internal/payloadwire"
	commonpb "go.temporal.io/api/common/v1"
	failurepb "go.temporal.io/api/failure/v1"
	"google.golang.org/protobuf/proto"
)

func validate(failure *failurepb.Failure) error {
	for depth := 0; failure != nil; depth++ {
		if depth > 1000 {
			return fmt.Errorf("failure: recursion limit exceeded")
		}
		if failure.EncodedAttributes != nil {
			if err := payloadwire.Validate(&commonpb.Payloads{Payloads: []*commonpb.Payload{failure.EncodedAttributes}}); err != nil {
				return err
			}
		}
		for _, details := range []*commonpb.Payloads{
			failure.GetApplicationFailureInfo().GetDetails(),
			failure.GetCanceledFailureInfo().GetDetails(),
			failure.GetTimeoutFailureInfo().GetLastHeartbeatDetails(),
			failure.GetResetWorkflowFailureInfo().GetLastHeartbeatDetails(),
		} {
			if err := payloadwire.Validate(details); err != nil {
				return err
			}
		}
		failure = failure.Cause
	}
	return nil
}

func Encode(failure *failurepb.Failure) ([]byte, error) {
	if failure == nil {
		return nil, nil
	}
	if err := validate(failure); err != nil {
		return nil, err
	}
	return proto.MarshalOptions{Deterministic: true}.Marshal(failure)
}

func Decode(data []byte) (*failurepb.Failure, error) {
	failure := new(failurepb.Failure)
	if err := proto.Unmarshal(data, failure); err != nil {
		return nil, err
	}
	if err := validate(failure); err != nil {
		return nil, err
	}
	return failure, nil
}
