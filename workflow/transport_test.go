package workflow

import (
	"bytes"
	"math"
	"testing"
	"time"

	wirepb "github.com/mfateev/sdk-go-poc/internal/transportpb"
	commonpb "go.temporal.io/api/common/v1"
	failurepb "go.temporal.io/api/failure/v1"
	"google.golang.org/protobuf/proto"
)

func TestTransportUsesNestedProtobufWithoutTextExpansion(t *testing.T) {
	payloads := &commonpb.Payloads{Payloads: []*commonpb.Payload{{Metadata: map[string][]byte{"encoding": []byte("binary/plain")}, Data: []byte{0, 255, 1, 254}}}}
	header := &commonpb.Header{Fields: map[string]*commonpb.Payload{"trace": {Data: []byte{255, 0, 128}}}}
	request := ActivityPayloadRequest{ID: 1<<53 + 1, Name: "activity", Payloads: payloads, Header: header, Options: &ActivityOptions{StartToCloseTimeout: time.Second, RetryPolicy: &RetryPolicy{MaximumAttempts: 3}, Priority: Priority{PriorityKey: 7, FairnessWeight: 0.5}}}
	raw, err := MarshalTransport(request)
	if err != nil {
		t.Fatal(err)
	}
	// Inspect the shared schema independently of the domain conversion.
	envelope := new(wirepb.ActivityPayloadRequest)
	if err = proto.Unmarshal(raw, envelope); err != nil {
		t.Fatal(err)
	}
	if !proto.Equal(payloads, envelope.Payloads) || !proto.Equal(header, envelope.Header) || envelope.ID != request.ID || !bytes.Contains(raw, payloads.Payloads[0].Data) {
		t.Fatal("payload/header submessages or integer precision lost")
	}
	var roundTrip ActivityPayloadRequest
	if err = UnmarshalTransport(raw, &roundTrip); err != nil {
		t.Fatal(err)
	}
	if roundTrip.Options.StartToCloseTimeout != time.Second || roundTrip.Options.RetryPolicy.MaximumAttempts != 3 || roundTrip.Options.Priority.FairnessWeight != 0.5 {
		t.Fatal("activity configuration changed")
	}
}
func TestLocalOutcomePreservesCommonFutureWireFields(t *testing.T) {
	outcome := LocalActivityOutcome{ActivityOutcome: ActivityOutcome{Failed: true, Error: "failed", Failure: &failurepb.Failure{Message: "failure"}}, Backoff: time.Second, Attempt: 3}
	raw, err := MarshalTransport(outcome)
	if err != nil {
		t.Fatal(err)
	}
	var common ActivityOutcome
	if err = UnmarshalTransport(raw, &common); err != nil {
		t.Fatal(err)
	}
	if !common.Failed || common.Error != "failed" || common.Failure.GetMessage() != "failure" {
		t.Fatal("local outcome cannot be read by common future")
	}
	var local LocalActivityOutcome
	if err = UnmarshalTransport(raw, &local); err != nil || local.Backoff != time.Second || local.Attempt != 3 {
		t.Fatalf("local retry framing: %v", err)
	}
}
func TestTransportRejectsMalformedValues(t *testing.T) {
	for _, data := range [][]byte{{0}, {0x0a, 0x80}} {
		var start PayloadStart
		if err := UnmarshalTransport(data, &start); err == nil {
			t.Fatalf("invalid wire accepted: %x", data)
		}
	}
	raw, err := proto.Marshal(&wirepb.ActivityPayloadRequest{Options: &wirepb.ActivityOptions{RetryPolicy: &wirepb.RetryPolicy{MaximumAttempts: math.MaxInt64}}})
	if err != nil {
		t.Fatal(err)
	}
	var activity ActivityPayloadRequest
	if err = UnmarshalTransport(raw, &activity); err == nil {
		t.Fatal("integer overflow accepted")
	}
	raw, err = proto.Marshal(&wirepb.ChildRequest{Options: &wirepb.ChildWorkflowOptions{VersioningOverride: &wirepb.VersioningOverride{Kind: "unknown"}}})
	if err != nil {
		t.Fatal(err)
	}
	var child ChildRequest
	if err = UnmarshalTransport(raw, &child); err == nil {
		t.Fatal("unknown versioning override accepted")
	}
	for _, destination := range []any{(*PayloadStart)(nil), (*bool)(nil), (*uint64)(nil), (*Version)(nil), (*map[string]*commonpb.Payload)(nil)} {
		if err = UnmarshalTransport(nil, destination); err == nil {
			t.Fatalf("nil destination accepted: %T", destination)
		}
	}
}
