package failurewire

import (
	"testing"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	"go.temporal.io/sdk/converter"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
)

func TestPinnedFailureSchemaMatchesProtobuf(t *testing.T) {
	details, err := converter.GetDefaultDataConverter().ToPayloads("detail", int64(9007199254740993), []byte("private"))
	if err != nil {
		t.Fatal(err)
	}
	base := &failurepb.Failure{Message: "cause", FailureInfo: &failurepb.Failure_ApplicationFailureInfo{ApplicationFailureInfo: &failurepb.ApplicationFailureInfo{Type: "invalid", NonRetryable: true, Details: details, NextRetryDelay: durationpb.New(3 * time.Second), Category: enumspb.APPLICATION_ERROR_CATEGORY_BENIGN}}}
	cases := []*failurepb.Failure{
		{}, base,
		{FailureInfo: &failurepb.Failure_TimeoutFailureInfo{TimeoutFailureInfo: &failurepb.TimeoutFailureInfo{TimeoutType: enumspb.TIMEOUT_TYPE_HEARTBEAT, LastHeartbeatDetails: details}}},
		{FailureInfo: &failurepb.Failure_CanceledFailureInfo{CanceledFailureInfo: &failurepb.CanceledFailureInfo{Details: details}}},
		{FailureInfo: &failurepb.Failure_TerminatedFailureInfo{TerminatedFailureInfo: &failurepb.TerminatedFailureInfo{}}},
		{FailureInfo: &failurepb.Failure_ServerFailureInfo{ServerFailureInfo: &failurepb.ServerFailureInfo{NonRetryable: true}}},
		{FailureInfo: &failurepb.Failure_ResetWorkflowFailureInfo{ResetWorkflowFailureInfo: &failurepb.ResetWorkflowFailureInfo{LastHeartbeatDetails: details}}},
		{FailureInfo: &failurepb.Failure_ActivityFailureInfo{ActivityFailureInfo: &failurepb.ActivityFailureInfo{ScheduledEventId: 9007199254740993, StartedEventId: 4, Identity: "worker", ActivityType: &commonpb.ActivityType{Name: "Act"}, ActivityId: "id", RetryState: enumspb.RETRY_STATE_MAXIMUM_ATTEMPTS_REACHED}}},
		{FailureInfo: &failurepb.Failure_ChildWorkflowExecutionFailureInfo{ChildWorkflowExecutionFailureInfo: &failurepb.ChildWorkflowExecutionFailureInfo{Namespace: "namespace", WorkflowExecution: &commonpb.WorkflowExecution{WorkflowId: "child", RunId: "run"}, WorkflowType: &commonpb.WorkflowType{Name: "Workflow"}, InitiatedEventId: 3, StartedEventId: 4, RetryState: enumspb.RETRY_STATE_NON_RETRYABLE_FAILURE}}},
		{FailureInfo: &failurepb.Failure_NexusOperationExecutionFailureInfo{NexusOperationExecutionFailureInfo: &failurepb.NexusOperationFailureInfo{ScheduledEventId: 3, Endpoint: "endpoint", Service: "service", Operation: "operation", OperationId: "id", OperationToken: "token"}}},
		{FailureInfo: &failurepb.Failure_NexusHandlerFailureInfo{NexusHandlerFailureInfo: &failurepb.NexusHandlerFailureInfo{Type: "handler", RetryBehavior: enumspb.NEXUS_HANDLER_ERROR_RETRY_BEHAVIOR_NON_RETRYABLE}}},
	}
	for i, input := range cases {
		input = proto.Clone(input).(*failurepb.Failure)
		input.Message, input.Source, input.StackTrace = "message", "source", "stack"
		input.EncodedAttributes, input.Cause = details.Payloads[0], base
		standard, err := proto.MarshalOptions{Deterministic: true}.Marshal(input)
		if err != nil {
			t.Fatal(err)
		}
		decoded, err := Decode(standard)
		if err != nil || !proto.Equal(input, decoded) {
			t.Fatalf("case %d decode: %v", i, err)
		}
		encoded, err := Encode(input)
		if err != nil {
			t.Fatal(err)
		}
		output := new(failurepb.Failure)
		if err := proto.Unmarshal(encoded, output); err != nil || !proto.Equal(input, output) {
			t.Fatalf("case %d encode: %v", i, err)
		}
		// Decoding owns all byte storage, including nested payload metadata.
		clear(standard)
		if !proto.Equal(input, decoded) {
			t.Fatalf("case %d retained wire bytes", i)
		}
	}
}

func TestMalformedFailuresAreRejected(t *testing.T) {
	for _, data := range [][]byte{{0}, {0x0a, 0x80}, {0x08, 1}, {0xf8, 0x01, 1}, {0x2a, 1, 0x80}, {0x0a, 1, 0xff}} {
		if _, err := Decode(data); err == nil {
			t.Fatalf("accepted %x", data)
		}
	}
}

func TestRepeatedFailureInfoMergesLikeProtobuf(t *testing.T) {
	// Repeated occurrences of the same message oneof merge their contents.
	data := []byte{0x2a, 3, 0x0a, 1, 'x', 0x2a, 2, 0x10, 1}
	standard := new(failurepb.Failure)
	if err := proto.Unmarshal(data, standard); err != nil {
		t.Fatal(err)
	}
	decoded, err := Decode(data)
	if err != nil || !proto.Equal(standard, decoded) {
		t.Fatalf("oneof merge: %v", err)
	}
}
