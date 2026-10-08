package workflow

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/mfateev/sdk-go-poc/internal/payloadwire"
	commonpb "go.temporal.io/api/common/v1"
	goWorkflow "go.temporal.io/sdk/workflow"
)

func TestContinuationUsesSDKErrorAndCopiesArgumentsAndOptions(t *testing.T) {
	ctx := context.WithValue(context.Background(), runOptionsKey{}, RunOptions{TaskQueue: "original", WorkflowRunTimeout: time.Hour, WorkflowTaskTimeout: time.Second})
	ctx = WithWorkflowTaskQueue(ctx, "next")
	ctx = WithWorkflowRunTimeout(ctx, 2*time.Hour)
	policy := &RetryPolicy{MaximumAttempts: 3, NonRetryableErrorTypes: []string{"invalid"}}
	err := NewContinueAsNewErrorWithOptions(ctx, ContinueAsNewErrorOptions{RetryPolicy: policy, BackoffStartInterval: 2 * time.Second}, "next-workflow", 42, "hello")
	var sdkError *goWorkflow.ContinueAsNewError
	if !errors.As(err, &sdkError) || sdkError.TaskQueueName != "next" || sdkError.WorkflowRunTimeout != 2*time.Hour || sdkError.WorkflowTaskTimeout != time.Second {
		t.Fatalf("SDK error: %+v", err)
	}
	policy.NonRetryableErrorTypes[0] = "mutated"
	if sdkError.RetryPolicy.NonRetryableErrorTypes[0] != "invalid" {
		t.Fatal("retry policy aliased caller state")
	}
	var number int
	var text string
	if err := instanceDataConverter.FromPayloads(sdkError.Input, &number, &text); err != nil || number != 42 || text != "hello" {
		t.Fatalf("arguments: %d %s %v", number, text, err)
	}
	sdkError.Header = &commonpb.Header{Fields: map[string]*commonpb.Payload{"trace": {Metadata: map[string][]byte{"encoding": []byte("binary/plain")}, Data: []byte("trace")}}}
	request, err := continuationRequest(fmt.Errorf("wrapped: %w", sdkError))
	if err != nil {
		t.Fatal(err)
	}
	if request.Name != "next-workflow" || request.BackoffStartInterval != int64(2*time.Second) {
		t.Fatalf("wire options: %+v", request)
	}
	header, err := payloadwire.Decode(request.Header["trace"])
	if err != nil || string(header.Payloads[0].Data) != "trace" {
		t.Fatalf("header: %+v %v", header, err)
	}
	if !IsContinueAsNewError(fmt.Errorf("wrapped: %w", sdkError)) || IsContinueAsNewError(errors.New("ordinary")) {
		t.Fatal("continuation identity lost")
	}
}
func TestContinuationRejectsInvalidArgumentsBeforeHostLookup(t *testing.T) {
	ctx := context.Background()
	for _, fn := range []func(){
		func() { WithWorkflowTaskQueue(ctx, "") },
		func() { NewContinueAsNewError(ctx, "", 1) },
		func() { NewContinueAsNewError(ctx, func(context.Context, int) error { return nil }, "wrong") },
		func() { NewContinueAsNewError(ctx, "target", make(chan int)) },
	} {
		func() {
			defer func() {
				if recover() == nil {
					t.Error("invalid continuation did not fail task")
				}
			}()
			fn()
		}()
	}
}
