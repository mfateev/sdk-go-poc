package workflow

import (
	"context"
	"errors"
	goWorkflow "go.temporal.io/sdk/workflow"
	"testing"
	"time"
)

func TestLocalOptionsAndInvalidInvocation(t *testing.T) {
	o := LocalActivityOptions{StartToCloseTimeout: time.Second, RetryPolicy: &RetryPolicy{NonRetryableErrorTypes: []string{"keep"}}}
	ctx := WithLocalActivityOptions(context.Background(), o)
	o.RetryPolicy.NonRetryableErrorTypes[0] = "changed"
	if GetLocalActivityOptions(ctx).RetryPolicy.NonRetryableErrorTypes[0] != "keep" {
		t.Fatal("options shared mutable policy")
	}
	fn := func(context.Context, int) (string, error) { panic("never invoke on workflow goroutine") }
	if err := ExecuteLocalActivity(ctx, fn, "wrong").Get(ctx, nil); err == nil {
		t.Fatal("invalid local input accepted")
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if err := ExecuteLocalActivity(canceled, fn, 1).Get(ctx, nil); !errors.Is(err, context.Canceled) {
		t.Fatal("pre-canceled local scheduled")
	}
}
func TestNexusInvalidAndPreCanceled(t *testing.T) {
	c := NewNexusClient("endpoint", "service")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	f := c.ExecuteOperation(ctx, "op", 1, NexusOperationOptions{})
	if !errors.Is(f.Get(ctx, nil), context.Canceled) || !errors.Is(f.GetNexusOperationExecution().Get(ctx, nil), context.Canceled) {
		t.Fatal("pre-canceled Nexus did not resolve both futures")
	}
	if err := c.ExecuteOperation(context.Background(), 123, 1, NexusOperationOptions{}).Get(ctx, nil); err == nil {
		t.Fatal("invalid Nexus operation accepted")
	}
}
func TestFailedSessionActivityAndToken(t *testing.T) {
	if SessionStateOpen != goWorkflow.SessionStateOpen || SessionStateFailed != goWorkflow.SessionStateFailed || SessionStateClosed != goWorkflow.SessionStateClosed {
		t.Fatal("session enum diverged from pinned SDK")
	}
	s := &SessionInfo{SessionState: SessionStateFailed, taskqueue: "resource-queue"}
	ctx := context.WithValue(context.Background(), sessionKey{}, s)
	if err := ExecuteActivity(ctx, "activity").Get(ctx, nil); !errors.Is(err, ErrSessionFailed) {
		t.Fatal("failed session scheduled activity")
	}
	if string(s.GetRecreateToken()) != "{\"Taskqueue\":\"resource-queue\"}" {
		t.Fatal("SDK recreate token format changed")
	}
	if _, err := CreateSession(ctx, nil); err == nil {
		t.Fatal("missing session options accepted")
	}
}
