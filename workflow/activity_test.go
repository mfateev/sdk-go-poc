package workflow

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"
)

func TestActivityOptionsCopiesPolicyAndPreservesTaskQueue(t *testing.T) {
	policy := &RetryPolicy{InitialInterval: time.Second, MaximumAttempts: 3, NonRetryableErrorTypes: []string{"invalid"}}
	base := WithActivityOptions(context.Background(), ActivityOptions{TaskQueue: "activities", RetryPolicy: policy, HeartbeatTimeout: time.Minute})
	policy.MaximumAttempts = 99
	policy.NonRetryableErrorTypes[0] = "changed"
	updated := WithActivityOptions(base, ActivityOptions{StartToCloseTimeout: time.Second})
	if o := GetActivityOptions(updated); o.TaskQueue != "activities" || o.HeartbeatTimeout != 0 || o.RetryPolicy != nil {
		t.Fatalf("replacement options: %+v", o)
	}
	o := GetActivityOptions(base)
	if o.RetryPolicy.MaximumAttempts != 3 || o.RetryPolicy.NonRetryableErrorTypes[0] != "invalid" {
		t.Fatalf("policy was aliased: %+v", o.RetryPolicy)
	}
	o.RetryPolicy.MaximumAttempts = 42
	if GetActivityOptions(base).RetryPolicy.MaximumAttempts != 3 {
		t.Fatal("getter leaked mutable context options")
	}
	derived := WithWaitForCancellation(WithHeartbeatTimeout(base, 2*time.Second), true)
	if !GetActivityOptions(derived).WaitForCancellation || GetActivityOptions(base).WaitForCancellation {
		t.Fatal("derived context mutated parent")
	}
}

func TestExecuteActivityValidatesReferencesAndArguments(t *testing.T) {
	ctx := WithStartToCloseTimeout(context.Background(), time.Second)
	var nilActivity func(context.Context, int) (string, error)
	for _, tc := range []struct {
		fn   any
		args []any
	}{
		{nilActivity, []any{5}}, {func(context.Context, int) (string, error) { panic("host only") }, nil},
		{func(context.Context, int) (string, error) { panic("host only") }, []any{"wrong"}},
		{func(int) (string, error) { panic("host only") }, []any{1}}, {"", nil},
	} {
		f := ExecuteActivity(ctx, tc.fn, tc.args...)
		if !f.IsReady() || f.Get(ctx, nil) == nil {
			t.Fatalf("accepted invalid call: %v", reflect.TypeOf(tc.fn))
		}
	}
	if err := ExecuteActivity(context.Background(), "name").Get(ctx, nil); err == nil {
		t.Fatal("missing timeouts accepted")
	}
	ctx, cancel := context.WithCancel(ctx)
	cancel()
	if err := ExecuteActivity(ctx, "name").Get(ctx, nil); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled call: %v", err)
	}
}

func TestFutureRepeatableDecodeAndReadiness(t *testing.T) {
	payload, err := encodeActivityArgs([]any{struct{ Name string }{"hello"}})
	if err != nil {
		t.Fatal(err)
	}
	f := &activityFuture{done: make(chan struct{}), payload: payload}
	if f.IsReady() {
		t.Fatal("unfinished future ready")
	}
	close(f.done)
	if !f.IsReady() {
		t.Fatal("finished future not ready")
	}
	for range 3 {
		var result struct{ Name string }
		if err := f.Get(context.Background(), &result); err != nil || result.Name != "hello" {
			t.Fatalf("decode: %+v %v", result, err)
		}
	}
	if err := f.Get(context.Background(), nil); err != nil {
		t.Fatal(err)
	}
	if err := f.Get(context.Background(), 42); err == nil {
		t.Fatal("accepted nonpointer result")
	}
	var nilPtr *string
	if err := f.Get(context.Background(), nilPtr); err == nil {
		t.Fatal("accepted nil result pointer")
	}
	var wrong int
	if err := f.Get(context.Background(), &wrong); err == nil {
		t.Fatal("accepted mismatched result")
	}
	// Waiting context cancellation does not override an already completed result.
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	var result struct{ Name string }
	if err := f.Get(canceled, &result); err != nil {
		t.Fatal(err)
	}
}

func TestExecutionContextInheritsWorkflowTaskQueue(t *testing.T) {
	// Avoid starting the host cancellation listener by using an already canceled execution.
	ctx, cancel := executionContext(true, "workflow-queue")
	defer cancel()
	if GetActivityOptions(ctx).TaskQueue != "workflow-queue" {
		t.Fatal("workflow task queue not inherited")
	}
	ctx = WithActivityOptions(ctx, ActivityOptions{ScheduleToCloseTimeout: time.Minute})
	if GetActivityOptions(ctx).TaskQueue != "workflow-queue" {
		t.Fatal("default options erased workflow task queue")
	}
}
