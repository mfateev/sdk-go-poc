package workflow

import (
	"context"
	"errors"
	"testing"
	"time"

	"go.temporal.io/sdk/temporal"
)

func TestChildOptionsInheritanceAndReplacement(t *testing.T) {
	base := WithWorkflowNamespace(WithWorkflowTaskQueue(context.Background(), "parent-queue"), "parent-namespace")
	policy := &RetryPolicy{MaximumAttempts: 3, NonRetryableErrorTypes: []string{"bad"}}
	memo := map[string]any{"key": "initial"}
	ctx := WithChildWorkflowOptions(base, ChildWorkflowOptions{WorkflowID: "child-id", WorkflowRunTimeout: time.Minute, RetryPolicy: policy, Memo: memo, WaitForCancellation: true})
	policy.NonRetryableErrorTypes[0], memo["key"] = "changed", "changed"
	o := GetChildWorkflowOptions(ctx)
	if o.Namespace != "parent-namespace" || o.TaskQueue != "parent-queue" || o.WorkflowID != "child-id" || o.WorkflowRunTimeout != time.Minute || !o.WaitForCancellation || o.RetryPolicy.NonRetryableErrorTypes[0] != "bad" || o.Memo["key"] != "initial" {
		t.Fatalf("child options=%+v", o)
	}
	o.RetryPolicy.MaximumAttempts, o.Memo["key"] = 99, "getter-change"
	if actual := GetChildWorkflowOptions(ctx); actual.RetryPolicy.MaximumAttempts != 3 || actual.Memo["key"] != "initial" {
		t.Fatal("getter exposed mutable options")
	}
	ctx = WithWorkflowID(ctx, "override")
	if GetChildWorkflowOptions(ctx).WorkflowID != "override" {
		t.Fatal("workflow option helper ignored")
	}
	replacement := GetChildWorkflowOptions(WithChildWorkflowOptions(ctx, ChildWorkflowOptions{}))
	if replacement.TaskQueue != "parent-queue" || replacement.Namespace != "parent-namespace" || replacement.WorkflowID != "" || replacement.RetryPolicy != nil || replacement.Memo != nil || replacement.WaitForCancellation || replacement.WorkflowRunTimeout != 0 {
		t.Fatalf("replacement=%+v", replacement)
	}
	if GetChildWorkflowOptions(base).WorkflowID != "" {
		t.Fatal("parent context mutated")
	}
}

func TestInvalidChildResolvesBothFuturesWithoutScheduling(t *testing.T) {
	ctx := context.Background()
	var nilFn func(context.Context) error
	for _, tc := range []struct {
		fn   any
		args []any
	}{
		{nilFn, nil}, {"", nil}, {func(int) error { panic("must not execute") }, []any{1}},
		{func(context.Context, int) error { panic("must not execute") }, []any{"wrong"}},
		{func(context.Context) int { panic("must not execute") }, nil},
	} {
		f := ExecuteChildWorkflow(ctx, tc.fn, tc.args...)
		if !f.IsReady() || !f.GetChildWorkflowExecution().IsReady() || f.Get(ctx, nil) == nil || f.GetChildWorkflowExecution().Get(ctx, nil) == nil {
			t.Fatalf("invalid child not resolved: %T", tc.fn)
		}
	}
	ctx, cancel := context.WithCancel(ctx)
	cancel()
	f := ExecuteChildWorkflow(ctx, "name")
	if !errors.Is(f.Get(ctx, nil), context.Canceled) || !errors.Is(f.GetChildWorkflowExecution().Get(ctx, nil), context.Canceled) {
		t.Fatal("pre-canceled child lost native cancellation")
	}
	typed := temporal.NewSearchAttributes(temporal.NewSearchAttributeKeyInt64("key").ValueSet(42))
	f = ExecuteChildWorkflow(WithChildWorkflowOptions(context.Background(), ChildWorkflowOptions{TypedSearchAttributes: typed}), "name")
	if !f.IsReady() || f.Get(context.Background(), nil) == nil {
		t.Fatal("unsupported typed search attributes silently dropped")
	}
}
