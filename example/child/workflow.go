// Package child exercises SDK child-workflow contracts with native contexts.
package child

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/mfateev/sdk-go-poc/workflow"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/temporal"
	goWorkflow "go.temporal.io/sdk/workflow"
)

var executions int

//go:isolate
func Child(ctx context.Context, mode string) (int64, error) {
	executions++
	if executions != 1 {
		return 0, errors.New("child reused parent isolate state")
	}
	switch mode {
	case "signal":
		input := <-workflow.GetSignalChannel(ctx, "go")
		if input.Err != nil {
			return 0, input.Err
		}
		if string(input.Signal.Input) != "go" {
			return 0, errors.New("wrong child signal")
		}
	case "cancel", "cancel-before-start", "cancel-wait":
		<-ctx.Done()
		return 0, ctx.Err()
	case "failure":
		return 0, temporal.NewNonRetryableApplicationError("child rejected", "ChildFailure", nil, int64(42))
	case "retry-failure":
		return 0, temporal.NewApplicationError("retry child", "ChildFailure", int64(42))
	case "child-continue":
		return 0, workflow.NewContinueAsNewError(ctx, Child, "success")
	}
	return 9007199254740993, nil
}

// OrdinaryChild deliberately uses the unmodified SDK to check interoperability.
func OrdinaryChild(ctx goWorkflow.Context, mode string) (int64, error) {
	return 9007199254740993, nil
}

//go:isolate
func Parent(ctx context.Context, mode string) (int64, error) {
	executions++
	if executions != 1 {
		return 0, errors.New("parent reused isolate state")
	}
	o := workflow.ChildWorkflowOptions{WorkflowRunTimeout: 2 * time.Minute,
		RetryPolicy:       &temporal.RetryPolicy{MaximumAttempts: 1},
		ParentClosePolicy: enumspb.PARENT_CLOSE_POLICY_ABANDON,
		Memo:              map[string]any{"precise": int64(9007199254740993)},
		StaticSummary:     "isolate child", StaticDetails: "child integration check",
		WaitForCancellation: mode == "cancel-wait"}
	if mode == "options" {
		o.SearchAttributes = map[string]any{"CustomIntField": int64(9007199254740993)}
	}
	if mode == "retry-failure" {
		o.RetryPolicy.MaximumAttempts = 2
		o.RetryPolicy.InitialInterval = 10 * time.Millisecond
		o.RetryPolicy.MaximumInterval = 10 * time.Millisecond
	}
	ctx = workflow.WithChildWorkflowOptions(ctx, o)
	if mode == "duplicate" {
		ctx = workflow.WithWorkflowID(ctx, "duplicate-child")
		first := workflow.ExecuteChildWorkflow(ctx, Child, "signal")
		if err := first.GetChildWorkflowExecution().Get(ctx, nil); err != nil {
			return 0, err
		}
		second := workflow.ExecuteChildWorkflow(ctx, Child, "success")
		var alreadyStarted *temporal.ChildWorkflowExecutionAlreadyStartedError
		if err := second.GetChildWorkflowExecution().Get(ctx, nil); !errors.As(err, &alreadyStarted) {
			return 0, fmt.Errorf("duplicate start error identity: %w", err)
		}
		if err := second.Get(ctx, nil); !errors.As(err, &alreadyStarted) {
			return 0, fmt.Errorf("duplicate result error identity: %w", err)
		}
		if err := first.SignalChildWorkflow(ctx, "go", []byte("go")).Get(ctx, nil); err != nil {
			return 0, err
		}
		if err := first.Get(ctx, nil); err != nil {
			return 0, err
		}
		return 1, nil
	}
	childContext, cancel := context.WithCancel(ctx)
	if mode != "ignored" {
		defer cancel()
	}
	if mode == "already-canceled" {
		cancel()
	}
	var fn any = Child
	if mode == "ordinary" {
		fn = OrdinaryChild
	}
	if mode == "name" {
		fn = "ChildAlias"
	}
	f := workflow.ExecuteChildWorkflow(childContext, fn, mode)
	if mode == "already-canceled" {
		if !f.IsReady() || !f.GetChildWorkflowExecution().IsReady() {
			return 0, errors.New("canceled child not ready")
		}
		if !errors.Is(f.Get(ctx, nil), context.Canceled) || !errors.Is(f.GetChildWorkflowExecution().Get(ctx, nil), context.Canceled) {
			return 0, errors.New("pre-start cancellation lost")
		}
		return 1, nil
	}
	if mode == "ignored" {
		return 1, nil
	}
	if mode == "cancel-before-start" {
		cancel()
	}
	var execution workflow.Execution
	if err := f.GetChildWorkflowExecution().Get(ctx, &execution); err != nil {
		if mode == "start-failure" {
			var started *temporal.ChildWorkflowExecutionAlreadyStartedError
			if !errors.As(err, &started) {
				return 0, errors.New("child initiation error identity lost")
			}
			if f.Get(ctx, nil) == nil || f.SignalChildWorkflow(ctx, "unused", nil).Get(ctx, nil) == nil {
				return 0, errors.New("start error not propagated")
			}
			return 1, nil
		}
		return 0, err
	}
	if execution.ID == "" || execution.RunID == "" {
		return 0, errors.New("empty child execution")
	}
	if mode == "signal" {
		if err := f.SignalChildWorkflow(ctx, "go", []byte("go")).Get(ctx, nil); err != nil {
			return 0, err
		}
	}
	if mode == "cancel" || mode == "cancel-wait" {
		cancel()
	}
	if mode == "cancel" || mode == "cancel-before-start" || mode == "cancel-wait" {
		err := f.Get(ctx, nil)
		if !errors.Is(err, context.Canceled) || !temporal.IsCanceledError(err) {
			return 0, fmt.Errorf("child cancellation: %w", err)
		}
		return 1, nil
	}
	if mode == "failure" || mode == "retry-failure" {
		err := f.Get(ctx, nil)
		var childError *temporal.ChildWorkflowExecutionError
		var app *temporal.ApplicationError
		if !errors.As(err, &childError) || !errors.As(err, &app) || app.Type() != "ChildFailure" {
			return 0, fmt.Errorf("structured child failure lost: %w", err)
		}
		if app.NonRetryable() != (mode == "failure") {
			return 0, errors.New("child retry metadata lost")
		}
		var detail int64
		if err := app.Details(&detail); err != nil || detail != 42 {
			return 0, errors.New("child error detail lost")
		}
		return detail, nil
	}
	var value int64
	if err := f.Get(ctx, &value); err != nil {
		return 0, err
	}
	var again int64
	if err := f.Get(ctx, &again); err != nil || again != value {
		return 0, errors.New("child Get not repeatable")
	}
	outcome := <-f.ToChannel()
	if outcome.Err != nil || outcome.Value == nil {
		return 0, errors.New("child channel failed")
	}
	if err := outcome.Value.Get(&again); err != nil || again != value {
		return 0, errors.New("child channel value lost")
	}
	return value, nil
}
