package converter

import (
	"context"
	"errors"
	"time"

	"github.com/mfateev/sdk-go-poc/workflow"
	"go.temporal.io/sdk/temporal"
)

var executions int

//go:isolate
func Workflow(ctx context.Context, input int64) (int64, error) {
	executions++
	if executions != 1 {
		return 0, errors.New("workflow package state leaked")
	}
	state := input
	if err := workflow.SetQueryHandler(ctx, "state", func() (int64, error) { return state, nil }); err != nil {
		return 0, err
	}
	if err := workflow.SetQueryHandler(ctx, "bad", func() (int64, error) { state = 99; return state, nil }); err != nil {
		return 0, err
	}
	if err := workflow.SetUpdateHandlerWithOptions(ctx, "add", func(_ context.Context, delta int64) (int64, error) { state += delta; return state, nil },
		workflow.UpdateHandlerOptions{Validator: func(delta int64) error {
			if delta < 0 {
				return temporal.NewNonRetryableApplicationError("negative delta", "InvalidDelta", nil, delta)
			}
			return nil
		}}); err != nil {
		return 0, err
	}
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{StartToCloseTimeout: time.Minute})
	var result int64
	if err := workflow.ExecuteActivity(ctx, "Echo", input).Get(ctx, &result); err != nil {
		return 0, err
	}
	if result != input {
		return 0, errors.New("activity argument precision lost")
	}
	var application *temporal.ApplicationError
	err := workflow.ExecuteActivity(ctx, "Fail", input).Get(ctx, nil)
	if !errors.As(err, &application) {
		return 0, errors.New("application error type lost")
	}
	var detail int64
	if err := application.Details(&detail); err != nil || detail != input {
		return 0, errors.New("application error details lost")
	}
	select {
	case <-time.After(time.Minute):
	case signal := <-workflow.GetSignalChannel(ctx, "finish"):
		if signal.Err != nil {
			return 0, signal.Err
		}
		if string(signal.Signal.Input) != "finish" {
			return 0, errors.New("signal data lost")
		}
	}
	return state, nil
}

//go:isolate
func Child(ctx context.Context, input int64) (int64, error) {
	if input < 0 {
		return 0, temporal.NewNonRetryableApplicationError("child rejected", "ChildError", nil, input)
	}
	signal := <-workflow.GetSignalChannel(ctx, "finish")
	if signal.Err != nil {
		return 0, signal.Err
	}
	return input, nil
}

//go:isolate
func Parent(ctx context.Context, input int64, continued bool) (int64, error) {
	if !continued {
		return 0, workflow.NewContinueAsNewError(ctx, Parent, input, true)
	}
	f := workflow.ExecuteChildWorkflow(ctx, Child, input)
	var execution workflow.Execution
	if err := f.GetChildWorkflowExecution().Get(ctx, &execution); err != nil {
		return 0, err
	}
	if execution.ID == "" {
		return 0, errors.New("child ID missing")
	}
	if err := f.SignalChildWorkflow(ctx, "finish", []byte("finish")).Get(ctx, nil); err != nil {
		return 0, err
	}
	var result int64
	if err := f.Get(ctx, &result); err != nil || result != input {
		return 0, errors.New("child result lost")
	}
	// Child failures must be decoded with the child's serialization context.
	f = workflow.ExecuteChildWorkflow(ctx, Child, -input)
	var application *temporal.ApplicationError
	if err := f.Get(ctx, nil); !errors.As(err, &application) {
		return 0, errors.New("child failure type lost")
	}
	var detail int64
	if err := application.Details(&detail); err != nil || detail != -input {
		return 0, errors.New("child failure details lost")
	}
	return result, nil
}
