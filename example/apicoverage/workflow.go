// Package apicoverage checks the remaining SDK operations behind the byte boundary.
package apicoverage

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/mfateev/sdk-go-poc/workflow"
	"github.com/nexus-rpc/sdk-go/nexus"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/temporal"
	goWorkflow "go.temporal.io/sdk/workflow"
)

type Input struct{ Mode, Target, Endpoint string }

const Precise int64 = 9007199254740993

func Local(ctx context.Context, mode string, input int64) (int64, error) {
	if mode == "local-failure" {
		return 0, temporal.NewNonRetryableApplicationError("rejected", "LocalFailure", nil, input)
	}
	if mode == "local-retry" && activity.GetInfo(ctx).Attempt == 1 {
		return 0, temporal.NewApplicationError("retry", "LocalRetry")
	}
	if mode == "local-cancel" {
		<-ctx.Done()
		return 0, ctx.Err()
	}
	return input, nil
}
func Identity(ctx context.Context) (string, error) { return activity.GetInfo(ctx).TaskQueue, nil }

//go:isolate
func Coverage(ctx context.Context, input Input) (string, error) {
	switch input.Mode {
	case "external", "external-failure":
		err := workflow.SignalExternalWorkflow(ctx, input.Target, "", "precise", Precise).Get(ctx, nil)
		if input.Mode == "external-failure" {
			var missing *temporal.UnknownExternalWorkflowExecutionError
			if !errors.As(err, &missing) {
				return "", fmt.Errorf("external failure identity: %w", err)
			}
			err = workflow.RequestCancelExternalWorkflow(ctx, input.Target, "").Get(ctx, nil)
			if !errors.As(err, &missing) {
				return "", fmt.Errorf("external cancel failure identity: %w", err)
			}
			return "ok", nil
		}
		if err != nil {
			return "", err
		}
		if err = workflow.RequestCancelExternalWorkflow(ctx, input.Target, "").Get(ctx, nil); err != nil {
			return "", err
		}
	case "local", "local-retry", "local-failure", "local-cancel":
		localCtx, cancel := context.WithCancel(workflow.WithLocalActivityOptions(ctx, workflow.LocalActivityOptions{StartToCloseTimeout: 10 * time.Second, ScheduleToCloseTimeout: 20 * time.Second, RetryPolicy: &temporal.RetryPolicy{InitialInterval: 2 * time.Second, MaximumInterval: 2 * time.Second, MaximumAttempts: 2}}))
		defer cancel()
		f := workflow.ExecuteLocalActivity(localCtx, Local, input.Mode, Precise)
		if input.Mode == "local-cancel" {
			cancel()
		}
		var result int64
		err := f.Get(ctx, &result)
		if input.Mode == "local-cancel" {
			if err == nil {
				return "", errors.New("local cancellation lost")
			}
			return "ok", nil
		}
		if input.Mode == "local-failure" {
			var app *temporal.ApplicationError
			if !errors.As(err, &app) || app.Type() != "LocalFailure" {
				return "", fmt.Errorf("local error identity: %w", err)
			}
			if err = app.Details(&result); err != nil {
				return "", err
			}
		} else if err != nil {
			return "", err
		}
		if result != Precise {
			return "", fmt.Errorf("local precision lost: %d", result)
		}
		var again int64
		if input.Mode != "local-failure" {
			if err = f.Get(ctx, &again); err != nil || again != result {
				return "", errors.New("local future not repeatable")
			}
		}
	case "session", "session-recreate", "session-failure":
		ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{StartToCloseTimeout: 5 * time.Second})
		sessionCtx, err := workflow.CreateSession(ctx, &workflow.SessionOptions{CreationTimeout: 5 * time.Second, ExecutionTimeout: 20 * time.Second, HeartbeatTimeout: time.Second})
		if err != nil {
			return "", err
		}
		info := workflow.GetSessionInfo(sessionCtx)
		if info == nil || info.HostName == "" || info.SessionState != workflow.SessionStateOpen {
			return "", errors.New("session metadata lost")
		}
		if input.Mode == "session-failure" {
			if err = workflow.SetQueryHandler(ctx, "session-ready", func() (bool, error) { return true, nil }); err != nil {
				return "", err
			}
			<-sessionCtx.Done()
			if err = workflow.ExecuteActivity(sessionCtx, Identity).Get(ctx, nil); !errors.Is(err, workflow.ErrSessionFailed) {
				return "", fmt.Errorf("session failure: %w", err)
			}
			return "ok", nil
		}
		var queue string
		if err = workflow.ExecuteActivity(sessionCtx, Identity).Get(ctx, &queue); err != nil {
			return "", err
		}
		if queue == "" {
			return "", errors.New("session resource queue missing")
		}
		token := info.GetRecreateToken()
		workflow.CompleteSession(sessionCtx)
		workflow.CompleteSession(sessionCtx)
		if info.SessionState != workflow.SessionStateClosed {
			return "", errors.New("session did not close")
		}
		if input.Mode == "session-recreate" {
			recreated, err := workflow.RecreateSession(ctx, token, &workflow.SessionOptions{CreationTimeout: 5 * time.Second, ExecutionTimeout: 20 * time.Second})
			if err != nil {
				return "", err
			}
			var nextQueue string
			if err = workflow.ExecuteActivity(recreated, Identity).Get(ctx, &nextQueue); err != nil {
				return "", err
			}
			workflow.CompleteSession(recreated)
			if queue != nextQueue {
				return "", errors.New("recreated session changed worker")
			}
		}
	case "nexus", "nexus-async", "nexus-failure", "nexus-cancel", "nexus-abandon", "nexus-try-cancel", "nexus-wait-requested":
		nctx, cancel := context.WithCancel(ctx)
		defer cancel()
		options := workflow.NexusOperationOptions{ScheduleToCloseTimeout: 20 * time.Second}
		if input.Mode == "nexus-abandon" {
			options.CancellationType = workflow.NexusOperationCancellationTypeAbandon
		}
		if input.Mode == "nexus-try-cancel" {
			options.CancellationType = workflow.NexusOperationCancellationTypeTryCancel
		}
		if input.Mode == "nexus-wait-requested" {
			options.CancellationType = workflow.NexusOperationCancellationTypeWaitRequested
		}
		name := "echo"
		if input.Mode == "nexus-async" || input.Mode == "nexus-cancel" || input.Mode == "nexus-abandon" || input.Mode == "nexus-try-cancel" || input.Mode == "nexus-wait-requested" {
			name = "async"
		}
		if input.Mode == "nexus-failure" {
			name = "failure"
		}
		f := workflow.NewNexusClient(input.Endpoint, "coverage").ExecuteOperation(nctx, nexus.NewOperationReference[string, int64](name), input.Mode, options)
		var execution workflow.NexusOperationExecution
		err := f.GetNexusOperationExecution().Get(ctx, &execution)
		if input.Mode == "nexus-failure" {
			var ne *temporal.NexusOperationError
			if !errors.As(err, &ne) {
				return "", fmt.Errorf("Nexus error identity: %w", err)
			}
			return "ok", nil
		}
		if err != nil {
			return "", err
		}
		if name == "async" && execution.OperationToken == "" {
			return "", errors.New("Nexus token missing")
		}
		if input.Mode == "nexus-cancel" || input.Mode == "nexus-abandon" || input.Mode == "nexus-try-cancel" || input.Mode == "nexus-wait-requested" {
			cancel()
			if err = f.Get(ctx, nil); err == nil {
				return "", errors.New("Nexus cancellation lost")
			}
			return "ok", nil
		}
		var result int64
		if err = f.Get(ctx, &result); err != nil {
			return "", err
		}
		if result != Precise {
			return "", errors.New("Nexus result precision lost")
		}
	default:
		return "", fmt.Errorf("unknown coverage mode %s", input.Mode)
	}
	return "ok", nil
}

// Target checks interoperability with ordinary SDK workflows and signal types.
func Target(ctx goWorkflow.Context) error {
	var value int64
	goWorkflow.GetSignalChannel(ctx, "precise").Receive(ctx, &value)
	if value != Precise {
		return errors.New("external precision lost")
	}
	return goWorkflow.Await(ctx, func() bool { return ctx.Err() != nil })
}
func NexusTarget(ctx goWorkflow.Context, mode string) (int64, error) {
	if mode == "nexus-cancel" || mode == "nexus-abandon" || mode == "nexus-try-cancel" || mode == "nexus-wait-requested" {
		if err := goWorkflow.Sleep(ctx, time.Minute); err != nil {
			return 0, err
		}
	}
	return Precise, nil
}
