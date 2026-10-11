package update

import (
	"context"
	"errors"
	"runtime"
	"time"

	"github.com/mfateev/sdk-go-poc/example/query"
	"github.com/mfateev/sdk-go-poc/workflow"
	"go.temporal.io/sdk/temporal"
)

//go:isolate
func UpdateWorkflow(ctx context.Context, initial int64) (int64, error) {
	state := &query.State{Count: int(initial), Bytes: []byte{7}, Values: map[string]int{"count": int(initial)}}
	state.Atomic.Store(initial)
	if err := workflow.SetQueryHandler(ctx, "state", func() (int64, error) { return int64(state.Count), nil }); err != nil {
		return 0, err
	}
	if err := workflow.SetUpdateHandlerWithOptions(ctx, "bad", func(context.Context, string) error {
		panic("rejected handler must never execute")
	}, workflow.UpdateHandlerOptions{Validator: func(ctx context.Context, mode string) error {
		_, err := query.BadHandler(ctx, state, mode)
		return err
	}}); err != nil {
		return 0, err
	}
	if err := workflow.SetUpdateHandlerWithOptions(ctx, "add", func(ctx context.Context, value int64) (int64, error) {
		if workflow.IsReadOnly(ctx) || workflow.GetCurrentUpdateInfo(ctx) == nil || workflow.AllHandlersFinished(ctx) {
			return 0, errors.New("handler metadata missing")
		}
		state.Count += int(value)
		return int64(state.Count), nil
	}, workflow.UpdateHandlerOptions{Description: "increment state", Validator: func(ctx context.Context, value int64) error {
		if !workflow.IsReadOnly(ctx) || workflow.GetCurrentUpdateInfo(ctx).Name != "add" || workflow.AllHandlersFinished(ctx) {
			return errors.New("validator metadata missing")
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if value < 0 {
			return temporal.NewApplicationError("negative increment", "Validation", value)
		}
		return nil
	}}); err != nil {
		return 0, err
	}
	if err := workflow.SetUpdateHandler(ctx, "delayed", func(ctx context.Context, value int64) (int64, error) {
		if err := workflow.Sleep(ctx, time.Second); err != nil {
			return 0, err
		}
		state.Count += int(value)
		return int64(state.Count), nil
	}); err != nil {
		return 0, err
	}
	if err := workflow.SetUpdateHandlerWithOptions(ctx, "error-only", func(ctx context.Context, value int64) error {
		state.Count += int(value)
		return nil
	}, workflow.UpdateHandlerOptions{Validator: func(value int64) error {
		if value < 0 {
			return errors.New("negative")
		}
		return nil
	}}); err != nil {
		return 0, err
	}
	if err := workflow.SetUpdateHandler(ctx, "failure", func(context.Context, int64) error {
		return temporal.NewApplicationError("update failed", "HandlerFailure", "details")
	}); err != nil {
		return 0, err
	}
	if err := workflow.SetUpdateHandler(ctx, "cancellation", func(ctx context.Context) error {
		<-ctx.Done()
		return ctx.Err()
	}); err != nil {
		return 0, err
	}
	if err := workflow.SetQueryHandler(ctx, "finished", func() (bool, error) { return workflow.AllHandlersFinished(ctx), nil }); err != nil {
		return 0, err
	}
	// Keep the root alive after cancellation so the update can publish its own
	// canceled result and a subsequent query can inspect the original state.
	<-workflow.GetSignalChannel[[]byte](context.WithoutCancel(ctx), "finish")
	if err := workflow.WaitHandlersFinished(context.WithoutCancel(ctx)); err != nil {
		return 0, err
	}
	return int64(state.Count), nil
}

//go:isolate
func WaitWorkflow(ctx context.Context) (int64, error) {
	var count int64
	if err := workflow.SetUpdateHandler(ctx, "delayed", func(ctx context.Context, value int64) (int64, error) {
		if err := workflow.Sleep(ctx, time.Second); err != nil {
			return 0, err
		}
		count += value
		return count, nil
	}); err != nil {
		return 0, err
	}
	select {
	case <-ctx.Done():
		return 0, ctx.Err()
	case <-workflow.GetSignalChannel[any](ctx, "finish"):
	}
	if err := workflow.WaitHandlersFinished(ctx); err != nil {
		return 0, err
	}
	return count, nil
}

//go:isolate
func OrderingWorkflow(ctx context.Context, mode string) ([]string, error) {
	var events []string
	wake := make(chan struct{})
	var shared workflow.Future
	if mode == "shared-future" {
		activityCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{StartToCloseTimeout: time.Second})
		shared = workflow.ExecuteActivity(activityCtx, "ordering")
	}
	if err := workflow.SetUpdateHandlerWithOptions(ctx, "record", func(ctx context.Context, value string) ([]string, error) {
		events = append(events, value)
		switch mode {
		case "handler-timer":
			if err := workflow.Sleep(ctx, time.Second); err != nil {
				return nil, err
			}
			events = append(events, "resumed")
		case "native-timer":
			timer := time.NewTimer(time.Second)
			<-timer.C
			events = append(events, "resumed")
		case "handler-activity", "mixed-wakeup", "mixed-channel", "shared-future":
			activityCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{StartToCloseTimeout: time.Second})
			future := shared
			if future == nil {
				future = workflow.ExecuteActivity(activityCtx, "ordering")
			}
			var err error
			if mode == "mixed-channel" {
				err = (<-future.ToChannel()).Err
			} else {
				err = future.Get(ctx, nil)
			}
			if err != nil {
				return nil, err
			}
			events = append(events, "resumed")
		case "child":
			done := make(chan struct{})
			go func() { events = append(events, "child"); close(done) }()
			<-done
			events = append(events, "resumed")
		case "channel":
			<-wake
			events = append(events, "resumed")
		}
		return append([]string(nil), events...), nil
	}, workflow.UpdateHandlerOptions{Validator: func(value string) error {
		if mode == "validators" && (value == "first" && len(events) != 0 || value == "second" && len(events) != 1) {
			return errors.New("validator did not observe the preceding handler")
		}
		return nil
	}}); err != nil {
		return nil, err
	}
	if mode == "main-timer" || mode == "mixed-wakeup" || mode == "mixed-channel" || mode == "shared-future" || mode == "native-timer" {
		if err := workflow.Sleep(ctx, time.Second); err != nil {
			return nil, err
		}
	} else if mode == "main-activity" {
		activityCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{StartToCloseTimeout: time.Second})
		if err := workflow.ExecuteActivity(activityCtx, "ordering").Get(ctx, nil); err != nil {
			return nil, err
		}
	} else if mode != "registration" && mode != "validators" {
		<-workflow.GetSignalChannel[bool](ctx, "finish")
	}
	if mode == "channel" {
		go func() { events = append(events, "helper") }()
		wake <- struct{}{}
		events = append(events, "main-before-yield")
		runtime.Gosched()
		events = append(events, "main-after-yield")
	} else {
		events = append(events, "main")
	}
	if err := workflow.WaitHandlersFinished(ctx); err != nil {
		return nil, err
	}
	return events, nil
}

//go:isolate
func CancellationOrderingWorkflow(ctx context.Context) (bool, error) {
	var sawCancellation bool
	if err := workflow.SetUpdateHandler(ctx, "check-cancellation", func(ctx context.Context) (bool, error) {
		sawCancellation = ctx.Err() != nil
		return sawCancellation, nil
	}); err != nil {
		return false, err
	}
	detached := context.WithoutCancel(ctx)
	<-workflow.GetSignalChannel[bool](detached, "finish")
	if err := workflow.WaitHandlersFinished(detached); err != nil {
		return false, err
	}
	return sawCancellation, nil
}
