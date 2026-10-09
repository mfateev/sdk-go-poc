package update

import (
	"context"
	"errors"
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
	<-workflow.GetSignalChannel(context.WithoutCancel(ctx), "finish")
	return int64(state.Count), nil
}
