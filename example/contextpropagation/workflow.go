package contextpropagation

import (
	"context"
	"errors"
	"time"

	"github.com/mfateev/sdk-go-poc/example/contextpropagation/support"
	"github.com/mfateev/sdk-go-poc/workflow"
	"go.temporal.io/sdk/converter"
)

func requireValue(ctx context.Context, want string) error {
	if ctx.Value(support.HeaderKey) != want {
		return errors.New("context value did not propagate")
	}
	return nil
}

//temporal:workflow
func Workflow(ctx context.Context, scenario string) (string, error) {
	if err := requireValue(ctx, "test-data"); err != nil {
		return "", err
	}
	ctx = workflow.WithStartToCloseTimeout(ctx, time.Minute)
	switch scenario {
	case "mock-header":
		if err := workflow.ExecuteChildWorkflow(ctx, "MockChild").Get(ctx, nil); err != nil {
			return "", err
		}
	case "per-context-converter", "per-context-only":
		first := workflow.WithDataConverter(ctx, support.PerContextConverter())
		second := workflow.WithDataConverter(ctx, converter.NewCompositeDataConverter(converter.NewJSONPayloadConverter()))
		var a, b string
		f := workflow.ExecuteActivity(first, "Hello", "world")
		g := workflow.ExecuteActivity(second, "Hello", "world1")
		// A different waiting context must not change the scheduled serializer.
		if err := f.Get(second, &a); err != nil {
			return "", err
		}
		if err := g.Get(first, &b); err != nil {
			return "", err
		}
		if a != "mock_world" || b != "mock_world1" {
			return "", errors.New("per-context mock result")
		}
		localCtx := workflow.WithLocalActivityOptions(first, workflow.LocalActivityOptions{StartToCloseTimeout: time.Minute})
		if err := workflow.ExecuteLocalActivity(localCtx, "Hello", "world").Get(second, &a); err != nil {
			return "", err
		}
		if a != "mock_world" {
			return "", errors.New("per-context local result")
		}
		childCtx := first
		if scenario == "per-context-only" {
			// Without a child serializer factory, its default decoder needs JSON.
			childCtx = second
		}
		if err := workflow.ExecuteChildWorkflow(childCtx, "Child", "test-data").Get(second, &a); err != nil {
			return "", err
		}
		if a != "test-data" {
			return "", errors.New("per-context child result")
		}
	case "context-aware-converter":
		first := context.WithValue(ctx, support.MaskKey, "e")
		second := context.WithValue(ctx, support.MaskKey, "t")
		for _, call := range []struct {
			ctx  context.Context
			want string
		}{{first, "t?st"}, {second, "?es?"}, {ctx, "test"}} {
			var result string
			if err := workflow.ExecuteActivity(call.ctx, "Echo", "test").Get(ctx, &result); err != nil {
				return "", err
			}
			if result != call.want {
				return "", errors.New("context-aware serializer was not selected")
			}
		}
	case "local-header":
		ctx = workflow.WithLocalActivityOptions(ctx, workflow.LocalActivityOptions{StartToCloseTimeout: time.Minute})
		var value string
		if err := workflow.ExecuteLocalActivity(ctx, "ContextActivity").Get(ctx, &value); err != nil {
			return "", err
		}
		if value != "test-data" {
			return "", errors.New("local context value lost")
		}
	default:
		want := "test-data"
		childCtx := ctx
		if scenario == "child-derived" {
			want = "test-data-for-child"
			childCtx = context.WithValue(ctx, support.HeaderKey, want)
		}
		var value string
		if err := workflow.ExecuteChildWorkflow(childCtx, "Child", want).Get(ctx, &value); err != nil {
			return "", err
		}
		if value != want {
			return "", errors.New("child context value lost")
		}
		if err := requireValue(ctx, "test-data"); err != nil {
			return "", err
		}
		if err := workflow.ExecuteActivity(ctx, "ContextActivity").Get(ctx, &value); err != nil {
			return "", err
		}
		if value != "test-data" {
			return "", errors.New("activity context value lost")
		}
	}
	completed := workflow.ExecuteActivity(ctx, "Echo", "query-result")
	if err := completed.Get(ctx, nil); err != nil {
		return "", err
	}
	if err := workflow.SetQueryHandler(ctx, "future", func() (string, error) { var value string; err := completed.Get(ctx, &value); return value, err }); err != nil {
		return "", err
	}
	if err := workflow.SetQueryHandler(ctx, "value", func() (string, error) { return ctx.Value(support.HeaderKey).(string), nil }); err != nil {
		return "", err
	}
	if err := workflow.SetUpdateHandlerWithOptions(ctx, "value", func(updateCtx context.Context) (string, error) {
		return updateCtx.Value(support.HeaderKey).(string), nil
	}, workflow.UpdateHandlerOptions{Validator: func(updateCtx context.Context) error { return requireValue(updateCtx, "test-data") }}); err != nil {
		return "", err
	}
	customCtx := workflow.WithDataConverter(ctx, support.PerContextConverter())
	if err := workflow.SetQueryHandler(customCtx, "custom", func() (string, error) { return "query-custom", nil }); err != nil {
		return "", err
	}
	if err := workflow.SetUpdateHandler(customCtx, "custom", func(updateCtx context.Context) (string, error) {
		return updateCtx.Value(support.HeaderKey).(string), nil
	}); err != nil {
		return "", err
	}
	if err := workflow.SetQueryHandler(ctx, "ready", func() (bool, error) { return true, nil }); err != nil {
		return "", err
	}
	<-time.After(time.Second)
	return "ok", nil
}

//temporal:workflow
func Child(ctx context.Context, want string) (string, error) {
	if err := requireValue(ctx, want); err != nil {
		return "", err
	}
	return ctx.Value(support.HeaderKey).(string), nil
}
