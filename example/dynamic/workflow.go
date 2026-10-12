package dynamic

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/mfateev/sdk-go-poc/workflow"
	"go.temporal.io/sdk/converter"
)

//go:isolate
func Workflow(ctx context.Context, args converter.EncodedValues) (string, error) {
	name := workflow.GetInfo(ctx).WorkflowType.Name
	if name == "empty" {
		if args.HasValues() {
			return "", errors.New("unexpected arguments")
		}
		return "empty", nil
	}
	var first, second string
	if err := args.Get(&first, &second); err != nil {
		return "", err
	}
	switch name {
	case "dynamic-activity":
		ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{StartToCloseTimeout: time.Minute})
		var result string
		err := workflow.ExecuteActivity(ctx, "random-activity-name", first, second).Get(ctx, &result)
		return result, err
	case "parent":
		var result string
		err := workflow.ExecuteChildWorkflow(ctx, "child-dynamic", first, second).Get(ctx, &result)
		return result, err
	default:
		return fmt.Sprintf("%s - %s - %s", name, first, second), nil
	}
}

//go:isolate
func Handlers(ctx context.Context, args converter.EncodedValues) (int64, error) {
	var mode string
	if err := args.Get(&mode); err != nil {
		return 0, err
	}
	value := int64(9007199254740993)

	if err := workflow.SetDynamicQueryHandler(ctx, func(name string, args converter.EncodedValues) (string, error) {
		if name == "mutate" {
			value++
			return "", nil
		}
		if name == "panic" {
			panic("query panic")
		}
		if !args.HasValues() {
			return fmt.Sprintf("%s:%d", name, value), nil
		}
		var prefix string
		if err := args.Get(&prefix); err != nil {
			return "", err
		}
		return fmt.Sprintf("%s:%s:%d", name, prefix, value), nil
	}, workflow.QueryHandlerOptions{Description: "dynamic queries"}); err != nil {
		return 0, err
	}
	if err := workflow.SetQueryHandler(ctx, "named", func() (string, error) { return "named", nil }); err != nil {
		return 0, err
	}
	if err := workflow.SetDynamicUpdateHandler(ctx, func(ctx context.Context, name string, args converter.EncodedValues) (int64, error) {
		info := workflow.GetCurrentUpdateInfo(ctx)
		if info == nil || info.Name != name || info.ID == "" || workflow.IsReadOnly(ctx) {
			return 0, errors.New("invalid update context")
		}
		var delta int64
		if err := args.Get(&delta); err != nil {
			return 0, err
		}
		value += delta
		return value, nil
	}, workflow.UpdateHandlerOptions{Description: "dynamic updates", Validator: func(ctx context.Context, name string, args converter.EncodedValues) error {
		if !workflow.IsReadOnly(ctx) || workflow.AllHandlersFinished(ctx) || workflow.GetCurrentUpdateInfo(ctx).Name != name {
			return errors.New("invalid validator context")
		}
		if name == "mutate" {
			value++
		}
		if name == "panic" {
			panic("validator panic")
		}
		var delta int64
		if err := args.Get(&delta); err != nil {
			return err
		}
		if delta < 0 {
			return errors.New("negative delta")
		}
		return nil
	}}); err != nil {
		return 0, err
	}
	if err := workflow.SetUpdateHandler(ctx, "named", func(context.Context) (string, error) { return "named", nil }); err != nil {
		return 0, err
	}
	if mode == "complete" {
		return value, nil
	}
	_, err := workflow.NextSignal(ctx)
	if err != nil {
		return 0, err
	}
	if err := workflow.WaitHandlersFinished(ctx); err != nil {
		return 0, err
	}
	return value, nil
}

//go:isolate
func Named(_ context.Context, input string) (string, error) { return "child:" + input, nil }

//go:isolate
func Replacement(ctx context.Context) (string, error) {
	last := "none"
	query := func(string, converter.EncodedValues) (string, error) { return "old", nil }
	if err := workflow.SetDynamicQueryHandler(ctx, query, workflow.QueryHandlerOptions{}); err != nil {
		return "", err
	}
	if err := workflow.SetDynamicQueryHandler(ctx, func(name string, args converter.EncodedValues) (string, error) {
		return name + ":" + last, nil
	}, workflow.QueryHandlerOptions{}); err != nil {
		return "", err
	}
	if err := workflow.SetQueryHandler(ctx, "", func() (string, error) { return "empty named query", nil }); err != nil {
		return "", err
	}
	if err := workflow.SetDynamicUpdateHandler(ctx, func(context.Context, string, converter.EncodedValues) (string, error) { return "old", nil }, workflow.UpdateHandlerOptions{}); err != nil {
		return "", err
	}
	if err := workflow.SetDynamicUpdateHandler(ctx, func(_ context.Context, name string, args converter.EncodedValues) error {
		if args.HasValues() {
			return fmt.Errorf("expected zero arguments")
		}
		last = name
		return nil
	}, workflow.UpdateHandlerOptions{Validator: func(name string, args converter.EncodedValues) error {
		if name == "reject" {
			return fmt.Errorf("rejected by replacement")
		}
		return nil
	}}); err != nil {
		return "", err
	}
	if _, err := workflow.NextSignal(ctx); err != nil {
		return "", err
	}
	if err := workflow.WaitHandlersFinished(ctx); err != nil {
		return "", err
	}
	return last, nil
}
