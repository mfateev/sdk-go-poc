package failureconversion

import (
	"context"
	"errors"
	"time"

	contextsupport "github.com/mfateev/sdk-go-poc/example/contextpropagation/support"
	"github.com/mfateev/sdk-go-poc/example/failureconversion/support"
	"github.com/mfateev/sdk-go-poc/workflow"
	"go.temporal.io/sdk/temporal"
)

//go:isolate
func Child(context.Context) error { return support.CustomError{Detail: support.Precise} }

//go:isolate
func Workflow(ctx context.Context, scenario string) error {
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{StartToCloseTimeout: time.Hour,
		RetryPolicy: &workflow.RetryPolicy{MaximumAttempts: 3}})
	var err error
	switch scenario {
	case "workflow":
		err = support.CustomError{Detail: support.Precise}
	case "local":
		local := workflow.WithLocalActivityOptions(ctx, workflow.LocalActivityOptions{StartToCloseTimeout: time.Minute,
			RetryPolicy: &workflow.RetryPolicy{MaximumAttempts: 3}})
		err = workflow.ExecuteLocalActivity(local, "Fail").Get(ctx, nil)
	case "child":
		err = workflow.ExecuteChildWorkflow(ctx, "Child").Get(ctx, nil)
	case "updates", "updates-context":
		if scenario == "updates-context" {
			ctx = workflow.WithDataConverter(ctx, contextsupport.PerContextConverter())
		}
		if err := workflow.SetUpdateHandlerWithOptions(ctx, "fail", func(context.Context, bool) error {
			return support.CustomError{Detail: support.Precise}
		}, workflow.UpdateHandlerOptions{Validator: func(reject bool) error {
			if reject {
				return support.CustomError{Detail: support.Precise}
			}
			return nil
		}}); err != nil {
			return err
		}
		if err := workflow.SetQueryHandler(ctx, "last", func() (int64, error) {
			var custom support.CustomError
			if err := workflow.GetLastError(ctx); !errors.As(err, &custom) {
				return 0, errors.New("previous-run custom error lost")
			}
			return custom.Detail, nil
		}); err != nil {
			return err
		}
		<-time.After(time.Second)
		return nil
	case "fallback":
		err = temporal.NewNonRetryableApplicationError("ordinary error", "Ordinary", nil, support.Precise)
		return err
	default:
		err = workflow.ExecuteActivity(ctx, "Fail").Get(ctx, nil)
	}
	var custom support.CustomError
	if !errors.As(err, &custom) || custom.Detail != support.Precise {
		return errors.New("native custom error type or detail lost")
	}
	return err
}
