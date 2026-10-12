package upstream

import (
	"context"
	"fmt"
	"time"

	"github.com/mfateev/sdk-go-poc/workflow"
)

// EnvironmentWorkflow exercises the existing host test environment with an
// isolate definition. No host test closures are captured by workflow code.
//
//go:isolate
func EnvironmentWorkflow(ctx context.Context, scenario string) (string, error) {
	ctx = workflow.WithStartToCloseTimeout(ctx, time.Minute)
	switch scenario {
	case "activity-listener":
		for i := 1; i <= 100; i++ {
			if err := workflow.ExecuteActivity(ctx, "Hello", fmt.Sprintf("msg%d", i)).Get(ctx, nil); err != nil {
				return "", err
			}
		}
		return "ok", nil
	case "timer-clock":
		start := time.Now()
		one, two, five := time.After(time.Second), time.After(2*time.Second), time.After(5*time.Second)
		<-one
		check(time.Since(start) == time.Second, "first timer clock")
		<-two
		check(time.Since(start) == 2*time.Second, "second timer clock")
		<-five
		check(time.Since(start) == 5*time.Second, "third timer clock")
		return "ok", nil
	case "clock-activity":
		start := time.Now()
		timer := time.After(time.Minute)
		activity := workflow.ExecuteActivity(ctx, "Hello", "controlled_execution")
		<-timer
		check(activity.IsReady(), "clock advanced while activity was running")
		check(time.Since(start) == time.Minute, "activity and timer virtual clock")
		var result string
		err := activity.Get(ctx, &result)
		return result, err
	case "query-nil":
		err := workflow.SetQueryHandler(ctx, "nil_response", func(input *string) (*string, error) {
			check(input == nil, "nil query input changed")
			return nil, nil
		})
		// Keep the isolate alive until the query is serviced; completed native
		// executions revoke their heap rather than retain query closures.
		if err == nil {
			<-time.After(time.Second)
		}
		return "ok", err
	case "version-mock":
		check(workflow.GetVersion(ctx, "change_1", workflow.DefaultVersion, 2) == workflow.DefaultVersion, "specific version mock")
		check(workflow.GetVersion(ctx, "change_2", workflow.DefaultVersion, 2) == 2, "fallback version mock")
		return "ok", nil
	case "child-completed-listener":
		var result string
		err := workflow.ExecuteChildWorkflow(ctx, "EnvironmentChild", "world").Get(ctx, &result)
		return result, err
	case "local-listener":
		ctx = workflow.WithLocalActivityOptions(ctx, workflow.LocalActivityOptions{StartToCloseTimeout: time.Minute})
		var result string
		err := workflow.ExecuteLocalActivity(ctx, "Hello", "world").Get(ctx, &result)
		return result, err
	default:
		var result string
		err := workflow.ExecuteActivity(ctx, "Hello", "world").Get(ctx, &result)
		return result, err
	}
}

//go:isolate
func EnvironmentChild(ctx context.Context, input string) (string, error) {
	ctx = workflow.WithStartToCloseTimeout(ctx, time.Minute)
	var result string
	err := workflow.ExecuteActivity(ctx, "Hello", input).Get(ctx, &result)
	return result, err
}
