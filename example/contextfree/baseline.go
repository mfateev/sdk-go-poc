package contextfree

import (
	"fmt"
	"strings"
	"time"

	sdkworkflow "go.temporal.io/sdk/workflow"
)

// Baseline runs the same scenarios through the original SDK workflow API.
func Baseline(ctx sdkworkflow.Context, local bool) ([]int, error) {
	ctx = sdkworkflow.WithActivityOptions(ctx, sdkworkflow.ActivityOptions{StartToCloseTimeout: time.Second * 10})
	ctx = sdkworkflow.WithLocalActivityOptions(ctx, sdkworkflow.LocalActivityOptions{StartToCloseTimeout: time.Second * 10})
	execute := func(fn any, args ...any) sdkworkflow.Future {
		if local {
			return sdkworkflow.ExecuteLocalActivity(ctx, fn, args...)
		}
		return sdkworkflow.ExecuteActivity(ctx, fn, args...)
	}
	var results []int
	for _, call := range Calls() {
		var result int
		if err := execute(call.Fn, call.Args...).Get(ctx, &result); err != nil {
			return nil, err
		}
		results = append(results, result)
	}
	if err := execute(ErrorOnly).Get(ctx, nil); err != nil {
		return nil, err
	}
	if err := execute(Failure).Get(ctx, nil); err == nil || !strings.Contains(err.Error(), "context-free failure") {
		return nil, fmt.Errorf("unexpected failure result: %v", err)
	}

	return results, nil
}
