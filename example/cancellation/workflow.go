package cancellation

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/mfateev/sdk-go-poc/workflow"
	"go.temporal.io/sdk/activity"
)

// WaitActivity runs only on the host and heartbeats so Temporal can deliver
// activity cancellation to its standard Go context.
func WaitActivity(ctx context.Context, input string) (string, error) {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		activity.RecordHeartbeat(ctx, input)
		select {
		case <-ctx.Done():
			activity.GetLogger(ctx).Info("activity observed context cancellation")
			return "", ctx.Err()
		case <-ticker.C:
		}
	}
}

//go:isolate
func ActivityWorkflow(ctx context.Context, input string) (string, error) {
	return workflow.ExecuteActivity(ctx, WaitActivity, time.Minute, input)
}

//go:isolate
func IdleWorkflow(ctx context.Context) (string, error) {
	<-ctx.Done()
	return "", ctx.Err()
}

//go:isolate
func DeadlineWorkflow(ctx context.Context) (string, error) {
	child, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	_, err := workflow.ExecuteActivity(child, WaitActivity, time.Minute, "deadline")
	if !errors.Is(err, context.DeadlineExceeded) || ctx.Err() != nil {
		return "", fmt.Errorf("incorrect child deadline: activity=%v parent=%v", err, ctx.Err())
	}
	return "deadline observed", nil
}

//go:isolate
func LocalCancelWorkflow(ctx context.Context) (string, error) {
	child, cancel := context.WithCancelCause(ctx)
	cause := errors.New("local cancellation cause")
	results := workflow.ExecuteActivityAsync(child, WaitActivity, time.Minute, "local")
	if err := workflow.Sleep(ctx, time.Second); err != nil {
		return "", err
	}
	cancel(cause)
	result := <-results
	if !errors.Is(result.Err, context.Canceled) || context.Cause(child) != cause || ctx.Err() != nil {
		return "", fmt.Errorf("incorrect local cancellation: result=%v parent=%v", result.Err, ctx.Err())
	}
	return "local cancellation observed", nil
}

// ContextStressWorkflow exercises the standard context child tree and callback
// order under the native dispatcher. Values/maps remain owned by this isolate.
//
//go:isolate
func ContextStressWorkflow(ctx context.Context) (int, error) {
	const count = 64
	parent, cancel := context.WithCancel(ctx)
	callbacks := make(chan int, count)
	for index := range count {
		child, cancelChild := context.WithCancel(parent)
		defer cancelChild()
		context.AfterFunc(child, func() { callbacks <- index })
	}
	cancel()
	for index := range count {
		if got := <-callbacks; got != index {
			return 0, fmt.Errorf("callback %d, want %d", got, index)
		}
	}
	return count, nil
}
