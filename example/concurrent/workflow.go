// Package concurrent exercises native activity result channels and timers.
package concurrent

import (
	"context"
	"runtime"
	"strings"
	"time"

	"github.com/mfateev/sdk-go-poc/workflow"
)

//go:isolate
func ConcurrentWorkflow(ctx context.Context) ([]byte, error) {
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{StartToCloseTimeout: time.Minute})
	first := workflow.ExecuteActivity(ctx, "echo", []byte("one")).ToChannel()
	second := workflow.ExecuteActivity(ctx, "echo", []byte("two")).ToChannel()
	timer := time.After(time.Second)
	var events []string
	for first != nil || second != nil || timer != nil {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case result := <-first:
			if result.Err != nil {
				return nil, result.Err
			}
			var value []byte
			if err := result.Value.Get(&value); err != nil {
				return nil, err
			}
			events = append(events, string(value))
			first = nil
		case result := <-second:
			if result.Err != nil {
				return nil, result.Err
			}
			var value []byte
			if err := result.Value.Get(&value); err != nil {
				return nil, err
			}
			events = append(events, string(value))
			second = nil
		case <-timer:
			events = append(events, "timer")
			timer = nil
		}
	}
	return []byte(strings.Join(events, "|")), nil
}

//go:isolate
func WaitForCancellationWorkflow(ctx context.Context) ([]byte, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}

//go:isolate
func YieldForeverWorkflow(ctx context.Context) ([]byte, error) {
	for {
		runtime.Gosched()
	}
}
