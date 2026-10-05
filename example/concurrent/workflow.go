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
	first := workflow.ExecuteActivityAsyncByName[[]byte](ctx, "echo", time.Minute, []byte("one"))
	second := workflow.ExecuteActivityAsyncByName[[]byte](ctx, "echo", time.Minute, []byte("two"))
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
			events = append(events, string(result.Result))
			first = nil
		case result := <-second:
			if result.Err != nil {
				return nil, result.Err
			}
			events = append(events, string(result.Result))
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
