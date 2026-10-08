// Package lifecycle exercises terminal outcomes and leftover native goroutines.
package lifecycle

import (
	"context"
	"errors"
	"os"
	"runtime"
	"time"

	"github.com/mfateev/sdk-go-poc/workflow"
)

//go:isolate
func LifecycleWorkflow(ctx context.Context, mode string) (string, error) {
	switch mode {
	case "panic":
		panic("root lifecycle panic")
	case "child":
		go func() { panic("child lifecycle panic") }()
		select {}
	case "goexit":
		runtime.Goexit()
	case "exit":
		os.Exit(17)
	case "cancel":
		<-ctx.Done()
		return "", ctx.Err()
	case "error":
		return "", errors.New("ordinary workflow error")
	case "evict":
		ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{StartToCloseTimeout: time.Minute})
		future := workflow.ExecuteActivity(ctx, "pending", []byte("input"))
		activity := make(chan struct{})
		go func() { _ = future.Get(ctx, nil); close(activity) }()
		signals := workflow.GetSignalChannel(ctx, "pending")
		timer := time.NewTimer(time.Hour)
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case <-activity:
		case <-signals:
		case <-timer.C:
		}
	case "recover":
		finished := make(chan struct{})
		go func() { defer close(finished); runtime.Goexit() }()
		<-finished
		func() {
			defer func() {
				if recover() != "ordinary" {
					panic("lost recovery")
				}
			}()
			panic("ordinary")
		}()
	case "children":
		entered := make(chan struct{})
		for range 32 {
			go func() { entered <- struct{}{}; select {} }()
		}
		for range 32 {
			<-entered
		}
	}
	return "finished", nil
}
