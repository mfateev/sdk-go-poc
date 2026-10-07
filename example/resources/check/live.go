package main

import (
	"context"
	"fmt"
	"isolate"
	"strings"
	"time"

	"github.com/mfateev/sdk-go-poc/example/resources"
	"github.com/mfateev/sdk-go-poc/worker"
	"go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/client"
)

// Opt-in validation checks actual server events, in addition to the compiled
// bindings harness. It creates uniquely named workflows on a development server.
func checkLiveServer(address string) error {
	c, err := client.Dial(client.Options{HostPort: address})
	if err != nil {
		return err
	}
	defer c.Close()
	queue := fmt.Sprintf("isolate-resources-check-%d", time.Now().UnixNano())
	w := worker.New(c, queue, worker.Options{})
	w.RegisterWorkflow(resources.ResourceWorkflow)
	if err := w.Start(); err != nil {
		return err
	}
	defer w.Stop()
	for _, mode := range []string{"normal", "goroutines", "memory", "yield", "busy"} {
		options := worker.ResourceOptions{}
		want := ""
		switch mode {
		case "goroutines":
			options.Limits = isolate.ResourceLimits{MaxGoroutines: 2}
			want = "goroutines"
		case "memory":
			options.Limits = isolate.ResourceLimits{MaxMemoryBytes: 2 << 20}
			want = "memory"
		case "yield":
			options.MaxTaskDuration = 100 * time.Millisecond
			want = "task duration"
		case "busy":
			options.MaxNoProgressDuration = 30 * time.Millisecond
			want = "no progress"
		}
		if err := worker.SetIsolateResourceOptions(w, options); err != nil {
			return err
		}
		if err := checkLiveWorkflow(c, queue, mode, want); err != nil {
			return err
		}
	}
	fmt.Println("live server: resource limits and watchdogs failed tasks only; unlimited workflow completed")
	return nil
}

func checkLiveWorkflow(c client.Client, queue, mode, want string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	run, err := c.ExecuteWorkflow(ctx, client.StartWorkflowOptions{ID: queue + "-" + mode, TaskQueue: queue, WorkflowTaskTimeout: 10 * time.Second}, "ResourceWorkflow", resources.Input{Mode: mode, Iterations: 500000000})
	if err != nil {
		return err
	}
	if want == "" {
		var result uint64
		if err := run.Get(ctx, &result); err != nil {
			return err
		}
		if result != 7 {
			return fmt.Errorf("unlimited workflow result=%d", result)
		}
		return nil
	}
	defer func() {
		cleanup, done := context.WithTimeout(context.Background(), 5*time.Second)
		defer done()
		_ = c.TerminateWorkflow(cleanup, run.GetID(), run.GetRunID(), "resource-check cleanup")
	}()
	for {
		history := c.GetWorkflowHistory(ctx, run.GetID(), run.GetRunID(), false, enums.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)
		failedTask := false
		for history.HasNext() {
			event, err := history.Next()
			if err != nil {
				return err
			}
			switch event.EventType {
			case enums.EVENT_TYPE_WORKFLOW_EXECUTION_FAILED, enums.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED, enums.EVENT_TYPE_WORKFLOW_EXECUTION_CANCELED:
				return fmt.Errorf("resource %s closed execution: %s", mode, event.EventType)
			case enums.EVENT_TYPE_WORKFLOW_TASK_FAILED:
				failure := event.GetWorkflowTaskFailedEventAttributes().GetFailure()
				if failure == nil || !strings.Contains(failure.Message, "isolate: "+want+" limit exceeded") {
					return fmt.Errorf("resource %s lost limit diagnostic: %v", mode, failure)
				}
				failedTask = true
			}
		}
		if failedTask {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(50 * time.Millisecond):
		}
	}
}
