package main

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/mfateev/sdk-go-poc/example/lifecycle"
	"github.com/mfateev/sdk-go-poc/worker"
	"go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/client"
)

// Opt-in service validation: the normal test uses only the recorded-history
// replayer. This path verifies the server's actual task failure event and keeps
// the development server's workflow IDs separate from application workloads.
func checkLiveServer(address string) error {
	c, err := client.Dial(client.Options{HostPort: address})
	if err != nil {
		return err
	}
	defer c.Close()
	queue := fmt.Sprintf("isolate-lifecycle-check-%d", time.Now().UnixNano())
	w := worker.New(c, queue, worker.Options{})
	w.RegisterWorkflow(lifecycle.LifecycleWorkflow)
	if err := w.Start(); err != nil {
		return err
	}
	defer w.Stop()
	for _, mode := range []string{"recover", "children", "panic", "child", "goexit", "exit", "error", "cancel"} {
		if err := checkLiveWorkflow(c, queue, mode); err != nil {
			return err
		}
	}
	fmt.Println("live server: normal completion, cancellation, application errors and lifecycle task failures passed")
	return nil
}

func checkLiveWorkflow(c client.Client, queue, mode string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	run, err := c.ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		ID: queue + "-" + mode, TaskQueue: queue,
		WorkflowTaskTimeout: 10 * time.Second,
	}, "LifecycleWorkflow", mode)
	if err != nil {
		return err
	}
	if mode == "recover" || mode == "children" {
		var result string
		if err := run.Get(ctx, &result); err != nil {
			return err
		}
		if result != "finished" {
			return fmt.Errorf("live lifecycle result: %q", result)
		}
		return nil
	}
	defer func() {
		cleanup, done := context.WithTimeout(context.Background(), 5*time.Second)
		defer done()
		_ = c.TerminateWorkflow(cleanup, run.GetID(), run.GetRunID(), "lifecycle-check cleanup")
	}()
	if mode == "cancel" {
		if err := c.CancelWorkflow(ctx, run.GetID(), run.GetRunID()); err != nil {
			return err
		}
	}
	operation := map[string]string{"panic": "root lifecycle panic", "child": "child lifecycle panic", "goexit": "main goroutine exited", "exit": "status 17", "error": "ordinary workflow error"}[mode]
	for {
		history := c.GetWorkflowHistory(ctx, run.GetID(), run.GetRunID(), false, enums.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)
		failedTask := false
		for history.HasNext() {
			event, err := history.Next()
			if err != nil {
				return err
			}
			switch event.EventType {
			case enums.EVENT_TYPE_WORKFLOW_EXECUTION_FAILED:
				if mode == "error" && strings.Contains(event.GetWorkflowExecutionFailedEventAttributes().GetFailure().GetMessage(), operation) {
					return nil
				}
				return fmt.Errorf("lifecycle closed execution: %s", event.EventType)
			case enums.EVENT_TYPE_WORKFLOW_EXECUTION_CANCELED:
				if mode == "cancel" {
					return nil
				}
				return fmt.Errorf("unexpected cancellation: %s", mode)
			case enums.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED:
				return fmt.Errorf("failure completed execution: %s", mode)
			case enums.EVENT_TYPE_WORKFLOW_TASK_FAILED:
				if mode == "error" || mode == "cancel" {
					return fmt.Errorf("ordinary outcome failed its task: %s", mode)
				}
				failure := event.GetWorkflowTaskFailedEventAttributes().GetFailure()
				if failure == nil || !strings.Contains(failure.Message, operation) {
					return fmt.Errorf("task failure lacks operation/stack: %v", failure)
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
