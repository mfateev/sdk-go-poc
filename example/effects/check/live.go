package main

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/mfateev/sdk-go-poc/example/effects"
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
	queue := fmt.Sprintf("isolate-effects-check-%d", time.Now().UnixNano())
	w := worker.New(c, queue, worker.Options{})
	w.RegisterWorkflow(effects.EffectsWorkflow)
	if err := w.Start(); err != nil {
		return err
	}
	defer w.Stop()
	for _, mode := range []string{"log", "deny", "metadata"} {
		if err := checkLiveWorkflow(c, queue, mode); err != nil {
			return err
		}
	}
	fmt.Println("live server: logging completed; file and metadata violations failed tasks only")
	return nil
}

func checkLiveWorkflow(c client.Client, queue, mode string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	run, err := c.ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		ID: queue + "-" + mode, TaskQueue: queue,
		WorkflowTaskTimeout: 10 * time.Second,
	}, "EffectsWorkflow", mode)
	if err != nil {
		return err
	}
	if mode == "log" {
		var result string
		if err := run.Get(ctx, &result); err != nil {
			return err
		}
		if result != "logged" {
			return fmt.Errorf("live logging result: %q", result)
		}
		return nil
	}
	defer func() {
		cleanup, done := context.WithTimeout(context.Background(), 5*time.Second)
		defer done()
		_ = c.TerminateWorkflow(cleanup, run.GetID(), run.GetRunID(), "effect-check cleanup")
	}()
	operation, function := "os.WriteFile", "effects.writeFile"
	if mode == "metadata" {
		operation, function = "unaudited metadata operation", "effects.mutateMetadata"
	}
	for {
		history := c.GetWorkflowHistory(ctx, run.GetID(), run.GetRunID(), false, enums.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)
		failedTask := false
		for history.HasNext() {
			event, err := history.Next()
			if err != nil {
				return err
			}
			switch event.EventType {
			case enums.EVENT_TYPE_WORKFLOW_EXECUTION_FAILED, enums.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED:
				return fmt.Errorf("violation closed execution: %s", event.EventType)
			case enums.EVENT_TYPE_WORKFLOW_TASK_FAILED:
				failure := event.GetWorkflowTaskFailedEventAttributes().GetFailure()
				if failure == nil || !strings.Contains(failure.Message, operation) || !strings.Contains(failure.Message, function) {
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
