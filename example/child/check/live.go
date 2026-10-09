package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/mfateev/sdk-go-poc/example/child"
	"github.com/mfateev/sdk-go-poc/worker"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/sdk/client"
	goWorkflow "go.temporal.io/sdk/workflow"
	"google.golang.org/protobuf/encoding/protojson"
)

func register(w interface {
	RegisterWorkflowWithOptions(any, goWorkflow.RegisterOptions)
}) {
	w.RegisterWorkflowWithOptions(child.Parent, goWorkflow.RegisterOptions{Name: "ParentAlias"})
	w.RegisterWorkflowWithOptions(child.Child, goWorkflow.RegisterOptions{Name: "ChildAlias"})
	w.RegisterWorkflowWithOptions(child.OrdinaryChild, goWorkflow.RegisterOptions{Name: "OrdinaryAlias"})
}

func expected(mode string) int64 {
	if mode == "failure" || mode == "retry-failure" {
		return 42
	}
	if strings.HasPrefix(mode, "cancel") || mode == "already-canceled" || mode == "duplicate" {
		return 1
	}
	return 9007199254740993
}

func replayHistory(history *historypb.History, mode string) error {
	replayer := worker.NewWorkflowReplayer()
	register(replayer)
	if err := replayer.ReplayWorkflowHistory(nil, history); err != nil {
		return err
	}
	var actual int64
	getter, ok := replayer.(interface{ GetWorkflowResult(string, any) error })
	if !ok {
		return fmt.Errorf("replayer has no result accessor")
	}
	if err := getter.GetWorkflowResult("", &actual); err != nil {
		return err
	}
	if actual != expected(mode) {
		return fmt.Errorf("replayed %s result=%d", mode, actual)
	}
	return nil
}

func replayHistories(dir string) error {
	files, err := filepath.Glob(filepath.Join(dir, "*.json"))
	if err != nil {
		return err
	}
	if len(files) == 0 {
		return fmt.Errorf("no child histories in %s", dir)
	}
	for _, file := range files {
		raw, err := os.ReadFile(file)
		if err != nil {
			return err
		}
		history := new(historypb.History)
		if err := protojson.Unmarshal(raw, history); err != nil {
			return err
		}
		mode := strings.TrimSuffix(filepath.Base(file), ".json")
		if err := replayHistory(history, mode); err != nil {
			return fmt.Errorf("%s: %w", file, err)
		}
	}
	fmt.Println("saved child start/signal/cancellation/failure histories and results replayed")
	return nil
}

func checkLive(address string) error {
	c, err := client.Dial(client.Options{HostPort: address})
	if err != nil {
		return err
	}
	defer c.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	queue := fmt.Sprintf("isolate-child-%d", time.Now().UnixNano())
	w := worker.New(c, queue, worker.Options{})
	register(w)
	if err := w.Start(); err != nil {
		return err
	}
	defer w.Stop()
	if err := os.MkdirAll("/tmp/feature7-child-histories", 0755); err != nil {
		return err
	}
	for _, mode := range []string{"success", "ordinary", "name", "signal", "failure", "retry-failure", "child-continue", "cancel", "cancel-before-start", "cancel-wait", "already-canceled", "duplicate"} {
		run, err := c.ExecuteWorkflow(ctx, client.StartWorkflowOptions{ID: queue + "-" + mode, TaskQueue: queue}, "ParentAlias", mode)
		if err != nil {
			return err
		}
		var value int64
		if err := run.Get(ctx, &value); err != nil {
			return fmt.Errorf("live %s: %w", mode, err)
		}
		if value != expected(mode) {
			return fmt.Errorf("live %s result=%d", mode, value)
		}
		history := new(historypb.History)
		it := c.GetWorkflowHistory(ctx, run.GetID(), run.GetRunID(), false, enumspb.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)
		for it.HasNext() {
			event, err := it.Next()
			if err != nil {
				return err
			}
			history.Events = append(history.Events, event)
		}
		if mode == "retry-failure" {
			childID := ""
			for _, event := range history.Events {
				if start := event.GetChildWorkflowExecutionStartedEventAttributes(); start != nil {
					childID = start.WorkflowExecution.GetWorkflowId()
				}
			}
			if childID == "" {
				return fmt.Errorf("retry child has no initiation event")
			}
			childHistory := c.GetWorkflowHistory(ctx, childID, "", false, enumspb.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)
			attempt := int32(0)
			for childHistory.HasNext() {
				event, err := childHistory.Next()
				if err != nil {
					return err
				}
				if start := event.GetWorkflowExecutionStartedEventAttributes(); start != nil {
					attempt = start.Attempt
				}
			}
			if attempt != 2 {
				return fmt.Errorf("child retry attempt=%d, want 2", attempt)
			}
		}
		raw, err := (protojson.MarshalOptions{Indent: "  "}).Marshal(history)
		if err != nil {
			return err
		}
		if err := os.WriteFile(filepath.Join("/tmp/feature7-child-histories", mode+".json"), raw, 0644); err != nil {
			return err
		}
		if err := replayHistory(history, mode); err != nil {
			return fmt.Errorf("live replay %s: %w", mode, err)
		}
	}
	fmt.Println("live isolate and ordinary SDK children, signals, failures, cancellation and replay passed")
	return nil
}
