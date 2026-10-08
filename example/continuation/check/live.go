package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/mfateev/sdk-go-poc/example/continuation"
	"github.com/mfateev/sdk-go-poc/worker"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/sdk/client"
	sdkwf "go.temporal.io/sdk/workflow"
	"google.golang.org/protobuf/encoding/protojson"
)

func checkLive(address, dir string) error {
	c, err := client.Dial(client.Options{HostPort: address})
	if err != nil {
		return err
	}
	defer c.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	queue := fmt.Sprintf("isolate-continuation-%d", time.Now().UnixNano())
	w := worker.New(c, queue, worker.Options{})
	w.RegisterWorkflowWithOptions(continuation.ContinueWorkflow, sdkwf.RegisterOptions{Name: "ContinuedAlias"})
	w.RegisterWorkflowWithOptions(continuation.LegacyVersionWorkflow, sdkwf.RegisterOptions{Name: "VersionAlias"})
	if err := w.Start(); err != nil {
		return err
	}
	for _, name := range []string{"continued", "legacy"} {
		workflowName := "ContinuedAlias"
		args := []any{3}
		if name == "legacy" {
			workflowName = "VersionAlias"
			args = nil
		}
		run, err := c.ExecuteWorkflow(ctx, client.StartWorkflowOptions{ID: queue + "-" + name, TaskQueue: queue}, workflowName, args...)
		if err != nil {
			w.Stop()
			return err
		}
		// WorkflowRun.Get follows continuations and advances GetRunID to the
		// terminal run. Keep the first run ID to check the entire chain.
		runID := run.GetRunID()
		var result int
		if err := run.Get(ctx, &result); err != nil {
			w.Stop()
			return err
		}
		if name == "legacy" && result != 0 || name == "continued" && result != 1 {
			w.Stop()
			return fmt.Errorf("%s live result: %d", name, result)
		}
		count := 0
		for {
			h, err := readHistory(ctx, c, run.GetID(), runID)
			if err != nil {
				w.Stop()
				return err
			}
			last := h.Events[len(h.Events)-1]
			next := last.GetWorkflowExecutionContinuedAsNewEventAttributes()
			filename := name
			if name == "continued" && next == nil {
				filename = "final"
			}
			if count == 0 || next == nil {
				if err := saveHistory(dir, filename, h); err != nil {
					w.Stop()
					return err
				}
			}
			count++
			if next == nil {
				break
			}
			runID = next.NewExecutionRunId
		}
		if name == "continued" && count != 4 {
			w.Stop()
			return fmt.Errorf("continuation chain has %d runs, want 4", count)
		}
	}
	w.Stop()
	// Same workflow name after deployment: legacy history has no version marker.
	w = worker.New(c, queue, worker.Options{})
	w.RegisterWorkflowWithOptions(continuation.VersionWorkflow, sdkwf.RegisterOptions{Name: "VersionAlias"})
	if err := w.Start(); err != nil {
		return err
	}
	defer w.Stop()
	run, err := c.ExecuteWorkflow(ctx, client.StartWorkflowOptions{ID: queue + "-version", TaskQueue: queue}, "VersionAlias")
	if err != nil {
		return err
	}
	var result int
	if err := run.Get(ctx, &result); err != nil {
		return err
	}
	if result != 1 {
		return fmt.Errorf("new version result: %d", result)
	}
	h, err := readHistory(ctx, c, run.GetID(), run.GetRunID())
	if err != nil {
		return err
	}
	markers := 0
	for _, event := range h.Events {
		if m := event.GetMarkerRecordedEventAttributes(); m != nil && m.MarkerName == "Version" {
			markers++
		}
	}
	if markers != 1 {
		return fmt.Errorf("GetVersion wrote %d version markers, want 1", markers)
	}
	if err := saveHistory(dir, "version", h); err != nil {
		return err
	}
	fmt.Println("live Temporal: four fresh runs, workflow alias and one version marker passed")
	return nil
}

func readHistory(ctx context.Context, c client.Client, id, run string) (*historypb.History, error) {
	h := new(historypb.History)
	i := c.GetWorkflowHistory(ctx, id, run, false, enumspb.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)
	for i.HasNext() {
		event, err := i.Next()
		if err != nil {
			return nil, err
		}
		h.Events = append(h.Events, event)
	}
	if len(h.Events) == 0 {
		return nil, fmt.Errorf("empty live history")
	}
	return h, nil
}

func saveHistory(dir, name string, h *historypb.History) error {
	if dir == "" {
		return nil
	}
	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}
	raw, err := protojson.MarshalOptions{Indent: "  "}.Marshal(h)
	if err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(dir, name+".json"), raw, 0644)
}
