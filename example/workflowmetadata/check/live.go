package main

import (
	"context"
	"fmt"
	"os"
	"time"

	metadata "github.com/mfateev/sdk-go-poc/example/workflowmetadata"
	"github.com/mfateev/sdk-go-poc/worker"
	historypb "go.temporal.io/api/history/v1"
	sdkpb "go.temporal.io/api/sdk/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
	"google.golang.org/protobuf/encoding/protojson"
)

func register(w interface{ RegisterWorkflow(any) }) {
	w.RegisterWorkflow(metadata.Workflow)
	must(worker.SetIsolateInterceptors(w, worker.InterceptorOptions{Factory: metadata.NewInterceptors}))
}
func replay(history *historypb.History) error {
	r := worker.NewWorkflowReplayer()
	register(r)
	if err := r.ReplayWorkflowHistory(nil, history); err != nil {
		return err
	}
	var result string
	if err := r.(interface{ GetWorkflowResult(string, any) error }).GetWorkflowResult("", &result); err != nil {
		return err
	}
	if result != "ok" {
		return fmt.Errorf("replay result: %q", result)
	}
	return nil
}
func replayFile(path string) error {
	raw, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	history := new(historypb.History)
	if err := protojson.Unmarshal(raw, history); err != nil {
		return err
	}
	return replay(history)
}
func liveCheck(address, output string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	c, err := client.Dial(client.Options{HostPort: address})
	if err != nil {
		return err
	}
	defer c.Close()
	queue := fmt.Sprintf("isolate-metadata-%d", time.Now().UnixNano())
	w := worker.New(c, queue, worker.Options{})
	register(w)
	if err := w.Start(); err != nil {
		return err
	}
	defer w.Stop()
	run, err := c.ExecuteWorkflow(ctx, client.StartWorkflowOptions{ID: queue, TaskQueue: queue}, "Workflow", "absent", true)
	if err != nil {
		return err
	}
	// Queries fence startup and prove that metadata calls remain read-only.
	for {
		result, err := c.QueryWorkflow(ctx, run.GetID(), run.GetRunID(), "metadata-state")
		if err == nil {
			var value string
			err = result.Get(&value)
			if err != nil {
				return err
			}
			if value != "ok" {
				return fmt.Errorf("query result: %q", value)
			}
			break
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(20 * time.Millisecond):
		}
	}
	result, err := c.QueryWorkflow(ctx, run.GetID(), run.GetRunID(), "__temporal_workflow_metadata")
	if err != nil {
		return err
	}
	var info sdkpb.WorkflowMetadata
	if err := result.Get(&info); err != nil {
		return err
	}
	if len(info.Definition.SignalDefinitions) != 1 || info.Definition.SignalDefinitions[0].Description != "Complete the metadata check" {
		return fmt.Errorf("signal descriptions: %v", info.Definition)
	}
	if _, err := c.QueryWorkflow(ctx, run.GetID(), run.GetRunID(), "bad-metadata"); err == nil {
		return fmt.Errorf("mutating query succeeded")
	}
	update, err := c.UpdateWorkflow(ctx, client.UpdateWorkflowOptions{
		WorkflowID: run.GetID(), RunID: run.GetRunID(), UpdateName: "check",
		WaitForStage: client.WorkflowUpdateStageCompleted,
	})
	if err != nil {
		return err
	}
	if err := update.Get(ctx, nil); err != nil {
		return err
	}
	if err := c.SignalWorkflow(ctx, run.GetID(), run.GetRunID(), "finish", nil); err != nil {
		return err
	}
	var completion string
	if err := run.Get(ctx, &completion); err != nil {
		return err
	}
	if completion != "ok" {
		return fmt.Errorf("completion: %q", completion)
	}
	description, err := c.DescribeWorkflowExecution(ctx, run.GetID(), run.GetRunID())
	if err != nil {
		return err
	}
	fields := description.WorkflowExecutionInfo.Memo.Fields
	var number int64
	if err := converter.GetDefaultDataConverter().FromPayload(fields["number"], &number); err != nil {
		return err
	}
	if number != metadata.Precise || len(fields) != 1 {
		return fmt.Errorf("memo value/deletion: %v", fields)
	}
	history := new(historypb.History)
	events := c.GetWorkflowHistory(ctx, run.GetID(), run.GetRunID(), false, 0)
	for events.HasNext() {
		event, err := events.Next()
		if err != nil {
			return err
		}
		history.Events = append(history.Events, event)
	}
	if output != "" {
		raw, err := protojson.MarshalOptions{Indent: "  "}.Marshal(history)
		if err != nil {
			return err
		}
		if err := os.WriteFile(output, raw, 0644); err != nil {
			return err
		}
	}
	return replay(history)
}
