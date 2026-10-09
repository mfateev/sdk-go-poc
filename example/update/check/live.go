package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/mfateev/sdk-go-poc/example/update"
	"github.com/mfateev/sdk-go-poc/worker"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/sdk/client"
	sdkwf "go.temporal.io/sdk/workflow"
	"google.golang.org/protobuf/encoding/protojson"
)

func checkLive(address string) error {
	c, err := client.Dial(client.Options{HostPort: address})
	if err != nil {
		return err
	}
	defer c.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	queue := fmt.Sprintf("isolate-update-%d", time.Now().UnixNano())
	w := worker.New(c, queue, worker.Options{})
	w.RegisterWorkflowWithOptions(update.UpdateWorkflow, sdkwf.RegisterOptions{Name: "UpdatedAlias"})
	if err := w.Start(); err != nil {
		return err
	}
	defer w.Stop()
	run, err := c.ExecuteWorkflow(ctx, client.StartWorkflowOptions{ID: queue, TaskQueue: queue}, "UpdatedAlias", int64(40))
	if err != nil {
		return err
	}
	state := func(expected int64) error {
		v, err := c.QueryWorkflow(ctx, run.GetID(), run.GetRunID(), "state")
		if err != nil {
			return err
		}
		var value int64
		if err := v.Get(&value); err != nil {
			return err
		}
		if value != expected {
			return fmt.Errorf("live state=%d, expected=%d", value, expected)
		}
		return nil
	}
	send := func(name string, args ...any) (client.WorkflowUpdateHandle, error) {
		return c.UpdateWorkflow(ctx, client.UpdateWorkflowOptions{WorkflowID: run.GetID(), RunID: run.GetRunID(), UpdateName: name, Args: args, WaitForStage: client.WorkflowUpdateStageCompleted})
	}
	if err := state(40); err != nil {
		return err
	}
	for _, mode := range []string{"field", "map", "slice", "atomic", "reflect", "callback", "error-callback", "goroutine", "channel", "waitgroup", "activity", "timer", "exit", "goexit"} {
		h, err := send("bad", mode)
		if err == nil {
			err = h.Get(ctx, nil)
		}
		if err == nil {
			return fmt.Errorf("live validator accepted %s", mode)
		}
		if err := state(40); err != nil {
			return fmt.Errorf("after %s: %w", mode, err)
		}
	}
	h, err := send("add", int64(2))
	if err != nil {
		return err
	}
	var value int64
	if err := h.Get(ctx, &value); err != nil {
		return err
	}
	if value != 42 {
		return fmt.Errorf("add result=%d", value)
	}
	h, err = send("delayed", int64(4))
	if err != nil {
		return err
	}
	if err := h.Get(ctx, &value); err != nil {
		return err
	}
	if value != 46 {
		return fmt.Errorf("delayed result=%d", value)
	}
	h, err = send("error-only", int64(3))
	if err != nil {
		return err
	}
	if err := h.Get(ctx, nil); err != nil {
		return err
	}
	if err := state(49); err != nil {
		return err
	}
	h, err = send("failure", int64(0))
	if err != nil {
		return err
	}
	if err := h.Get(ctx, nil); err == nil {
		return fmt.Errorf("handler failure was lost")
	}
	h, err = c.UpdateWorkflow(ctx, client.UpdateWorkflowOptions{WorkflowID: run.GetID(), RunID: run.GetRunID(), UpdateName: "cancellation", WaitForStage: client.WorkflowUpdateStageAccepted})
	if err != nil {
		return err
	}
	if err := c.CancelWorkflow(ctx, run.GetID(), run.GetRunID()); err != nil {
		return err
	}
	if err := h.Get(ctx, nil); err == nil {
		return fmt.Errorf("update cancellation was lost")
	}
	if err := c.SignalWorkflow(ctx, run.GetID(), run.GetRunID(), "finish", []byte("finish")); err != nil {
		return err
	}
	if err := run.Get(ctx, &value); err != nil {
		return err
	}
	if value != 49 {
		return fmt.Errorf("workflow result=%d", value)
	}
	if err := state(49); err != nil {
		return err
	}
	history := new(historypb.History)
	iter := c.GetWorkflowHistory(ctx, run.GetID(), run.GetRunID(), false, enumspb.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)
	for iter.HasNext() {
		event, err := iter.Next()
		if err != nil {
			return err
		}
		history.Events = append(history.Events, event)
	}
	raw, err := protojson.MarshalOptions{Indent: "  "}.Marshal(history)
	if err != nil {
		return err
	}
	if err := os.WriteFile("/tmp/feature7-update-history.json", raw, 0600); err != nil {
		return err
	}
	replayer := worker.NewWorkflowReplayer()
	replayer.RegisterWorkflowWithOptions(update.UpdateWorkflow, sdkwf.RegisterOptions{Name: "UpdatedAlias"})
	if err := replayer.ReplayWorkflowHistory(nil, history); err != nil {
		return err
	}
	fmt.Println("live updates, rejected validators, cancellation and saved-history replay passed")
	return nil
}
