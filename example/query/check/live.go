package main

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"time"

	"github.com/mfateev/sdk-go-poc/example/query"
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
	queue := fmt.Sprintf("isolate-query-%d", time.Now().UnixNano())
	newWorker := func() worker.Worker {
		w := worker.New(c, queue, worker.Options{})
		w.RegisterWorkflowWithOptions(query.QueryWorkflow, sdkwf.RegisterOptions{Name: "QueriedAlias"})
		return w
	}
	w := newWorker()
	if err := w.Start(); err != nil {
		return err
	}
	defer func() { w.Stop() }()
	run, err := c.ExecuteWorkflow(ctx, client.StartWorkflowOptions{ID: queue, TaskQueue: queue}, "QueriedAlias", 41)
	if err != nil {
		return err
	}
	state := func(expected int) error {
		v, err := c.QueryWorkflow(ctx, run.GetID(), run.GetRunID(), "state", "live")
		if err != nil {
			return err
		}
		var result struct {
			Count  int
			Prefix string
		}
		if err := v.Get(&result); err != nil {
			return err
		}
		if result.Count != expected || result.Prefix != "live" {
			return fmt.Errorf("query result: %+v", result)
		}
		return nil
	}
	if err := state(41); err != nil {
		return err
	}
	for _, mode := range []string{"field", "map", "copy", "atomic", "reflect", "callback", "error-callback", "goroutine", "channel", "exit", "goexit", "environment", "mutex", "waitgroup", "cond", "activity", "timer", "raw"} {
		if _, err := c.QueryWorkflow(ctx, run.GetID(), run.GetRunID(), "bad", mode); err == nil {
			return fmt.Errorf("live query accepted %s", mode)
		}
		if err := state(41); err != nil {
			return fmt.Errorf("state after %s: %w", mode, err)
		}
	}
	if err := c.SignalWorkflow(ctx, run.GetID(), run.GetRunID(), "finish", []byte("yes")); err != nil {
		return err
	}
	var result int
	if err := run.Get(ctx, &result); err != nil {
		return err
	}
	if result != 42 {
		return fmt.Errorf("workflow result=%d", result)
	}
	if err := state(42); err != nil {
		return fmt.Errorf("completed query: %w", err)
	}
	w.Stop()
	binary, err := os.Executable()
	if err != nil {
		return err
	}
	// A new process has no SDK sticky cache or retained isolate to reuse.
	cold := exec.CommandContext(ctx, binary, "-address", address, "-queue", queue, "-cold-id", run.GetID())
	if output, err := cold.CombinedOutput(); err != nil {
		return fmt.Errorf("cold completed query: %w\n%s", err, output)
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
	if err := os.WriteFile("/tmp/feature7-query-history.json", raw, 0600); err != nil {
		return err
	}
	replayer := worker.NewWorkflowReplayer()
	replayer.RegisterWorkflowWithOptions(query.QueryWorkflow, sdkwf.RegisterOptions{Name: "QueriedAlias"})
	if err := replayer.ReplayWorkflowHistory(nil, history); err != nil {
		return err
	}
	fmt.Println("live running/completed/cold queries and saved history replay passed")
	return nil
}

func checkCold(address, queue, id string) error {
	c, err := client.Dial(client.Options{HostPort: address})
	if err != nil {
		return err
	}
	defer c.Close()
	w := worker.New(c, queue, worker.Options{})
	w.RegisterWorkflowWithOptions(query.QueryWorkflow, sdkwf.RegisterOptions{Name: "QueriedAlias"})
	if err := w.Start(); err != nil {
		return err
	}
	defer w.Stop()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	v, err := c.QueryWorkflow(ctx, id, "", "state", "cold")
	if err != nil {
		return err
	}
	var result struct {
		Count  int
		Prefix string
	}
	if err := v.Get(&result); err != nil {
		return err
	}
	if result.Count != 42 || result.Prefix != "cold" {
		return fmt.Errorf("cold completed state: %+v", result)
	}
	return nil
}
