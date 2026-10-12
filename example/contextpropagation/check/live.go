package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	example "github.com/mfateev/sdk-go-poc/example/contextpropagation"
	"github.com/mfateev/sdk-go-poc/example/contextpropagation/support"
	"github.com/mfateev/sdk-go-poc/worker"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
	goWorker "go.temporal.io/sdk/worker"
	goWorkflow "go.temporal.io/sdk/workflow"
	"google.golang.org/protobuf/encoding/protojson"
)

func registerNative(w interface{ RegisterWorkflow(any) }) {
	w.RegisterWorkflow(example.Workflow)
	w.RegisterWorkflow(example.Child)
	check(worker.SetIsolateContextPropagators(w, worker.ContextPropagatorOptions{Factory: support.Propagators, Config: []byte("normal")}))
	check(worker.SetIsolateDataConverter(w, worker.DataConverterOptions{Factory: support.DataConverter}))
}
func replay(file string) error {
	raw, err := os.ReadFile(file)
	if err != nil {
		return err
	}
	history := new(historypb.History)
	if err := protojson.Unmarshal(raw, history); err != nil {
		return err
	}
	r, err := worker.NewWorkflowReplayerWithOptions(worker.WorkflowReplayerOptions{DataConverter: support.CompatibleConverter()})
	if err != nil {
		return err
	}
	registerNative(r)
	if err := r.ReplayWorkflowHistory(nil, history); err != nil {
		return err
	}
	var result string
	if err := r.(interface{ GetWorkflowResult(string, any) error }).GetWorkflowResult("", &result); err != nil {
		return err
	}
	if result != "ok" {
		return fmt.Errorf("replayed result %q", result)
	}
	return nil
}
func checkLive(address, dir string, cacheSize int) error {
	goWorker.SetStickyWorkflowCacheSize(cacheSize)
	c, err := client.Dial(client.Options{HostPort: address, DataConverter: support.CompatibleConverter(), ContextPropagators: []goWorkflow.ContextPropagator{hostPropagator{}}})
	if err != nil {
		return err
	}
	defer c.Close()
	ctx, cancel := context.WithTimeout(context.WithValue(context.Background(), support.HeaderKey, "test-data"), 90*time.Second)
	defer cancel()
	queue := fmt.Sprintf("isolate-context-%d", time.Now().UnixNano())
	w := worker.New(c, queue, worker.Options{})
	registerNative(w)
	w.RegisterActivity(ContextActivity)
	w.RegisterActivity(Echo)
	w.RegisterActivity(Hello)
	if err := w.Start(); err != nil {
		return err
	}
	defer w.Stop()
	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}
	for _, scenario := range []string{"header-root", "child-derived", "local-header", "context-aware-converter", "per-context-converter"} {
		run, err := c.ExecuteWorkflow(ctx, client.StartWorkflowOptions{ID: queue + "-" + scenario, TaskQueue: queue}, "Workflow", scenario)
		if err != nil {
			return err
		}
		for {
			value, err := c.QueryWorkflow(ctx, run.GetID(), "", "ready")
			if err != nil && strings.Contains(err.Error(), "unknown queryType ready") {
				// RPCs supply the wait while the workflow installs its handlers.
				continue
			}
			if err != nil {
				return err
			}
			var ready bool
			if err := value.Get(&ready); err != nil {
				return err
			}
			if ready {
				break
			}
		}
		update, err := c.UpdateWorkflow(ctx, client.UpdateWorkflowOptions{WorkflowID: run.GetID(), UpdateName: "value", WaitForStage: client.WorkflowUpdateStageCompleted})
		if err != nil {
			return err
		}
		var result string
		if err := update.Get(ctx, &result); err != nil || result != "test-data" {
			return fmt.Errorf("live update result %q: %w", result, err)
		}
		queryCtx := context.WithValue(ctx, support.HeaderKey, "test-query")
		value, err := c.QueryWorkflow(queryCtx, run.GetID(), "", "value")
		if err != nil {
			return err
		}
		var raw converter.RawValue
		if err := value.Get(&raw); err != nil {
			return err
		}
		if string(raw.Payload().Metadata["propagated-context"]) != "test-query" {
			return fmt.Errorf("query converter did not receive incoming context: %v", raw.Payload().Metadata)
		}
		value, err = c.QueryWorkflow(ctx, run.GetID(), "", "future")
		if err != nil {
			return err
		}
		if err := value.Get(&result); err != nil || result != "query-result" {
			return fmt.Errorf("live future query %q: %w", result, err)
		}
		if err := run.Get(ctx, &result); err != nil || result != "ok" {
			return fmt.Errorf("live result %q: %w", result, err)
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
		bytes, err := protojson.MarshalOptions{Indent: "  "}.Marshal(history)
		if err != nil {
			return err
		}
		file := filepath.Join(dir, scenario+".json")
		if err := os.WriteFile(file, bytes, 0644); err != nil {
			return err
		}
		if err := replay(file); err != nil {
			return err
		}
	}
	fmt.Printf("live native context propagation and replay passed (cache=%d)\n", cacheSize)
	return nil
}
