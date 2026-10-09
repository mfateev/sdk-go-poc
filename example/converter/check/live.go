package main

import (
	"context"
	"fmt"
	"os"
	"time"

	example "github.com/mfateev/sdk-go-poc/example/converter"
	"github.com/mfateev/sdk-go-poc/example/converter/custom"
	"github.com/mfateev/sdk-go-poc/worker"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/temporal"
	goWorkflow "go.temporal.io/sdk/workflow"
	"google.golang.org/protobuf/encoding/protojson"
)

func hostConverter() converter.DataConverter {
	dc, err := custom.New([]byte("configured"))
	check(err)
	return converter.NewCodecDataConverter(dc, &encryptedCodec{})
}
func register(w interface{ RegisterWorkflow(any) }) {
	w.RegisterWorkflow(example.Workflow)
	w.RegisterWorkflow(example.Parent)
	w.RegisterWorkflow(example.Child)
	w.RegisterWorkflow(ordinary)
	check(worker.SetIsolateDataConverter(w, worker.DataConverterOptions{Factory: custom.New, Config: []byte("configured")}))
}
func Echo(_ context.Context, input int64) (int64, error) { return input, nil }
func Fail(_ context.Context, input int64) (int64, error) {
	return 0, temporal.NewNonRetryableApplicationError("failed", "ExampleError", nil, input)
}
func ordinary(_ goWorkflow.Context, input int64) (int64, error) { return input, nil }

func replayFile(file string) error {
	raw, err := os.ReadFile(file)
	if err != nil {
		return err
	}
	history := new(historypb.History)
	if err := protojson.Unmarshal(raw, history); err != nil {
		return err
	}
	r, err := worker.NewWorkflowReplayerWithOptions(worker.WorkflowReplayerOptions{DataConverter: hostConverter()})
	if err != nil {
		return err
	}
	register(r)
	if err := r.ReplayWorkflowHistory(nil, history); err != nil {
		return err
	}
	var actual int64
	if err := r.(interface{ GetWorkflowResult(string, any) error }).GetWorkflowResult("", &actual); err != nil {
		return err
	}
	const original int64 = 9007199254740993
	expected := original
	if history.Events[0].GetWorkflowExecutionStartedEventAttributes().WorkflowType.Name == "Workflow" {
		expected += 5
	}
	if actual != expected {
		return fmt.Errorf("replayed result=%d want %d", actual, expected)
	}
	fmt.Println("encrypted custom-converter history replayed with exact result")
	return nil
}
func checkLive(address string) error {
	c, err := client.Dial(client.Options{HostPort: address, DataConverter: hostConverter(), FailureConverter: temporal.NewDefaultFailureConverter(temporal.DefaultFailureConverterOptions{DataConverter: hostConverter(), EncodeCommonAttributes: true})})
	if err != nil {
		return err
	}
	defer c.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	queue := fmt.Sprintf("isolate-converter-%d", time.Now().UnixNano())
	w := worker.New(c, queue, worker.Options{})
	register(w)
	w.RegisterActivity(Echo)
	w.RegisterActivity(Fail)
	if err := w.Start(); err != nil {
		return err
	}
	defer w.Stop()
	const input int64 = 9007199254740993
	for _, name := range []string{"Workflow", "Parent", "ordinary"} {
		args := []any{input}
		if name == "Parent" {
			args = append(args, false)
		}
		run, err := c.ExecuteWorkflow(ctx, client.StartWorkflowOptions{ID: queue + "-" + name, TaskQueue: queue}, name, args...)
		if err != nil {
			return err
		}
		expected := input
		if name == "Workflow" {
			// Updates wait until handler registration. Their validators and results use
			// the configured serializer while normal workflow state stays read-only.
			update, err := c.UpdateWorkflow(ctx, client.UpdateWorkflowOptions{WorkflowID: run.GetID(), UpdateName: "add", WaitForStage: client.WorkflowUpdateStageCompleted, Args: []any{int64(5)}})
			if err != nil {
				return err
			}
			var result int64
			if err := update.Get(ctx, &result); err != nil {
				return err
			}
			expected += 5
			if result != expected {
				return fmt.Errorf("update result=%d", result)
			}
			rejected, err := c.UpdateWorkflow(ctx, client.UpdateWorkflowOptions{WorkflowID: run.GetID(), UpdateName: "add", WaitForStage: client.WorkflowUpdateStageCompleted, Args: []any{int64(-3)}})
			if err == nil {
				err = rejected.Get(ctx, nil)
			}
			if err == nil {
				return fmt.Errorf("negative update accepted")
			}
			result = 0
			value, err := c.QueryWorkflow(ctx, run.GetID(), "", "state")
			if err != nil {
				return err
			}
			if err := value.Get(&result); err != nil || result != expected {
				return fmt.Errorf("query=%d err=%v", result, err)
			}
			if _, err := c.QueryWorkflow(ctx, run.GetID(), "", "bad"); err == nil {
				return fmt.Errorf("mutating query accepted")
			}
			if err := c.SignalWorkflow(ctx, run.GetID(), "", "finish", []byte("finish")); err != nil {
				return err
			}
		}
		var actual int64
		if err := run.Get(ctx, &actual); err != nil {
			return fmt.Errorf("%s: %w", name, err)
		}
		if actual != expected {
			return fmt.Errorf("%s result=%d", name, actual)
		}
		if name == "ordinary" {
			continue
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
		raw, err := protojson.MarshalOptions{Indent: "  "}.Marshal(history)
		if err != nil {
			return err
		}
		file := "/tmp/feature8-" + name + "-history.json"
		if err := os.WriteFile(file, raw, 0644); err != nil {
			return err
		}
		if err := replayFile(file); err != nil {
			return err
		}
	}
	fmt.Println("live encrypted activities, signals, queries, updates, children, continuation and ordinary SDK workflow passed")
	return nil
}
