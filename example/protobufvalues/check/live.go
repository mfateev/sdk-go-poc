package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	example "github.com/mfateev/sdk-go-poc/example/protobufvalues"
	"github.com/mfateev/sdk-go-poc/example/protobufvalues/model"
	"github.com/mfateev/sdk-go-poc/worker"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/temporal"
	goWorker "go.temporal.io/sdk/worker"
	"google.golang.org/protobuf/encoding/protojson"
)

func hostConverter(mode string) converter.DataConverter {
	return converter.NewCodecDataConverter(example.DataConverter(mode), converter.NewZlibCodec(converter.ZlibCodecOptions{AlwaysEncode: true}))
}
func replay(file string) error {
	raw, err := os.ReadFile(file)
	if err != nil {
		return err
	}
	h := new(historypb.History)
	if err := protojson.Unmarshal(raw, h); err != nil {
		return err
	}
	mode := strings.Split(filepath.Base(file), "-")[0]
	r, err := worker.NewWorkflowReplayerWithOptions(worker.WorkflowReplayerOptions{DataConverter: hostConverter(mode), FailureConverter: temporal.NewDefaultFailureConverter(temporal.DefaultFailureConverterOptions{DataConverter: hostConverter(mode)})})
	if err != nil {
		return err
	}
	register(r, mode)
	return r.ReplayWorkflowHistory(nil, h)
}
func replayDirectory(dir string) error {
	files, err := filepath.Glob(filepath.Join(dir, "*.json"))
	if err != nil {
		return err
	}
	if len(files) == 0 {
		return fmt.Errorf("no saved protobuf histories")
	}
	for _, file := range files {
		for _, procs := range []int{1, 2, 8} {
			runtime.GOMAXPROCS(procs)
			if err := replay(file); err != nil {
				return fmt.Errorf("%s: %w", file, err)
			}
		}
	}
	fmt.Println("saved protobuf histories replayed")
	return nil
}
func live(address, dir string, cache int) error {
	goWorker.SetStickyWorkflowCacheSize(cache)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}
	for _, mode := range []string{"json", "binary"} {
		c, err := client.Dial(client.Options{HostPort: address, DataConverter: hostConverter(mode), FailureConverter: temporal.NewDefaultFailureConverter(temporal.DefaultFailureConverterOptions{DataConverter: hostConverter(mode)})})
		if err != nil {
			return err
		}
		err = liveMode(c, mode, dir)
		c.Close()
		if err != nil {
			return err
		}
	}
	fmt.Printf("live protobuf conversion and replay passed (cache=%d)\n", cache)
	return nil
}
func liveMode(c client.Client, mode, dir string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	queue := fmt.Sprintf("native-protobuf-%s-%d", mode, time.Now().UnixNano())
	w := worker.New(c, queue, worker.Options{DeadlockDetectionTimeout: 10 * time.Second})
	register(w, mode)
	w.RegisterActivity(Echo)
	w.RegisterActivity(Payloads)
	w.RegisterActivity(Random)
	if err := w.Start(); err != nil {
		return err
	}
	defer w.Stop()
	for _, scenario := range []string{"values", "failure", "continue", "payloads", "random"} {
		name := "Workflow"
		args := []any{example.Record(), scenario}
		if scenario == "payloads" {
			name = "UpstreamPayloads"
			args = nil
		}
		if scenario == "random" {
			name = "UpstreamRandom"
			args = nil
		}
		run, err := c.ExecuteWorkflow(ctx, client.StartWorkflowOptions{ID: queue + "-" + scenario, TaskQueue: queue}, name, args...)
		if err != nil {
			return err
		}
		if scenario == "values" {
			// Retry only registration readiness; no fixed sleeps or workflow decisions.
			for {
				value, err := c.QueryWorkflow(ctx, run.GetID(), run.GetRunID(), "ready")
				if err == nil {
					var ready bool
					if err := value.Get(&ready); err != nil {
						return err
					}
					if ready {
						break
					}
				} else if !strings.Contains(err.Error(), "Unknown query type") && !strings.Contains(err.Error(), "unknown queryType") {
					return err
				}
				if ctx.Err() != nil {
					return ctx.Err()
				}
			}
			for range 2 {
				checkValue, err := c.QueryWorkflow(ctx, run.GetID(), run.GetRunID(), "conversions")
				if err != nil {
					return err
				}
				var valid bool
				if err := checkValue.Get(&valid); err != nil {
					return err
				}
				if !valid {
					return fmt.Errorf("scratch conversions failed")
				}
				for _, name := range []string{"value", "future", "echo"} {
					var value converter.EncodedValue
					var err error
					if name == "echo" {
						value, err = c.QueryWorkflow(ctx, run.GetID(), run.GetRunID(), name, example.Record())
					} else {
						value, err = c.QueryWorkflow(ctx, run.GetID(), run.GetRunID(), name)
					}
					if err != nil {
						return err
					}
					var record *model.Record
					if err := value.Get(&record); err != nil {
						return err
					}
					if err := example.CheckRecord(record); err != nil {
						return err
					}
				}
				for _, name := range []string{"mutate", "mutate-reflection"} {
					_, err := c.QueryWorkflow(ctx, run.GetID(), run.GetRunID(), name)
					if err == nil || !strings.Contains(err.Error(), "read-only") {
						return fmt.Errorf("query mutation was accepted: %v", err)
					}
				}
				update, err := c.UpdateWorkflow(ctx, client.UpdateWorkflowOptions{WorkflowID: run.GetID(), RunID: run.GetRunID(), UpdateName: "update", WaitForStage: client.WorkflowUpdateStageCompleted, Args: []any{example.Record()}})
				if err != nil {
					return err
				}
				var record *model.Record
				if err := update.Get(ctx, &record); err != nil {
					return err
				}
				if err := example.CheckRecord(record); err != nil {
					return err
				}
			}
			if err := c.SignalWorkflow(ctx, run.GetID(), run.GetRunID(), "record", example.Record()); err != nil {
				return err
			}
		}
		if scenario == "failure" {
			err = run.Get(ctx, nil)
			var app *temporal.ApplicationError
			if !errors.As(err, &app) {
				return fmt.Errorf("missing application failure: %v", err)
			}
			var record *model.Record
			if err := app.Details(&record); err != nil {
				return err
			}
			if err := example.CheckRecord(record); err != nil {
				return err
			}
		} else if scenario == "values" || scenario == "continue" {
			var record *model.Record
			if err := run.Get(ctx, &record); err != nil {
				return err
			}
			if err := example.CheckRecord(record); err != nil {
				return err
			}
		} else if err := run.Get(ctx, nil); err != nil {
			return err
		}
		h := new(historypb.History)
		it := c.GetWorkflowHistory(ctx, run.GetID(), run.GetRunID(), false, enumspb.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)
		for it.HasNext() {
			event, err := it.Next()
			if err != nil {
				return err
			}
			h.Events = append(h.Events, event)
		}
		raw, err := protojson.MarshalOptions{Indent: "  "}.Marshal(h)
		if err != nil {
			return err
		}
		file := filepath.Join(dir, mode+"-"+scenario+".json")
		if err := os.WriteFile(file, raw, 0644); err != nil {
			return err
		}
		if err := replay(file); err != nil {
			return err
		}
	}
	return nil
}
