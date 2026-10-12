package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	example "github.com/mfateev/sdk-go-poc/example/failureconversion"
	"github.com/mfateev/sdk-go-poc/example/failureconversion/support"
	"github.com/mfateev/sdk-go-poc/worker"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/temporal"
	goWorker "go.temporal.io/sdk/worker"
	"google.golang.org/protobuf/encoding/protojson"
)

func hostDataConverter() converter.DataConverter {
	return converter.NewCodecDataConverter(converter.GetDefaultDataConverter(), converter.NewZlibCodec(converter.ZlibCodecOptions{AlwaysEncode: true}))
}

func registerNative(w interface{ RegisterWorkflow(any) }) {
	w.RegisterWorkflow(example.Workflow)
	w.RegisterWorkflow(example.Child)
	check(worker.SetIsolateFailureConverter(w, worker.FailureConverterOptions{Factory: support.Factory, Config: []byte(support.ErrorType)}))
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
	dc := hostDataConverter()
	r, err := worker.NewWorkflowReplayerWithOptions(worker.WorkflowReplayerOptions{DataConverter: dc, FailureConverter: support.New(dc)})
	if err != nil {
		return err
	}
	registerNative(r)
	return r.ReplayWorkflowHistory(nil, h)
}

func live(address, dir string, cacheSize int) error {
	goWorker.SetStickyWorkflowCacheSize(cacheSize)
	dc := hostDataConverter()
	c, err := client.Dial(client.Options{HostPort: address, DataConverter: dc, FailureConverter: support.New(dc)})
	if err != nil {
		return err
	}
	defer c.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	queue := fmt.Sprintf("native-failures-%d", time.Now().UnixNano())
	w := worker.New(c, queue, worker.Options{})
	registerNative(w)
	w.RegisterActivity(Fail)
	if err := w.Start(); err != nil {
		return err
	}
	defer w.Stop()
	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}
	for _, scenario := range []string{"activity", "local", "child", "workflow", "fallback"} {
		run, err := c.ExecuteWorkflow(ctx, client.StartWorkflowOptions{ID: queue + "-" + scenario, TaskQueue: queue}, "Workflow", scenario)
		if err != nil {
			return err
		}
		err = run.Get(ctx, nil)
		if scenario == "fallback" {
			var app *temporal.ApplicationError
			if !errors.As(err, &app) || app.Type() != "Ordinary" || !app.NonRetryable() {
				return fmt.Errorf("fallback failure lost: %v", err)
			}
		} else {
			assertCustom(err)
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
		if scenario == "activity" {
			count := 0
			for _, event := range h.Events {
				if event.GetActivityTaskFailedEventAttributes() != nil {
					count++
				}
			}
			if count != 1 {
				return fmt.Errorf("activity failure events=%d", count)
			}
		}
		if last := h.Events[len(h.Events)-1].GetWorkflowExecutionFailedEventAttributes(); last == nil || last.Failure.GetApplicationFailureInfo().GetNonRetryable() != true {
			return fmt.Errorf("workflow retry flag lost")
		}
		raw, err := protojson.MarshalOptions{Indent: "  "}.Marshal(h)
		if err != nil {
			return err
		}
		file := filepath.Join(dir, scenario+".json")
		if err := os.WriteFile(file, raw, 0644); err != nil {
			return err
		}
		if err := replay(file); err != nil {
			return err
		}
	}
	fmt.Printf("live custom failure conversion and replay passed (cache=%d)\n", cacheSize)
	return nil
}

func Fail(context.Context) error { return support.CustomError{Detail: support.Precise} }
