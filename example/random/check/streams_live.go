package main

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"time"

	example "github.com/mfateev/sdk-go-poc/example/random"
	"github.com/mfateev/sdk-go-poc/worker"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/testsuite"
	sdkworker "go.temporal.io/sdk/worker"
	"google.golang.org/protobuf/encoding/protojson"
)

type streamActivities struct{ started, release chan struct{} }

func (a *streamActivities) EchoStream(ctx context.Context, trace example.StreamTrace) (example.StreamTrace, error) {
	select {
	case a.started <- struct{}{}:
	default:
	}
	select {
	case <-a.release:
		return trace, nil
	case <-ctx.Done():
		return trace, ctx.Err()
	}
}

func recordStreams(dir string) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	server, err := testsuite.StartDevServer(ctx, testsuite.DevServerOptions{})
	must(err)
	defer func() { must(server.Stop()) }()
	c := server.Client()
	defer sdkworker.SetStickyWorkflowCacheSize(1000)
	for _, cache := range []int{1000, 0} {
		sdkworker.SetStickyWorkflowCacheSize(cache)
		queue := fmt.Sprintf("named-streams-%d-%d", cache, time.Now().UnixNano())
		w := worker.New(c, queue, worker.Options{})
		w.RegisterWorkflow(example.StreamsWorkflow)
		w.RegisterWorkflow(example.DistributionWorkflow)
		activities := &streamActivities{started: make(chan struct{}, 1), release: make(chan struct{})}
		w.RegisterActivityWithOptions(activities.EchoStream, activity.RegisterOptions{Name: "EchoStream"})
		must(w.Start())
		func() {
			defer w.Stop()
			run, err := c.ExecuteWorkflow(ctx, client.StartWorkflowOptions{ID: queue, TaskQueue: queue}, "StreamsWorkflow", "other-noise")
			must(err)
			select {
			case <-activities.started:
			case <-ctx.Done():
				panic(ctx.Err())
			}
			var before []uint64
			for i := range 3 {
				query, err := c.QueryWorkflow(ctx, run.GetID(), run.GetRunID(), "named")
				must(err)
				var values []uint64
				must(query.Get(&values))
				if i == 0 {
					before = values
				} else if !reflect.DeepEqual(before, values) {
					panic("live query advanced named state")
				}
				_, err = c.QueryWorkflow(ctx, run.GetID(), run.GetRunID(), "new-name")
				must(err)
				handle, err := c.UpdateWorkflow(ctx, client.UpdateWorkflowOptions{WorkflowID: run.GetID(), RunID: run.GetRunID(), UpdateName: "probe", UpdateID: fmt.Sprint(i), WaitForStage: client.WorkflowUpdateStageCompleted})
				if err == nil && handle.Get(ctx, nil) == nil {
					panic("random probe validator was accepted")
				}
			}
			close(activities.release)
			var result example.StreamTrace
			must(run.Get(ctx, &result))
			if !reflect.DeepEqual(result.Primary[4:], before) {
				panic("live queries or validators changed subsequent draws")
			}
			recordStreamHistory(ctx, c, run, dir, fmt.Sprintf("named-cache-%d.json", cache))
			distribution, err := c.ExecuteWorkflow(ctx, client.StartWorkflowOptions{ID: queue + "-distributions", TaskQueue: queue}, "DistributionWorkflow")
			must(err)
			var values []string
			must(distribution.Get(ctx, &values))
			recordStreamHistory(ctx, c, distribution, dir, fmt.Sprintf("distributions-cache-%d.json", cache))
		}()
	}
	fmt.Println("live named streams, validators, cache eviction and saved-history replay passed")
}

func recordStreamHistory(ctx context.Context, c client.Client, run client.WorkflowRun, dir, name string) {
	iterator := c.GetWorkflowHistory(ctx, run.GetID(), run.GetRunID(), false, enumspb.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)
	history := new(historypb.History)
	for iterator.HasNext() {
		event, err := iterator.Next()
		must(err)
		history.Events = append(history.Events, event)
	}
	raw, err := protojson.MarshalOptions{Indent: "  "}.Marshal(history)
	must(err)
	must(os.MkdirAll(dir, 0755))
	path := filepath.Join(dir, name)
	must(os.WriteFile(path, raw, 0644))
	must(replayStreamsHistory(path))
}

func replayStreamsHistory(path string) error {
	raw, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	history, err := client.HistoryFromJSON(bytes.NewReader(raw), client.HistoryJSONOptions{})
	if err != nil {
		return err
	}
	if len(history.Events) == 0 {
		return fmt.Errorf("named stream history is empty")
	}
	completion := history.Events[len(history.Events)-1].GetWorkflowExecutionCompletedEventAttributes()
	if completion == nil {
		return fmt.Errorf("named stream history is not completed")
	}
	replayer := worker.NewWorkflowReplayer()
	replayer.RegisterWorkflow(example.StreamsWorkflow)
	replayer.RegisterWorkflow(example.DistributionWorkflow)
	replayer.RegisterActivityWithOptions(example.EchoStream, activity.RegisterOptions{Name: "EchoStream"})
	if err := replayer.ReplayWorkflowHistory(nil, history); err != nil {
		return err
	}
	getter := replayer.(interface{ GetWorkflowResult(string, any) error })
	dc := converter.GetDefaultDataConverter()
	if history.Events[0].GetWorkflowExecutionStartedEventAttributes().WorkflowType.Name == "StreamsWorkflow" {
		var expected, actual example.StreamTrace
		if err := dc.FromPayloads(completion.Result, &expected); err != nil {
			return err
		}
		if err := getter.GetWorkflowResult("", &actual); err != nil {
			return err
		}
		if !reflect.DeepEqual(actual, expected) {
			return fmt.Errorf("named stream outputs changed on replay")
		}
	} else {
		var expected, actual []string
		if err := dc.FromPayloads(completion.Result, &expected); err != nil {
			return err
		}
		if err := getter.GetWorkflowResult("", &actual); err != nil {
			return err
		}
		if !reflect.DeepEqual(actual, expected) {
			return fmt.Errorf("named distribution outputs changed on replay")
		}
	}
	return nil
}
