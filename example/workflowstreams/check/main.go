package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	streams "github.com/mfateev/sdk-go-poc/contrib/workflowstreams"
	example "github.com/mfateev/sdk-go-poc/example/workflowstreams"
	"github.com/mfateev/sdk-go-poc/worker"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/testsuite"
	sdkworker "go.temporal.io/sdk/worker"
	"google.golang.org/protobuf/encoding/protojson"
)

func must(err error) {
	if err != nil {
		panic(err)
	}
}
func dataConverter() converter.DataConverter {
	return converter.NewCodecDataConverter(converter.GetDefaultDataConverter(), converter.NewZlibCodec(converter.ZlibCodecOptions{AlwaysEncode: true}))
}
func replay(dir string) {
	paths, err := filepath.Glob(filepath.Join(dir, "*.json"))
	must(err)
	if len(paths) == 0 {
		panic("no workflow stream histories found")
	}
	for _, path := range paths {
		r, err := worker.NewWorkflowReplayerWithOptions(worker.WorkflowReplayerOptions{DataConverter: dataConverter()})
		must(err)
		r.RegisterWorkflow(example.Host)
		must(r.ReplayWorkflowHistoryFromJSONFile(nil, path))
	}
	fmt.Println("workflow stream history replay passed")
}

// Admission is observed directly so rollover happens while a subscription is
// waiting for the next item, rather than after an arbitrary delay.
type observingClient struct {
	client.Client
	waiting chan string
	once    sync.Once
}

func (c *observingClient) UpdateWorkflow(ctx context.Context, opts client.UpdateWorkflowOptions) (client.WorkflowUpdateHandle, error) {
	handle, err := c.Client.UpdateWorkflow(ctx, opts)
	if err == nil && opts.UpdateName == streams.PollUpdateName && opts.Args[0].(streams.PollInput).FromOffset == 3 {
		c.once.Do(func() { c.waiting <- handle.RunID() })
	}
	return handle, err
}
func saveHistory(ctx context.Context, c client.Client, id, runID, path string) {
	history := new(historypb.History)
	iterator := c.GetWorkflowHistory(ctx, id, runID, false, enumspb.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)
	for iterator.HasNext() {
		event, err := iterator.Next()
		must(err)
		history.Events = append(history.Events, event)
	}
	raw, err := protojson.Marshal(history)
	must(err)
	must(os.WriteFile(path, raw, 0644))
}
func waitSuccessor(ctx context.Context, c client.Client, id, previous string) string {
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		desc, err := c.DescribeWorkflowExecution(ctx, id, "")
		must(err)
		next := desc.GetWorkflowExecutionInfo().GetExecution().GetRunId()
		if next != previous {
			return next
		}
		select {
		case <-ctx.Done():
			panic(ctx.Err())
		case <-ticker.C:
		}
	}
}
func live(dir string) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	server, err := testsuite.StartDevServer(ctx, testsuite.DevServerOptions{ClientOptions: &client.Options{DataConverter: dataConverter()}})
	must(err)
	defer func() { must(server.Stop()) }()
	c := server.Client()
	must(os.MkdirAll(dir, 0755))
	for _, size := range []int{1000, 0} {
		sdkworker.SetStickyWorkflowCacheSize(size)
		queue := fmt.Sprintf("streams-%d-%d", size, time.Now().UnixNano())
		w := worker.New(c, queue, worker.Options{})
		w.RegisterWorkflow(example.Host)
		must(w.Start())
		func() {
			defer w.Stop()
			run, err := c.ExecuteWorkflow(ctx, client.StartWorkflowOptions{ID: queue, TaskQueue: queue}, "Host", (*streams.WorkflowStreamState)(nil))
			must(err)
			watched := &observingClient{Client: c, waiting: make(chan string, 1)}
			stream := streams.NewClient(watched, run.GetID(), streams.Options{BatchInterval: time.Hour})
			defer func() { must(stream.Close(ctx)) }()
			stream.Topic("a").Publish("one", false)
			stream.Topic("b").Publish("two", false)
			stream.Topic("a").Publish("three", false)
			must(stream.Flush(ctx))
			subscriptionDone := make(chan error, 1)
			go func() {
				count := 0
				for item, err := range stream.Subscribe(ctx, streams.SubscribeOptions{PollCooldown: time.Millisecond}) {
					if err != nil {
						subscriptionDone <- err
						return
					}
					var text string
					if err := converter.GetDefaultDataConverter().FromPayload(item.Data, &text); err != nil {
						subscriptionDone <- err
						return
					}
					if item.Offset != int64(count) || text != []string{"one", "two", "three", "four"}[count] {
						subscriptionDone <- fmt.Errorf("item %d: offset %d, value %q", count, item.Offset, text)
						return
					}
					count++
					if count == 4 {
						subscriptionDone <- nil
						return
					}
				}
				subscriptionDone <- errors.New("subscription ended before successor item")
			}()
			var admitted string
			select {
			case admitted = <-watched.waiting:
			case <-ctx.Done():
				panic(ctx.Err())
			}
			if admitted != run.GetRunID() {
				panic("subscription was not admitted to original run")
			}
			must(c.SignalWorkflow(ctx, run.GetID(), "", "command", "rollover"))
			nextRun := waitSuccessor(ctx, c, run.GetID(), run.GetRunID())
			stream.Topic("a").Publish("four", false)
			must(stream.Flush(ctx))
			select {
			case err := <-subscriptionDone:
				must(err)
			case <-ctx.Done():
				panic(ctx.Err())
			}
			offset, err := stream.GetOffset(ctx)
			must(err)
			if offset != 4 {
				panic(fmt.Sprintf("offset after rollover: %d", offset))
			}
			update, err := c.UpdateWorkflow(ctx, client.UpdateWorkflowOptions{WorkflowID: run.GetID(), UpdateName: "truncate", Args: []any{int64(2)}, WaitForStage: client.WorkflowUpdateStageCompleted})
			must(err)
			must(update.Get(ctx, nil))
			count := 0
			for item, err := range stream.Subscribe(ctx, streams.SubscribeOptions{FromOffset: 1, PollCooldown: time.Millisecond}) {
				must(err)
				if item.Offset != int64(count+2) {
					panic("truncation recovery lost offset")
				}
				count++
				if count == 2 {
					break
				}
			}
			for range 3 {
				stream.Topic("large").Publish(strings.Repeat("x", 600000), false)
				must(stream.Flush(ctx))
			}
			count = 0
			for item, err := range stream.Subscribe(ctx, streams.SubscribeOptions{Topics: []string{"large"}, FromOffset: 4, PollCooldown: time.Millisecond}) {
				must(err)
				if item.Offset != int64(4+count) {
					panic("paged offset changed")
				}
				count++
				if count == 3 {
					break
				}
			}
			must(c.SignalWorkflow(ctx, run.GetID(), "", "command", "finish"))
			var final int64
			must(run.Get(ctx, &final))
			if final != 7 {
				panic(fmt.Sprintf("final stream offset: %d", final))
			}
			for _, err := range stream.Subscribe(ctx, streams.SubscribeOptions{FromOffset: 7, PollCooldown: time.Millisecond}) {
				must(err)
				panic("terminal subscription yielded an item")
			}
			saveHistory(ctx, c, run.GetID(), run.GetRunID(), filepath.Join(dir, fmt.Sprintf("stream-cache-%d-first.json", size)))
			saveHistory(ctx, c, run.GetID(), nextRun, filepath.Join(dir, fmt.Sprintf("stream-cache-%d-next.json", size)))
		}()
	}
	replay(dir)
	fmt.Println("live workflow streams, rollover, truncation, paging, codecs and both cache modes passed")
}
func main() {
	runLive := flag.Bool("live", false, "run a Temporal dev server")
	dir := flag.String("history-dir", "../testdata", "saved histories")
	flag.Parse()
	if *runLive {
		live(*dir)
	} else {
		replay(*dir)
	}
}
