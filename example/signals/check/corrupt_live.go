package main

import (
	"context"
	"fmt"
	"time"

	"github.com/mfateev/sdk-go-poc/example/signals"
	"github.com/mfateev/sdk-go-poc/worker"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/sdk/client"
)

func checkLiveCorrupt(ctx context.Context, c client.Client) {
	for _, intercepted := range []bool{false, true} {
		queue := fmt.Sprintf("corrupt-signals-%t-%d", intercepted, time.Now().UnixNano())
		w := worker.New(c, queue, worker.Options{})
		w.RegisterWorkflow(signals.CorruptWorkflow)
		if intercepted {
			must(worker.SetIsolateInterceptors(w, worker.InterceptorOptions{Factory: signals.NewInterceptors}))
		}
		run, err := c.ExecuteWorkflow(ctx, client.StartWorkflowOptions{ID: queue, TaskQueue: queue}, "CorruptWorkflow", "receive")
		must(err)
		must(c.SignalWorkflow(ctx, run.GetID(), run.GetRunID(), "message", "wrong"))
		must(c.SignalWorkflow(ctx, run.GetID(), run.GetRunID(), "message", signals.Message{Text: "valid"}))
		must(w.Start())
		var got []signals.Message
		err = run.Get(ctx, &got)
		w.Stop()
		must(err)
		if len(got) != 1 || got[0].Text != "valid" {
			panic(fmt.Sprintf("live corrupt values delivered: %v", got))
		}
		iterator := c.GetWorkflowHistory(ctx, run.GetID(), run.GetRunID(), false, enumspb.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)
		history := new(historypb.History)
		for iterator.HasNext() {
			event, err := iterator.Next()
			must(err)
			history.Events = append(history.Events, event)
		}
		replayer := worker.NewWorkflowReplayer()
		replayer.RegisterWorkflow(signals.CorruptWorkflow)
		if intercepted {
			must(worker.SetIsolateInterceptors(replayer, worker.InterceptorOptions{Factory: signals.NewInterceptors}))
		}
		must(replayer.ReplayWorkflowHistory(nil, history))
	}
	fmt.Println("live corrupt signal skipping and recorded history replay passed")
}
