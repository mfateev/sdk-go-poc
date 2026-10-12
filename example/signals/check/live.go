package main

import (
	"context"
	"fmt"
	"slices"
	"time"

	"github.com/mfateev/sdk-go-poc/example/signals"
	"github.com/mfateev/sdk-go-poc/worker"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/testsuite"
)

func checkLive() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	server, err := testsuite.StartDevServer(ctx, testsuite.DevServerOptions{})
	must(err)
	defer func() { must(server.Stop()) }()
	c := server.Client()
	checkLiveCorrupt(ctx, c)
	for _, mode := range []string{"wake", "late", "continue"} {
		queue := fmt.Sprintf("proactive-signals-%s-%d", mode, time.Now().UnixNano())
		w := worker.New(c, queue, worker.Options{})
		w.RegisterWorkflow(signals.DrainWorkflow)
		w.RegisterWorkflow(signals.ContinueWorkflow)
		name := "DrainWorkflow"
		args := []any{mode}
		if mode == "continue" {
			name = "ContinueWorkflow"
			args = []any{[]int(nil)}
		}
		run, err := c.ExecuteWorkflow(ctx, client.StartWorkflowOptions{ID: queue, TaskQueue: queue}, name, args...)
		must(err)
		var want []int
		for value := range 17 {
			must(c.SignalWorkflow(ctx, run.GetID(), run.GetRunID(), "number", value))
			want = append(want, value)
		}
		if mode != "continue" {
			for value := range 11 {
				must(c.SignalWorkflow(ctx, run.GetID(), run.GetRunID(), "other", 100+value))
				want = append(want, 100+value)
			}
		}
		if mode == "late" {
			must(c.SignalWorkflow(ctx, run.GetID(), run.GetRunID(), "start", true))
		}
		// Start polling only after the full batch is durable in history.
		must(w.Start())
		var got []int
		err = run.Get(ctx, &got)
		w.Stop()
		must(err)
		if !slices.Equal(got, want) {
			panic(fmt.Sprintf("live %s lost signals: got=%v want=%v", mode, got, want))
		}
		iterator := c.GetWorkflowHistory(ctx, run.GetID(), run.GetRunID(), false, enumspb.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)
		history := new(historypb.History)
		for iterator.HasNext() {
			event, err := iterator.Next()
			must(err)
			history.Events = append(history.Events, event)
		}
		replayer := worker.NewWorkflowReplayer()
		replayer.RegisterWorkflow(signals.DrainWorkflow)
		replayer.RegisterWorkflow(signals.ContinueWorkflow)
		must(replayer.ReplayWorkflowHistory(nil, history))
	}
	fmt.Println("live proactive signal drains, Continue-As-New carryover and recorded history replay passed")
}
