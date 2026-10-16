package main

import (
	"context"
	"fmt"
	"reflect"
	"runtime"
	"time"

	"github.com/mfateev/sdk-go-poc/example/contextfree"
	"github.com/mfateev/sdk-go-poc/worker"
	commonpb "go.temporal.io/api/common/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/testsuite"
	sdkworker "go.temporal.io/sdk/worker"
	sdkworkflow "go.temporal.io/sdk/workflow"
	"google.golang.org/protobuf/proto"
)

func must(err error) {
	if err != nil {
		panic(err)
	}
}

type activityCommand struct {
	Name  string
	Input *commonpb.Payloads
}

func commands(h *historypb.History) []activityCommand {
	var result []activityCommand
	for _, event := range h.Events {
		if a := event.GetActivityTaskScheduledEventAttributes(); a != nil {
			// The SDK omits zero-argument Input; the bridge emits an empty
			// Payloads. Both decode to zero arguments.
			input := a.Input
			if input == nil {
				input = &commonpb.Payloads{}
			}
			result = append(result, activityCommand{a.ActivityType.Name, input})
		}
	}
	return result
}
func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	server, err := testsuite.StartDevServer(ctx, testsuite.DevServerOptions{})
	must(err)
	defer func() { must(server.Stop()) }()
	c := server.Client()
	for _, cache := range []int{1000, 0} {
		sdkworker.SetStickyWorkflowCacheSize(cache)
		for _, local := range []bool{false, true} {
			var baseline []activityCommand
			for _, native := range []bool{false, true} {
				queue := fmt.Sprintf("contextfree-%d-%t-%t-%d", cache, local, native, time.Now().UnixNano())
				var w sdkworker.Worker
				if native {
					w = worker.New(c, queue, worker.Options{})
					w.RegisterWorkflowWithOptions(contextfree.Workflow, sdkworkflow.RegisterOptions{Name: "contextfree"})
				} else {
					w = sdkworker.New(c, queue, sdkworker.Options{})
					w.RegisterWorkflowWithOptions(contextfree.Baseline, sdkworkflow.RegisterOptions{Name: "contextfree"})
				}
				contextfree.RegisterActivities(w)
				must(w.Start())
				func() {
					defer w.Stop()
					run, err := c.ExecuteWorkflow(ctx, client.StartWorkflowOptions{ID: queue, TaskQueue: queue}, "contextfree", local)
					must(err)
					var got []int
					must(run.Get(ctx, &got))
					if !reflect.DeepEqual(got, contextfree.Expected()) {
						panic(fmt.Sprintf("unexpected output: %v", got))
					}
					h := &historypb.History{}
					iter := c.GetWorkflowHistory(ctx, run.GetID(), run.GetRunID(), false, 0)
					for iter.HasNext() {
						event, err := iter.Next()
						must(err)
						h.Events = append(h.Events, event)
					}
					actual := commands(h)
					expectedCommands := 10
					if local {
						expectedCommands = 0
					}
					if len(actual) != expectedCommands {
						panic(fmt.Sprintf("activity count %d, want %d", len(actual), expectedCommands))
					}
					if !native {
						baseline = actual
						return
					}
					for i, a := range actual {
						if a.Name != baseline[i].Name || !proto.Equal(a.Input, baseline[i].Input) {
							panic(fmt.Sprintf("activity input differs from SDK at %d: %v / %v", i, a, baseline[i]))
						}
					}
					for _, procs := range []int{1, 2, 8} {
						runtime.GOMAXPROCS(procs)
						replayer := worker.NewWorkflowReplayer()
						replayer.RegisterWorkflowWithOptions(contextfree.Workflow, sdkworkflow.RegisterOptions{Name: "contextfree"})
						contextfree.RegisterActivities(replayer)
						must(replayer.ReplayWorkflowHistory(nil, h))
					}
				}()
			}
		}
	}
	fmt.Println("optional-context remote/local activities match SDK results and remote command inputs; native replay passed at GOMAXPROCS 1/2/8 with cache 1000 and 0")
}
