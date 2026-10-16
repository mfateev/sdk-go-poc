package main

import (
	"context"
	"fmt"
	"time"

	"github.com/mfateev/sdk-go-poc/example/dynamic"
	"github.com/mfateev/sdk-go-poc/example/order"
	"github.com/mfateev/sdk-go-poc/worker"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/testsuite"
	sdkworker "go.temporal.io/sdk/worker"
	sdkwf "go.temporal.io/sdk/workflow"
)

type clientPluginBase = client.PluginBase
type workerPluginBase = sdkworker.PluginBase
type registrationPlugin struct {
	clientPluginBase
	workerPluginBase
	dynamic bool
}

func (p *registrationPlugin) Name() string {
	if p.dynamic {
		return "native-dynamic"
	}
	return "native-named"
}
func (p *registrationPlugin) register(r interface {
	RegisterWorkflowWithOptions(any, sdkwf.RegisterOptions)
	RegisterDynamicWorkflow(any, sdkwf.DynamicRegisterOptions)
}) {
	if p.dynamic {
		r.RegisterDynamicWorkflow(dynamic.Workflow, sdkwf.DynamicRegisterOptions{})
	} else {
		r.RegisterWorkflowWithOptions(order.EchoWorkflow, sdkwf.RegisterOptions{Name: "plugin-echo"})
	}
}
func (p *registrationPlugin) StartWorker(ctx context.Context, o sdkworker.PluginStartWorkerOptions, next func(context.Context, sdkworker.PluginStartWorkerOptions) error) error {
	p.register(o.WorkerRegistry)
	return next(ctx, o)
}
func (p *registrationPlugin) ReplayWorkflow(ctx context.Context, o sdkworker.PluginReplayWorkflowOptions, next func(context.Context, sdkworker.PluginReplayWorkflowOptions) error) error {
	p.register(o.WorkflowReplayRegistry)
	return next(ctx, o)
}
func must(err error) {
	if err != nil {
		panic(err)
	}
}
func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	named := &registrationPlugin{}
	dynamicPlugin := &registrationPlugin{dynamic: true}
	server, err := testsuite.StartDevServer(ctx, testsuite.DevServerOptions{ClientOptions: &client.Options{Plugins: []client.Plugin{named}}})
	must(err)
	defer func() { must(server.Stop()) }()
	c := server.Client()
	for _, cache := range []int{1000, 0} {
		sdkworker.SetStickyWorkflowCacheSize(cache)
		queue := fmt.Sprintf("native-plugin-%d-%d", cache, time.Now().UnixNano())
		w := worker.New(c, queue, worker.Options{Plugins: []sdkworker.Plugin{dynamicPlugin}})
		must(w.Start())
		func() {
			defer w.Stop()
			for _, name := range []string{"plugin-echo", "runtime-dynamic"} {
				var args []any
				if name == "plugin-echo" {
					args = []any{[]byte("input")}
				} else {
					args = []any{"grape", "cherry"}
				}
				run, err := c.ExecuteWorkflow(ctx, client.StartWorkflowOptions{ID: queue + "-" + name, TaskQueue: queue}, name, args...)
				must(err)
				if name == "plugin-echo" {
					var got []byte
					must(run.Get(ctx, &got))
					if string(got) != "registered:input" {
						panic(fmt.Sprintf("named result %q", got))
					}
				} else {
					var got string
					must(run.Get(ctx, &got))
					if got != "runtime-dynamic - grape - cherry" {
						panic(fmt.Sprintf("dynamic result %q", got))
					}
				}
				history := &historypb.History{}
				iter := c.GetWorkflowHistory(ctx, run.GetID(), run.GetRunID(), false, 0)
				for iter.HasNext() {
					event, err := iter.Next()
					must(err)
					history.Events = append(history.Events, event)
				}
				replayer, err := worker.NewWorkflowReplayerWithOptions(worker.WorkflowReplayerOptions{Plugins: []sdkworker.Plugin{named, dynamicPlugin}})
				must(err)
				must(replayer.ReplayWorkflowHistory(nil, history))
			}
		}()
	}
	fmt.Println("plugin native named/dynamic execution and replay passed with cache 1000 and 0")
}
