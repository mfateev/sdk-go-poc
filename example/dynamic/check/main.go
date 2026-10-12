package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/mfateev/sdk-go-poc/example/converter/custom"
	"github.com/mfateev/sdk-go-poc/example/dynamic"
	"github.com/mfateev/sdk-go-poc/worker"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/testsuite"
	sdkworker "go.temporal.io/sdk/worker"
	sdkwf "go.temporal.io/sdk/workflow"
	"google.golang.org/protobuf/encoding/protojson"
)

func must(err error) {
	if err != nil {
		panic(err)
	}
}
func dataConverter() converter.DataConverter {
	dc, err := custom.New([]byte("dynamic"))
	must(err)
	return converter.NewCodecDataConverter(dc, converter.NewZlibCodec(converter.ZlibCodecOptions{AlwaysEncode: true}))
}
func configure(target any) {
	must(worker.SetIsolateDataConverter(target, worker.DataConverterOptions{Factory: custom.New, Config: []byte("dynamic")}))
	must(worker.SetIsolateInterceptors(target, worker.InterceptorOptions{Factory: dynamic.NewInterceptors}))
}
func replay(dir string) {
	paths, err := filepath.Glob(filepath.Join(dir, "*.json"))
	must(err)
	if len(paths) == 0 {
		panic("no dynamic histories found")
	}
	for _, path := range paths {
		r, err := worker.NewWorkflowReplayerWithOptions(worker.WorkflowReplayerOptions{DataConverter: dataConverter()})
		must(err)
		r.RegisterDynamicWorkflow(dynamic.Handlers, sdkwf.DynamicRegisterOptions{})
		configure(r)
		must(r.ReplayWorkflowHistoryFromJSONFile(nil, path))
	}
	fmt.Println("dynamic saved history replay passed")
}
func live(dir string) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	server, err := testsuite.StartDevServer(ctx, testsuite.DevServerOptions{ClientOptions: &client.Options{DataConverter: dataConverter()}})
	must(err)
	defer func() { must(server.Stop()) }()
	c := server.Client()
	for _, size := range []int{1000, 0} {
		sdkworker.SetStickyWorkflowCacheSize(size)
		queue := fmt.Sprintf("dynamic-%d-%d", size, time.Now().UnixNano())
		w := worker.New(c, queue, worker.Options{})
		w.RegisterDynamicWorkflow(dynamic.Handlers, sdkwf.DynamicRegisterOptions{})
		configure(w)
		must(w.Start())
		func() {
			defer w.Stop()
			run, err := c.ExecuteWorkflow(ctx, client.StartWorkflowOptions{ID: queue, TaskQueue: queue}, "runtime-chosen-name", "normal")
			must(err)
			handle, err := c.UpdateWorkflow(ctx, client.UpdateWorkflowOptions{WorkflowID: run.GetID(), RunID: run.GetRunID(), UpdateName: "first", UpdateID: "first", Args: []any{int64(2)}, WaitForStage: client.WorkflowUpdateStageCompleted})
			must(err)
			var value int64
			must(handle.Get(ctx, &value))
			if value != 9007199254740995 {
				panic(fmt.Sprintf("dynamic update result %d", value))
			}
			handle, err = c.UpdateWorkflow(ctx, client.UpdateWorkflowOptions{WorkflowID: run.GetID(), RunID: run.GetRunID(), UpdateName: "reject", UpdateID: "reject", Args: []any{int64(-1)}, WaitForStage: client.WorkflowUpdateStageCompleted})
			if err == nil && handle.Get(ctx, &value) == nil {
				panic("negative update accepted")
			}
			for name, want := range map[string]string{"arbitrary": "arbitrary:9007199254740995", "named": "named", "intercepted": "query intercepted"} {
				result, err := c.QueryWorkflow(ctx, run.GetID(), run.GetRunID(), name)
				must(err)
				var got string
				must(result.Get(&got))
				if got != want {
					panic(fmt.Sprintf("query %s: %s", name, got))
				}
			}
			must(c.SignalWorkflow(ctx, run.GetID(), run.GetRunID(), "finish", []byte("done")))
			must(run.Get(ctx, &value))
			if value != 9007199254740995 {
				panic("dynamic workflow result changed")
			}
			result, err := c.QueryWorkflow(ctx, run.GetID(), run.GetRunID(), "after-completion")
			must(err)
			var text string
			must(result.Get(&text))
			if text != "after-completion:9007199254740995" {
				panic(text)
			}
			iterator := c.GetWorkflowHistory(ctx, run.GetID(), run.GetRunID(), false, enumspb.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)
			history := new(historypb.History)
			for iterator.HasNext() {
				event, err := iterator.Next()
				must(err)
				history.Events = append(history.Events, event)
			}
			raw, err := protojson.Marshal(history)
			must(err)
			must(os.MkdirAll(dir, 0755))
			must(os.WriteFile(filepath.Join(dir, fmt.Sprintf("handlers-cache-%d.json", size)), raw, 0644))
		}()
	}
	replay(dir)
	fmt.Println("live dynamic handlers, codecs, cache eviction and replay passed")
}
func main() {
	runLive := flag.Bool("live", false, "run a local Temporal dev server")
	dir := flag.String("history-dir", "../testdata", "saved replay histories")
	flag.Parse()
	if *runLive {
		live(*dir)
	} else {
		replay(*dir)
	}
}
