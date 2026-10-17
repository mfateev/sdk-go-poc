package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"runtime"
	"strings"
	"time"

	example "github.com/mfateev/sdk-go-poc/example/protobufvalues"
	"github.com/mfateev/sdk-go-poc/example/protobufvalues/model"
	"github.com/mfateev/sdk-go-poc/worker"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
	goWorker "go.temporal.io/sdk/worker"
	goWorkflow "go.temporal.io/sdk/workflow"
)

func check(err error) {
	if err != nil {
		panic(err)
	}
}

type hostEnvironment struct {
	goWorker.Worker
	env *testsuite.TestWorkflowEnvironment
}

func (h *hostEnvironment) RegisterWorkflowWithOptions(fn any, options goWorkflow.RegisterOptions) {
	h.env.RegisterWorkflowWithOptions(fn, options)
}
func register(w interface{ RegisterWorkflow(any) }, mode string) {
	w.RegisterWorkflow(example.Workflow)
	w.RegisterWorkflow(example.Child)
	w.RegisterWorkflow(example.UpstreamPayloads)
	w.RegisterWorkflow(example.UpstreamRandom)
	w.RegisterWorkflow(example.ResolverWorkflow)
	w.RegisterWorkflow(example.CustomMessageWorkflow)
	if mode == "binary" {
		check(worker.SetIsolateDataConverter(w, worker.DataConverterOptions{Factory: example.ConverterFactory, Config: []byte(mode)}))
	}
}
func Echo(_ context.Context, v *model.Record) (*model.Record, error) {
	return v, example.CheckRecord(v)
}
func Payloads(_ context.Context, first, second *commonpb.Payloads) (commonpb.Payloads, error) {
	if string(first.GetPayloads()[0].Data) != "input1" || string(first.GetPayloads()[0].Metadata["encoding"]) != "someencoding" || string(second.GetPayloads()[0].Data) != "input2" {
		return commonpb.Payloads{}, fmt.Errorf("protobuf payload arguments lost")
	}
	return commonpb.Payloads{Payloads: []*commonpb.Payload{{Data: []byte("result")}}}, nil
}
func Random(_ context.Context, v *commonpb.WorkflowType, blob *commonpb.DataBlob) (*commonpb.WorkflowType, error) {
	if v.Name != "input1" || blob.EncodingType.String() != "Proto3" {
		return nil, fmt.Errorf("random protobuf arguments lost")
	}
	return &commonpb.WorkflowType{Name: "result"}, nil
}
func run(mode, scenario string) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.SetWorkerOptions(goWorker.Options{DeadlockDetectionTimeout: 30 * time.Second})
	env.SetDataConverter(example.DataConverter(mode))
	w := worker.Wrap(&hostEnvironment{env: env})
	register(w, mode)
	env.RegisterActivityWithOptions(Echo, activity.RegisterOptions{Name: "Echo"})
	env.RegisterActivityWithOptions(Payloads, activity.RegisterOptions{Name: "Payloads"})
	env.RegisterActivityWithOptions(Random, activity.RegisterOptions{Name: "Random"})
	if scenario == "custom-message" {
		env.ExecuteWorkflow("CustomMessageWorkflow")
		panic("custom protobuf message accepted")
	}
	if scenario == "resolver" {
		env.ExecuteWorkflow("ResolverWorkflow")
		panic("custom resolver accepted")
	}
	if scenario == "payloads" {
		env.ExecuteWorkflow("UpstreamPayloads")
		check(env.GetWorkflowError())
		return
	}
	if scenario == "random" {
		env.ExecuteWorkflow("UpstreamRandom")
		check(env.GetWorkflowError())
		return
	}
	queries, updates := 0, 0
	if scenario == "values" {
		deadline := time.Now().Add(30 * time.Second)
		var verify func()
		verify = func() {
			// Child startup is asynchronous in the host test environment. Wait
			// for registration instead of assuming a virtual timer implies it.
			readyValue, err := env.QueryWorkflow("ready")
			if err != nil {
				if strings.Contains(err.Error(), "unknown queryType ready") && time.Now().Before(deadline) {
					env.RegisterDelayedCallback(verify, time.Millisecond)
					return
				}
				check(err)
			}
			var ready bool
			check(readyValue.Get(&ready))
			if !ready {
				panic("protobuf handlers not ready")
			}
			for i := range 2 {
				checkValue, err := env.QueryWorkflow("conversions")
				check(err)
				var valid bool
				check(checkValue.Get(&valid))
				if !valid {
					panic("scratch conversions failed")
				}
				for _, name := range []string{"value", "future", "echo"} {
					var value converter.EncodedValue
					var err error
					if name == "echo" {
						value, err = env.QueryWorkflow(name, example.Record())
					} else {
						value, err = env.QueryWorkflow(name)
					}
					check(err)
					var result *model.Record
					check(value.Get(&result))
					check(example.CheckRecord(result))
					queries++
				}
				for _, name := range []string{"mutate", "mutate-reflection"} {
					_, err := env.QueryWorkflow(name)
					if err == nil || !strings.Contains(err.Error(), "read-only") {
						panic(fmt.Sprintf("mutation accepted: %v", err))
					}
				}
				env.UpdateWorkflow("update", fmt.Sprintf("update-%d", i), &testsuite.TestUpdateCallback{OnAccept: func() {}, OnReject: func(err error) { panic(err) }, OnComplete: func(v any, err error) {
					check(err)
					var result *model.Record
					if raw, ok := v.(converter.RawValue); ok {
						check(example.DataConverter(mode).FromPayload(raw.Payload(), &result))
					} else {
						result = v.(*model.Record)
					}
					check(example.CheckRecord(result))
					updates++
					if updates == 2 {
						env.SignalWorkflow("record", example.Record())
					}
				}}, example.Record())
			}
		}
		env.RegisterDelayedCallback(verify, time.Millisecond)
	}
	env.ExecuteWorkflow("Workflow", example.Record(), scenario)
	err := env.GetWorkflowError()
	if scenario == "failure" {
		var app *temporal.ApplicationError
		if !errors.As(err, &app) {
			panic(err)
		}
		var value *model.Record
		check(app.Details(&value))
		check(example.CheckRecord(value))
		return
	}
	check(err)
	var value *model.Record
	check(env.GetWorkflowResult(&value))
	check(example.CheckRecord(value))
	if scenario == "values" && (queries != 6 || updates != 2) {
		panic(fmt.Sprintf("queries=%d updates=%d", queries, updates))
	}
}
func main() {
	mode := flag.String("encoding", "json", "protobuf encoding")
	scenario := flag.String("scenario", "values", "scenario")
	address := flag.String("address", "", "live server")
	recordDir := flag.String("record-dir", "/tmp/native-protobuf-histories", "history output")
	historyDir := flag.String("history-dir", "", "saved histories")
	cacheSize := flag.Int("cache-size", 1000, "workflow cache size")
	flag.Parse()
	if *scenario == "cache" {
		check(cacheReuse())
		return
	}
	if *address != "" {
		check(live(*address, *recordDir, *cacheSize))
		return
	}
	if *historyDir != "" {
		check(replayDirectory(*historyDir))
		return
	}
	for _, procs := range []int{1, 2, 8} {
		runtime.GOMAXPROCS(procs)
		run(*mode, *scenario)
	}
	fmt.Println("native protobuf APIs passed:", *mode, *scenario)
}
