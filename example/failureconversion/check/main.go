package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"path/filepath"
	"runtime"
	"strings"
	"sync/atomic"
	"time"

	contextsupport "github.com/mfateev/sdk-go-poc/example/contextpropagation/support"
	example "github.com/mfateev/sdk-go-poc/example/failureconversion"
	"github.com/mfateev/sdk-go-poc/example/failureconversion/support"
	"github.com/mfateev/sdk-go-poc/temporalbridge"
	"github.com/mfateev/sdk-go-poc/worker"
	failurepb "go.temporal.io/api/failure/v1"
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
func assertCustom(err error) {
	var custom support.CustomError
	if !errors.As(err, &custom) || custom.Detail != support.Precise {
		panic(fmt.Sprintf("custom error lost: %v", err))
	}
}

type hostEnvironment struct {
	goWorker.Worker
	env     *testsuite.TestWorkflowEnvironment
	factory temporalbridge.Factory
}

type observingConverter struct {
	converter.FailureConverter
	contextConversions atomic.Int32
}

func (fc *observingConverter) FailureToError(f *failurepb.Failure) error {
	if info := f.GetApplicationFailureInfo(); info != nil && info.GetType() == support.ErrorType {
		if details := info.GetDetails().GetPayloads(); len(details) == 1 && string(details[0].GetMetadata()[converter.MetadataEncoding]) == contextsupport.Encoding {
			fc.contextConversions.Add(1)
		}
	}
	return fc.FailureConverter.FailureToError(f)
}

func (h *hostEnvironment) RegisterWorkflowWithOptions(fn any, o goWorkflow.RegisterOptions) {
	h.factory = fn.(temporalbridge.Factory)
	h.env.RegisterWorkflowWithOptions(fn, o)
}

func run(scenario string) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.SetWorkerOptions(goWorker.Options{DeadlockDetectionTimeout: 10 * time.Second})
	dc := converter.GetDefaultDataConverter()
	if scenario == "updates-context" {
		dc = contextsupport.CompatibleConverter()
		env.SetDataConverter(dc)
	}
	fc := &observingConverter{FailureConverter: support.New(dc)}
	env.SetFailureConverter(fc)
	env.SetLastError(support.CustomError{Detail: support.Precise})
	host := &hostEnvironment{env: env}
	w := worker.Wrap(host)
	w.RegisterWorkflow(example.Workflow)
	w.RegisterWorkflow(example.Child)
	config := []byte(support.ErrorType)
	if scenario == "factory-error" || scenario == "nil-converter" {
		config = []byte(scenario)
	}
	check(worker.SetIsolateFailureConverter(w, worker.FailureConverterOptions{Factory: support.Factory, Config: config}))
	config[0] = 'x'
	var calls atomic.Int32
	env.RegisterActivityWithOptions(func(context.Context) error { calls.Add(1); return support.CustomError{Detail: support.Precise} }, activity.RegisterOptions{Name: "Fail"})
	updates, queries := 0, 0
	if scenario == "updates" || scenario == "updates-context" {
		env.RegisterDelayedCallback(func() {
			for i := range 2 {
				value, err := env.QueryWorkflow("last")
				check(err)
				var detail int64
				check(value.Get(&detail))
				if detail != support.Precise {
					panic("last error detail")
				}
				queries++
				env.UpdateWorkflow("fail", fmt.Sprintf("reject-%d", i), &testsuite.TestUpdateCallback{
					OnReject:   func(err error) { assertCustom(err); updates++ },
					OnAccept:   func() { panic("validator accepted") },
					OnComplete: func(any, error) { panic("validator completed") },
				}, true)
				env.UpdateWorkflow("fail", fmt.Sprintf("handler-%d", i), &testsuite.TestUpdateCallback{
					OnReject: func(err error) { panic(err) }, OnAccept: func() {},
					OnComplete: func(_ any, err error) { assertCustom(err); updates++ },
				}, false)
			}
		}, 100*time.Millisecond)
	}
	env.ExecuteWorkflow("Workflow", scenario)
	if !env.IsWorkflowCompleted() {
		panic("workflow did not complete")
	}
	err := env.GetWorkflowError()
	switch scenario {
	case "updates", "updates-context":
		check(err)
		if updates != 4 || queries != 2 {
			panic(fmt.Sprintf("updates=%d queries=%d", updates, queries))
		}
		if scenario == "updates-context" && fc.contextConversions.Load() != 4 {
			panic("failure converter did not receive registration-context serializer")
		}
	case "factory-error", "nil-converter":
		if err == nil || !strings.Contains(err.Error(), "failure converter") {
			panic(fmt.Sprintf("factory failure missing: %v", err))
		}
	case "fallback":
		var app *temporal.ApplicationError
		if !errors.As(err, &app) || app.Type() != "Ordinary" || !app.NonRetryable() {
			panic(fmt.Sprintf("fallback lost: %v", err))
		}
		var detail int64
		check(app.Details(&detail))
		if detail != support.Precise {
			panic("fallback detail lost")
		}
	default:
		assertCustom(err)
		if (scenario == "activity" || scenario == "local") && calls.Load() != 1 {
			panic(fmt.Sprintf("activity retried %d times", calls.Load()))
		}
	}
	check(worker.SetIsolateFailureConverter(w, worker.FailureConverterOptions{}))
	if host.factory.ResolveFailureConverter().Factory.Name() != "" {
		panic("failure converter reset failed")
	}
}

func main() {
	scenario := flag.String("scenario", "activity", "failure conversion scenario")
	address := flag.String("address", "", "optional live Temporal server")
	recordDir := flag.String("record-dir", "/tmp/native-failure-histories", "directory for live histories")
	historyDir := flag.String("history-dir", "", "saved failure histories")
	cacheSize := flag.Int("cache-size", 1000, "live workflow cache size")
	flag.Parse()
	if *address != "" {
		check(live(*address, *recordDir, *cacheSize))
		return
	}
	if *historyDir != "" {
		files, err := filepath.Glob(filepath.Join(*historyDir, "*.json"))
		check(err)
		if len(files) == 0 {
			panic("no saved failure histories")
		}
		for _, file := range files {
			for _, procs := range []int{1, 2, 8} {
				runtime.GOMAXPROCS(procs)
				check(replay(file))
			}
		}
		fmt.Println("saved custom failure histories replayed")
		return
	}
	for _, procs := range []int{1, 2, 8} {
		runtime.GOMAXPROCS(procs)
		run(*scenario)
	}
	fmt.Println("native custom failure conversion passed:", *scenario)
}
