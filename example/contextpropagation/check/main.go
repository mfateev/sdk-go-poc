package main

import (
	"context"
	"flag"
	"fmt"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	example "github.com/mfateev/sdk-go-poc/example/contextpropagation"
	"github.com/mfateev/sdk-go-poc/example/contextpropagation/support"
	"github.com/mfateev/sdk-go-poc/temporalbridge"
	"github.com/mfateev/sdk-go-poc/worker"
	"github.com/stretchr/testify/mock"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/testsuite"
	goWorker "go.temporal.io/sdk/worker"
	goWorkflow "go.temporal.io/sdk/workflow"
)

type hostPropagator struct{}

func (hostPropagator) Inject(ctx context.Context, h goWorkflow.HeaderWriter) error {
	p, err := converter.GetDefaultDataConverter().ToPayload(ctx.Value(support.HeaderKey))
	if err != nil {
		return err
	}
	h.Set(string(support.HeaderKey), p)
	return nil
}
func (hostPropagator) Extract(ctx context.Context, h goWorkflow.HeaderReader) (context.Context, error) {
	p, ok := h.Get(string(support.HeaderKey))
	if !ok {
		return ctx, nil
	}
	var value string
	if err := converter.GetDefaultDataConverter().FromPayload(p, &value); err != nil {
		return nil, err
	}
	return context.WithValue(ctx, support.HeaderKey, value), nil
}
func (hostPropagator) InjectFromWorkflow(ctx goWorkflow.Context, h goWorkflow.HeaderWriter) error {
	p, err := converter.GetDefaultDataConverter().ToPayload(ctx.Value(support.HeaderKey))
	if err != nil {
		return err
	}
	h.Set(string(support.HeaderKey), p)
	return nil
}
func (hostPropagator) ExtractToWorkflow(ctx goWorkflow.Context, h goWorkflow.HeaderReader) (goWorkflow.Context, error) {
	p, ok := h.Get(string(support.HeaderKey))
	if !ok {
		return ctx, nil
	}
	var value string
	if err := converter.GetDefaultDataConverter().FromPayload(p, &value); err != nil {
		return nil, err
	}
	return goWorkflow.WithValue(ctx, support.HeaderKey, value), nil
}

func ContextActivity(ctx context.Context) (string, error) {
	v, _ := ctx.Value(support.HeaderKey).(string)
	if v == "" {
		return "", fmt.Errorf("missing activity context")
	}
	return v, nil
}
func Echo(_ context.Context, input string) (string, error)  { return input, nil }
func Hello(_ context.Context, input string) (string, error) { return "mock_" + input, nil }
func MockChild(goWorkflow.Context) error                    { return nil }

type hostEnvironment struct {
	goWorker.Worker
	env     *testsuite.TestWorkflowEnvironment
	factory temporalbridge.Factory
}

func (h *hostEnvironment) RegisterWorkflowWithOptions(fn any, o goWorkflow.RegisterOptions) {
	h.factory = fn.(temporalbridge.Factory)
	h.env.RegisterWorkflowWithOptions(fn, o)
}

type assertions struct{}

func (assertions) Logf(string, ...any)       {}
func (assertions) Errorf(f string, a ...any) { panic(fmt.Sprintf(f, a...)) }
func (assertions) FailNow()                  { panic("mock failure") }
func check(err error) {
	if err != nil {
		panic(err)
	}
}

func run(scenario string) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	// Native task execution includes isolate initialization and suspension;
	// give it the same budget as the live worker's workflow task timeout.
	env.SetWorkerOptions(goWorker.Options{DeadlockDetectionTimeout: 10 * time.Second})
	env.SetDataConverter(support.CompatibleConverter())
	env.SetContextPropagators([]goWorkflow.ContextPropagator{hostPropagator{}})
	p, err := converter.GetDefaultDataConverter().ToPayload("test-data")
	check(err)
	env.SetHeader(&commonpb.Header{Fields: map[string]*commonpb.Payload{string(support.HeaderKey): p}})
	host := &hostEnvironment{env: env}
	w := worker.Wrap(host)
	w.RegisterWorkflow(example.Workflow)
	w.RegisterWorkflow(example.Child)
	config := []byte("normal")
	if strings.HasSuffix(scenario, "error") || scenario == "nil-propagator" {
		config = []byte(scenario)
	}
	check(worker.SetIsolateContextPropagators(w, worker.ContextPropagatorOptions{Factory: support.Propagators, Config: config}))
	config[0] = 'x' // Configuration must have been copied after registration.
	if scenario != "per-context-only" {
		check(worker.SetIsolateDataConverter(w, worker.DataConverterOptions{Factory: support.DataConverter}))
	}
	env.RegisterActivity(ContextActivity)
	env.RegisterActivity(Echo)
	env.RegisterActivity(Hello)
	headerSeen := false
	queryCount := 0
	updateCount := 0
	if scenario == "mock-header" {
		env.RegisterWorkflow(MockChild)
		env.OnWorkflow(MockChild, mock.MatchedBy(func(ctx goWorkflow.Context) bool {
			headerSeen = ctx.Value(support.HeaderKey) == "test-data"
			return headerSeen
		})).Return(nil).Once()
	}
	if scenario == "per-context-converter" || scenario == "per-context-only" {
		env.OnActivity(Hello, mock.Anything, mock.Anything).Return(func(_ context.Context, input string) (string, error) { return "mock_" + input, nil }).Times(3)
	}
	if !strings.HasSuffix(scenario, "error") && scenario != "nil-propagator" {
		env.SetOnActivityCompletedListener(func(info *activity.Info, value converter.EncodedValue, err error) {
			if err != nil || info.ActivityType.Name != "Echo" {
				return
			}
			var result string
			check(value.Get(&result))
			if result != "query-result" {
				return
			}
			env.RegisterDelayedCallback(func() {
				for range 2 {
					value, err := env.QueryWorkflow("value")
					check(err)
					var result string
					check(value.Get(&result))
					if result != "test-data" {
						panic("query context value changed")
					}
					value, err = env.QueryWorkflow("future")
					check(err)
					check(value.Get(&result))
					if result != "query-result" {
						panic("query future converter changed")
					}
					custom, err := env.QueryWorkflow("custom")
					check(err)
					var customRaw converter.RawValue
					check(custom.Get(&customRaw))
					if string(customRaw.Payload().Metadata[converter.MetadataEncoding]) != support.Encoding {
						panic("query lost per-context converter")
					}
					check(custom.Get(&result))
					if result != "query-custom" {
						panic("custom query result")
					}
					queryCount++
					env.UpdateWorkflow("value", fmt.Sprintf("update-%d", queryCount), &testsuite.TestUpdateCallback{
						OnReject: func(err error) { panic(err) },
						OnAccept: func() {},
						OnComplete: func(value any, err error) {
							check(err)
							raw, ok := value.(converter.RawValue)
							if !ok {
								panic(fmt.Sprintf("update result type %T", value))
							}
							var got string
							check(support.CompatibleConverter().FromPayload(raw.Payload(), &got))
							if got != "test-data" {
								panic("update context lost")
							}
							updateCount++
						},
					})
					env.UpdateWorkflow("custom", fmt.Sprintf("custom-%d", queryCount), &testsuite.TestUpdateCallback{
						OnReject: func(err error) { panic(err) }, OnAccept: func() {},
						OnComplete: func(value any, err error) {
							check(err)
							raw, ok := value.(converter.RawValue)
							if !ok || string(raw.Payload().Metadata[converter.MetadataEncoding]) != support.Encoding {
								panic("update lost per-context converter")
							}
							updateCount++
						},
					})
				}
			}, 100*time.Millisecond)
		})
	}
	env.ExecuteWorkflow("Workflow", scenario)
	if !env.IsWorkflowCompleted() {
		panic("workflow did not complete")
	}
	if scenario == "factory-error" || scenario == "nil-propagator" || scenario == "extract-error" {
		if err := env.GetWorkflowError(); err == nil || !strings.Contains(err.Error(), "propagator") {
			panic(fmt.Sprintf("missing propagator failure %v", err))
		}
		return
	}
	check(env.GetWorkflowError())
	var result string
	check(env.GetWorkflowResult(&result))
	if result != "ok" {
		panic("wrong workflow result")
	}
	if queryCount != 2 || updateCount != 4 {
		panic("queries were not called")
	}
	if scenario == "mock-header" && !headerSeen {
		panic("mock did not see propagated header")
	}
	if !env.AssertExpectations(assertions{}) {
		panic("mock expectations")
	}
	check(worker.SetIsolateContextPropagators(w, worker.ContextPropagatorOptions{}))
	if host.factory.ResolveContextPropagators().Factory.Name() != "" {
		panic("propagator reset failed")
	}
}
func main() {
	scenario := flag.String("scenario", "header-root", "context test scenario")
	address := flag.String("address", "", "optional live Temporal server")
	historyDir := flag.String("history-dir", "", "saved context histories to replay")
	recordDir := flag.String("record-dir", "/tmp/native-context-histories", "directory for live histories")
	cacheSize := flag.Int("cache-size", 1000, "live workflow cache size")
	flag.Parse()
	if *address != "" {
		check(checkLive(*address, *recordDir, *cacheSize))
		return
	}
	if *historyDir != "" {
		files, err := filepath.Glob(filepath.Join(*historyDir, "*.json"))
		check(err)
		if len(files) == 0 {
			panic("no context histories")
		}
		for _, file := range files {
			for _, procs := range []int{1, 2, 8} {
				runtime.GOMAXPROCS(procs)
				check(replay(file))
			}
		}
		fmt.Println("saved native context histories replayed")
		return
	}
	for _, procs := range []int{1, 2, 8} {
		runtime.GOMAXPROCS(procs)
		run(*scenario)
	}
	fmt.Println("native context propagation passed:", *scenario)
}
