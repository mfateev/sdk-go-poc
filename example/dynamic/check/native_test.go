package main

import (
	"context"
	"fmt"
	"runtime"
	"testing"
	"time"

	"github.com/mfateev/sdk-go-poc/example/converter/custom"
	"github.com/mfateev/sdk-go-poc/example/dynamic"
	"github.com/mfateev/sdk-go-poc/testsuite"
	"github.com/mfateev/sdk-go-poc/worker"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/converter"
	sdkwf "go.temporal.io/sdk/workflow"
)

func dynamicActivity(ctx context.Context, args converter.EncodedValues) (string, error) {
	var first, second string
	if err := args.Get(&first, &second); err != nil {
		return "", err
	}
	return activity.GetInfo(ctx).ActivityType.Name + " - " + first + " - " + second, nil
}

func TestDynamicWorkflows(t *testing.T) {
	for _, procs := range []int{1, 2, 8} {
		t.Run(fmt.Sprint(procs), func(t *testing.T) {
			previous := runtime.GOMAXPROCS(procs)
			defer runtime.GOMAXPROCS(previous)
			var suite testsuite.WorkflowTestSuite
			env := suite.NewTestWorkflowEnvironment(t)
			env.RegisterDynamicActivity(dynamicActivity, activity.DynamicRegisterOptions{})
			env.RegisterDynamicWorkflow(dynamic.Workflow, sdkwf.DynamicRegisterOptions{})
			env.ExecuteWorkflow("dynamic-activity", "grape", "cherry")
			require.NoError(t, env.GetWorkflowError())
			var result string
			require.NoError(t, env.GetWorkflowResult(&result))
			require.Equal(t, "random-activity-name - grape - cherry", result)
		})
	}
}

func TestDynamicWorkflowChildEmptyAndNamedPrecedence(t *testing.T) {
	for _, name := range []string{"parent", "empty", "named"} {
		t.Run(name, func(t *testing.T) {
			var suite testsuite.WorkflowTestSuite
			env := suite.NewTestWorkflowEnvironment(t)
			env.RegisterDynamicWorkflow(dynamic.Workflow, sdkwf.DynamicRegisterOptions{})
			env.RegisterWorkflowWithOptions(dynamic.Named, sdkwf.RegisterOptions{Name: "named"})
			switch name {
			case "empty":
				env.ExecuteWorkflow(name)
			case "named":
				env.ExecuteWorkflow(name, "x")
			default:
				env.ExecuteWorkflow(name, "grape", "cherry")
			}
			require.NoError(t, env.GetWorkflowError())
			var result string
			require.NoError(t, env.GetWorkflowResult(&result))
			require.Equal(t, map[string]string{"parent": "child-dynamic - grape - cherry", "empty": "empty", "named": "child:x"}[name], result)
		})
	}
}

func TestNativeDynamicQueryAndUpdateHandlers(t *testing.T) {
	for _, configured := range []bool{false, true} {
		for _, mode := range []string{"normal", "late"} {
			t.Run(fmt.Sprintf("%t/%s", configured, mode), func(t *testing.T) {
				var suite testsuite.WorkflowTestSuite
				env := suite.NewTestWorkflowEnvironment(t)
				env.RegisterDynamicWorkflow(dynamic.Handlers, sdkwf.DynamicRegisterOptions{})
				hostDC := converter.GetDefaultDataConverter()
				if configured {
					dc, err := custom.New([]byte("dynamic"))
					require.NoError(t, err)
					require.NoError(t, env.SetIsolateInterceptors(worker.InterceptorOptions{Factory: dynamic.NewInterceptors}))
					hostDC = dc
					env.SetDataConverter(dc)
					require.NoError(t, env.SetIsolateDataConverter(worker.DataConverterOptions{Factory: custom.New, Config: []byte("dynamic")}))
				}
				var results []int64
				rejected, named := 0, 0
				send := func(name string, args ...any) {
					env.UpdateWorkflow(name, "id-"+name, &testsuite.TestUpdateCallback{
						OnReject: func(err error) { require.Error(t, err); rejected++ },
						OnAccept: func() {},
						OnComplete: func(result any, err error) {
							require.NoError(t, err)
							if name == "named" {
								require.Equal(t, "named", testsuite.DecodeResult[string](result, hostDC))
								named++
								return
							}
							results = append(results, testsuite.DecodeResult[int64](result, hostDC))
						},
					}, args...)
				}
				env.RegisterDelayedCallback(func() {
					send("first", int64(2))
					send("second", int64(3))
					send("reject", int64(-1))
					send("named")
				}, map[string]time.Duration{"normal": time.Second, "late": 0}[mode])
				env.RegisterDelayedCallback(func() {
					require.Equal(t, []int64{9007199254740995, 9007199254740998}, results)
					require.Equal(t, 1, rejected)
					require.Equal(t, 1, named)
					result, err := env.QueryWorkflow("arbitrary", "prefix")
					require.NoError(t, err)
					var text string
					require.NoError(t, result.Get(&text))
					require.Equal(t, "arbitrary:prefix:9007199254740998", text)
					result, err = env.QueryWorkflow("named")
					require.NoError(t, err)
					require.NoError(t, result.Get(&text))
					require.Equal(t, "named", text)
					_, err = env.QueryWorkflow("mutate")
					require.Error(t, err)
					_, err = env.QueryWorkflow("panic")
					require.Error(t, err)
					_, err = env.QueryWorkflow("__reserved")
					require.Error(t, err)
					send("mutate", int64(1))
					send("panic", int64(1))
					send("bad-type", "bad")
					send("__reserved", int64(1))
				}, 2*time.Minute)
				env.RegisterDelayedCallback(func() {
					require.Equal(t, 5, rejected)
					env.SignalWorkflow("finish", []byte("done"))
				}, 3*time.Minute)
				env.ExecuteWorkflow("handler-workflow", mode)
				require.NoError(t, env.GetWorkflowError())
				var result int64
				require.NoError(t, env.GetWorkflowResult(&result))
				require.Equal(t, int64(9007199254740998), result)
				query, err := env.QueryWorkflow("after-completion")
				require.NoError(t, err)
				var text string
				require.NoError(t, query.Get(&text))
				require.Equal(t, "after-completion:9007199254740998", text)
			})
		}
	}
}

func TestDynamicWorkflowDuplicateRegistration(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment(t)
	env.RegisterDynamicWorkflow(dynamic.Workflow, sdkwf.DynamicRegisterOptions{})
	require.PanicsWithValue(t, "dynamic workflow already registered", func() { env.RegisterDynamicWorkflow(dynamic.Workflow, sdkwf.DynamicRegisterOptions{}) })
	r := worker.NewWorkflowReplayer()
	r.RegisterDynamicWorkflow(dynamic.Workflow, sdkwf.DynamicRegisterOptions{})
	require.PanicsWithValue(t, "dynamic workflow already registered", func() { r.RegisterDynamicWorkflow(dynamic.Workflow, sdkwf.DynamicRegisterOptions{}) })
}

func TestDynamicHandlerReplacementAndErrorOnlyUpdate(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment(t)
	completed, rejected := false, false
	env.RegisterDelayedCallback(func() {
		env.UpdateWorkflow("replacement", "id", &testsuite.TestUpdateCallback{
			OnReject: func(err error) { t.Errorf("unexpected rejection: %v", err) },
			OnAccept: func() {},
			OnComplete: func(value any, err error) {
				require.NoError(t, err)
				require.Nil(t, testsuite.DecodeResult[*string](value))
				completed = true
			},
		})
		env.UpdateWorkflow("reject", "reject", &testsuite.TestUpdateCallback{
			OnReject:   func(err error) { require.ErrorContains(t, err, "rejected by replacement"); rejected = true },
			OnAccept:   func() { t.Error("rejected update accepted") },
			OnComplete: func(any, error) { t.Error("rejected update completed") },
		})
	}, time.Second)
	env.RegisterDelayedCallback(func() { env.SignalWorkflow("finish", []byte("done")) }, time.Minute)
	env.ExecuteWorkflow(dynamic.Replacement)
	require.NoError(t, env.GetWorkflowError())
	require.True(t, completed)
	require.True(t, rejected)
	query, err := env.QueryWorkflow("arbitrary")
	require.NoError(t, err)
	var result string
	require.NoError(t, query.Get(&result))
	require.Equal(t, "arbitrary:replacement", result)
	query, err = env.QueryWorkflow("")
	require.NoError(t, err)
	require.NoError(t, query.Get(&result))
	require.Equal(t, "empty named query", result)
}
