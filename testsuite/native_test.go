package testsuite_test

import (
	"context"
	"errors"
	"fmt"
	"isolate"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/mfateev/sdk-go-poc/testref"
	"github.com/mfateev/sdk-go-poc/testsuite"
	"github.com/mfateev/sdk-go-poc/workflow"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/temporal"
	goWorker "go.temporal.io/sdk/worker"
	goWorkflow "go.temporal.io/sdk/workflow"
)

func hello(_ context.Context, input string) (string, error) { return "hello:" + input, nil }

//go:isolate
func nativeChild(ctx context.Context, input string) (string, error) {
	if err := workflow.SetQueryHandler(ctx, "state", func() (string, error) { return input, nil }); err != nil {
		return "", err
	}
	return "child:" + input, nil
}

//go:isolate
func nativeParent(ctx context.Context, input string) (string, error) {
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{StartToCloseTimeout: time.Minute})
	var activity, child string
	if err := workflow.ExecuteActivity(ctx, hello, input).Get(ctx, &activity); err != nil {
		return "", err
	}
	if err := workflow.ExecuteChildWorkflow(ctx, nativeChild, input).Get(ctx, &child); err != nil {
		return "", err
	}
	return activity + "/" + child, nil
}

//go:isolate
func replacementChild(ctx context.Context, input string) (string, error) {
	time.Sleep(2 * time.Minute)
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{StartToCloseTimeout: time.Minute})
	var result string
	err := workflow.ExecuteActivity(ctx, hello, input).Get(ctx, &result)
	return "replacement:" + result, err
}

func TestNativeWorkflowMocksListenersAndAliases(t *testing.T) {
	for _, procs := range []int{1, 2, 8} {
		t.Run(fmt.Sprint(procs), func(t *testing.T) {
			previous := runtime.GOMAXPROCS(procs)
			defer runtime.GOMAXPROCS(previous)
			var suite testsuite.WorkflowTestSuite
			env := suite.NewTestWorkflowEnvironment(t)
			env.SetWorkerOptions(goWorker.Options{DeadlockDetectionTimeout: 10 * time.Second})
			env.RegisterWorkflowWithOptions(nativeParent, goWorkflow.RegisterOptions{Name: "parent-alias"})
			env.RegisterWorkflowWithOptions(nativeChild, goWorkflow.RegisterOptions{Name: "child-alias"})
			activityCalls, childCalls, started, completed := 0, 0, 0, 0
			activity := env.OnActivity(hello, mock.Anything, "x").Return(func(context.Context, string) (string, error) { activityCalls++; return "mock-activity", nil }).Once()
			child := env.OnWorkflow(nativeChild, mock.Anything, "x").Return(func(ctx context.Context, input string) (string, error) {
				require.False(t, isolate.IsActive())
				require.NotNil(t, ctx)
				childCalls++
				return "mock-child:" + input, nil
			}).Once().AfterFn(func() time.Duration { return 10 * time.Minute })
			env.InOrderMockCalls(activity, child)
			env.SetOnChildWorkflowStartedListener(func(info *goWorkflow.Info, ctx context.Context, args converter.EncodedValues) {
				require.False(t, isolate.IsActive())
				require.NotNil(t, ctx)
				require.Equal(t, "child-alias", info.WorkflowType.Name)
				var input string
				require.NoError(t, args.Get(&input))
				require.Equal(t, "x", input)
				started++
				info.WorkflowType.Name = "changed listener snapshot"
			})
			env.SetOnChildWorkflowCompletedListener(func(info *goWorkflow.Info, result converter.EncodedValue, err error) {
				require.NoError(t, err)
				require.Equal(t, "child-alias", info.WorkflowType.Name)
				var value string
				require.NoError(t, result.Get(&value))
				require.Equal(t, "mock-child:x", value)
				completed++
			})
			start := env.Now()
			env.ExecuteWorkflow(nativeParent, "x")
			require.NoError(t, env.GetWorkflowError())
			var result string
			require.NoError(t, env.GetWorkflowResult(&result))
			require.Equal(t, "mock-activity/mock-child:x", result)
			require.Equal(t, 10*time.Minute, env.Now().Sub(start))
			require.Equal(t, 1, activityCalls)
			require.Equal(t, 1, childCalls)
			require.Equal(t, 1, started)
			require.Equal(t, 1, completed)
			require.True(t, env.AssertExpectations(t))
			require.True(t, env.AssertWorkflowNumberOfCalls(t, "child-alias", 1))
			require.True(t, env.AssertCalled(t, "child-alias", mock.Anything, "x"))
			require.True(t, env.AssertNumberOfCalls(t, "child-alias", 1))
			require.True(t, env.AssertNotCalled(t, "child-alias", mock.Anything, "absent"))
		})
	}
}

func TestMarkedMockReplacementRunsWorkflowAPIs(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment(t)
	env.RegisterWorkflow(nativeChild)
	env.RegisterActivity(hello)
	env.OnWorkflow(nativeChild, mock.Anything, "x").ReturnIsolate(replacementChild).Once()
	start := env.Now()
	env.ExecuteWorkflow(nativeParent, "x")
	require.NoError(t, env.GetWorkflowError())
	var result string
	require.NoError(t, env.GetWorkflowResult(&result))
	require.Equal(t, "hello:x/replacement:hello:x", result)
	require.Equal(t, 2*time.Minute, env.Now().Sub(start))
	require.True(t, env.AssertExpectations(t))
}

func TestMockPanicAndUnexpectedCallsBecomeWorkflowFailures(t *testing.T) {
	for _, scenario := range []string{"panic", "never", "unexpected"} {
		t.Run(scenario, func(t *testing.T) {
			var suite testsuite.WorkflowTestSuite
			env := suite.NewTestWorkflowEnvironment(t)
			switch scenario {
			case "panic":
				env.OnWorkflow(nativeChild, mock.Anything, mock.Anything).Return(func(context.Context, string) (string, error) { panic("mock failure") })
			case "never":
				env.OnWorkflow(nativeChild, mock.Anything, mock.Anything).Never()
			case "unexpected":
				env.OnWorkflow(nativeChild, mock.Anything, "different").Return("wrong", nil)
			}
			env.ExecuteWorkflow(nativeChild, "x")
			err := env.GetWorkflowError()
			require.Error(t, err)
			var panicError *temporal.PanicError
			require.True(t, errors.As(err, &panicError))
		})
	}
}

//go:isolate
func refChild(ctx context.Context, ref testref.Ref[int]) (int, error) {
	value, err := ref.Load()
	if err != nil {
		return 0, err
	}
	return value + 1, ref.Store(value + 1)
}

//go:isolate
func refParent(ctx context.Context, ref testref.Ref[int]) (int, error) {
	var value int
	err := workflow.ExecuteChildWorkflow(ctx, refChild, ref).Get(ctx, &value)
	return value, err
}

func TestReferencesWorkAcrossChildrenAndRejectOtherEnvironments(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	first := suite.NewTestWorkflowEnvironment(t)
	first.RegisterWorkflow(refChild)
	ref, err := testsuite.NewRef(first, 10)
	require.NoError(t, err)
	first.ExecuteWorkflow(refParent, ref)
	require.NoError(t, first.GetWorkflowError())
	value, err := ref.Load()
	require.NoError(t, err)
	require.Equal(t, 11, value)
	second := suite.NewTestWorkflowEnvironment(t)
	second.ExecuteWorkflow(refChild, ref)
	require.ErrorContains(t, second.GetWorkflowError(), "another test environment")
	require.NoError(t, first.Close())
	_, err = ref.Load()
	require.ErrorContains(t, err, "closed")
}

//go:isolate
func readOnlyRefWorkflow(ctx context.Context, ref testref.Ref[int]) error {
	if err := workflow.SetQueryHandler(ctx, "write", func() (int, error) { return 0, ref.Store(99) }); err != nil {
		return err
	}
	return nil
}

func TestQueriesCannotChangeHostReferences(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment(t)
	ref, err := testsuite.NewRef(env, 7)
	require.NoError(t, err)
	env.ExecuteWorkflow(readOnlyRefWorkflow, ref)
	require.NoError(t, env.GetWorkflowError())
	_, err = env.QueryWorkflow("write")
	require.ErrorContains(t, err, "read-only")
	value, err := ref.Load()
	require.NoError(t, err)
	require.Equal(t, 7, value)
}

func ordinary(ctx goWorkflow.Context, input string) (string, error) { return "ordinary:" + input, nil }
func TestOrdinaryWorkflowsKeepSDKBehavior(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment(t)
	env.ExecuteWorkflow(ordinary, "x")
	require.NoError(t, env.GetWorkflowError())
	var result string
	require.NoError(t, env.GetWorkflowResult(&result))
	require.Equal(t, "ordinary:x", result)
}

func TestUnmarkedNativeWorkflowAndHostClosuresAreRejected(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment(t)
	require.Panics(t, func() { env.RegisterWorkflow(func(context.Context) error { return nil }) })
	call := env.OnWorkflow(nativeChild, mock.Anything, mock.Anything)
	require.Panics(t, func() { call.ReturnIsolate(func(context.Context, string) (string, error) { return "", nil }) })
}

func TestDelayedNativeMockCancellation(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment(t)
	called := false
	env.OnWorkflow(nativeChild, mock.Anything, "x").After(time.Hour).Return("unused", nil).Run(func(mock.Arguments) { called = true }).Once()
	env.RegisterDelayedCallback(env.CancelWorkflow, 10*time.Minute)
	start := env.Now()
	env.ExecuteWorkflow(nativeChild, "x")
	require.Error(t, env.GetWorkflowError())
	var canceled *temporal.CanceledError
	require.True(t, errors.As(env.GetWorkflowError(), &canceled))
	require.Equal(t, 10*time.Minute, env.Now().Sub(start))
	require.False(t, called)
	require.True(t, env.AssertExpectations(t))
}

func TestCompletedChildQueriesRetainState(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment(t)
	env.RegisterWorkflow(nativeChild)
	env.RegisterActivity(hello)
	var childID string
	env.SetOnChildWorkflowStartedListener(func(info *goWorkflow.Info, _ context.Context, _ converter.EncodedValues) {
		childID = info.WorkflowExecution.ID
	})
	env.ExecuteWorkflow(nativeParent, "x")
	require.NoError(t, env.GetWorkflowError())
	require.NotEmpty(t, childID)
	query, err := env.QueryWorkflowByID(childID, "state")
	require.NoError(t, err)
	var result string
	require.NoError(t, query.Get(&result))
	require.Equal(t, "x", result)
}

//go:isolate
func concurrentReferenceWorkflow(ctx context.Context, ref testref.Ref[int]) (int, error) {
	var workers sync.WaitGroup
	failures := make(chan error, 4)
	for range 4 {
		workers.Go(func() {
			for range 10 {
				for {
					old, err := ref.Load()
					if err != nil {
						failures <- err
						return
					}
					swapped, err := ref.CompareAndSwap(old, old+1)
					if err != nil {
						failures <- err
						return
					}
					if swapped {
						break
					}
				}
			}
		})
	}
	workers.Wait()
	close(failures)
	for err := range failures {
		return 0, err
	}
	return ref.Load()
}

func TestNativeGoroutinesUseAtomicReferenceCalls(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment(t)
	ref, err := testsuite.NewRef(env, 0)
	require.NoError(t, err)
	env.ExecuteWorkflow(concurrentReferenceWorkflow, ref)
	require.NoError(t, env.GetWorkflowError())
	var result int
	require.NoError(t, env.GetWorkflowResult(&result))
	require.Equal(t, 40, result)
	value, err := ref.Load()
	require.NoError(t, err)
	require.Equal(t, result, value)
}
