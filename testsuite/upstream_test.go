package testsuite_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/mfateev/sdk-go-poc/testsuite"
	"github.com/mfateev/sdk-go-poc/workflow"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/temporal"
	goWorkflow "go.temporal.io/sdk/workflow"
)

//temporal:workflow
func nilQueryWorkflow(ctx context.Context) error {
	return workflow.SetQueryHandler(ctx, "nil_response", func(input *string) (*string, error) {
		if input != nil {
			return nil, errors.New("nil query argument changed")
		}
		return nil, nil
	})
}

// TestQueryNilArgsAndResult ports Test_QueryWorkflow_NilArgsAndResult without
// keeping the workflow artificially running for the query.
func TestQueryNilArgsAndResult(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment(t)
	env.ExecuteWorkflow(nilQueryWorkflow)
	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())
	require.True(t, env.AssertExpectations(t))
	response, err := env.QueryWorkflow("nil_response", nil)
	require.NoError(t, err)
	require.NotNil(t, response)
	var value *string
	require.NoError(t, response.Get(&value))
	require.Nil(t, value)
}

// TestChildWorkflowListener ports the start/completion/result assertions of
// Test_ChildWorkflow_Listener; listener contexts are host context.Context values.
func TestChildWorkflowListener(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment(t)
	env.RegisterWorkflow(nativeChild)
	env.RegisterActivity(hello)
	var name, childResult string
	env.SetOnChildWorkflowStartedListener(func(info *goWorkflow.Info, _ context.Context, _ converter.EncodedValues) {
		name = info.WorkflowType.Name
	})
	env.SetOnChildWorkflowCompletedListener(func(_ *goWorkflow.Info, result converter.EncodedValue, err error) {
		require.NoError(t, err)
		require.NoError(t, result.Get(&childResult))
	})
	env.ExecuteWorkflow(nativeParent, "x")
	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())
	var result string
	require.NoError(t, env.GetWorkflowResult(&result))
	require.Equal(t, "hello:x/child:x", result)
	require.Equal(t, "nativeChild", name)
	require.Equal(t, "child:x", childResult)
}

//temporal:workflow
func waitForMockChild(ctx context.Context) (string, error) {
	child := workflow.ExecuteChildWorkflow(ctx, nativeChild, "x")
	select {
	case result := <-child.ToChannel():
		if result.Err != nil {
			return "", result.Err
		}
		var value string
		err := child.Get(ctx, &value)
		return value, err
	case <-time.After(time.Hour):
		return "", errors.New("child workflow takes too long")
	}
}

// TestMockWorkflowWait ports immediate, ten-minute and two-hour mock waits.
func TestMockWorkflowWait(t *testing.T) {
	for _, delay := range []time.Duration{0, 10 * time.Minute, 2 * time.Hour} {
		t.Run(delay.String(), func(t *testing.T) {
			var suite testsuite.WorkflowTestSuite
			env := suite.NewTestWorkflowEnvironment(t)
			env.OnWorkflow(nativeChild, mock.Anything, "x").After(delay).Return("hello_mock_delayed", nil).Once()
			env.ExecuteWorkflow(waitForMockChild)
			require.True(t, env.IsWorkflowCompleted())
			require.True(t, env.AssertExpectations(t))
			if delay < time.Hour {
				require.NoError(t, env.GetWorkflowError())
			} else {
				require.ErrorContains(t, env.GetWorkflowError(), "takes too long")
			}
		})
	}
}

//temporal:workflow
func getMockChildExecution(ctx context.Context) (string, error) {
	child := workflow.ExecuteChildWorkflow(ctx, nativeChild, "x")
	_ = child.GetChildWorkflowExecution().Get(ctx, nil)
	var result string
	err := child.Get(ctx, &result)
	return result, err
}

// TestChildWorkflowMockPanicGetExecution ports the invalid mock return case
// and its workflow -> child -> panic error chain without hanging on start.
func TestChildWorkflowMockPanicGetExecution(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment(t)
	env.OnWorkflow(nativeChild, mock.Anything, "x").Return("mock_result", nil, "extra_argument")
	env.ExecuteWorkflow(getMockChildExecution)
	require.True(t, env.IsWorkflowCompleted())
	err := env.GetWorkflowError()
	require.Error(t, err)
	var workflowErr *temporal.WorkflowExecutionError
	require.True(t, errors.As(err, &workflowErr))
	var childErr *temporal.ChildWorkflowExecutionError
	require.True(t, errors.As(errors.Unwrap(workflowErr), &childErr))
	var panicErr *temporal.PanicError
	require.True(t, errors.As(errors.Unwrap(childErr), &panicErr))
	require.Contains(t, panicErr.Error(), "mock returns 3 values, workflow requires 2")
}

//temporal:workflow
func mockChildStartFailure(ctx context.Context) error {
	child := workflow.ExecuteChildWorkflow(ctx, nativeChild, "x")
	return child.GetChildWorkflowExecution().Get(ctx, nil)
}

func TestMockChildStartFailure(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment(t)
	env.OnWorkflow(nativeChild, mock.Anything, "x").Return("", testsuite.ErrMockStartChildWorkflowFailed)
	env.ExecuteWorkflow(mockChildStartFailure)
	require.ErrorContains(t, env.GetWorkflowError(), "start child workflow failed")
}

//temporal:workflow
func localActivityByName(ctx context.Context, name string) (string, error) {
	ctx = workflow.WithLocalActivityOptions(ctx, workflow.LocalActivityOptions{StartToCloseTimeout: time.Minute})
	var result string
	err := workflow.ExecuteLocalActivity(ctx, name, "x").Get(ctx, &result)
	return result, err
}

func TestLocalActivityExecutionByNameAndAlias(t *testing.T) {
	for _, name := range []string{"hello", "alias"} {
		t.Run(name, func(t *testing.T) {
			var suite testsuite.WorkflowTestSuite
			env := suite.NewTestWorkflowEnvironment(t)
			env.RegisterActivityWithOptions(hello, activity.RegisterOptions{Name: name})
			env.ExecuteWorkflow(localActivityByName, name)
			require.NoError(t, env.GetWorkflowError())
			var result string
			require.NoError(t, env.GetWorkflowResult(&result))
			require.Equal(t, "hello:x", result)
		})
	}
}

type localActivities struct{}

func (*localActivities) Hello(ctx context.Context, input string) (string, error) {
	return hello(ctx, input)
}

func TestLocalActivityStructRegistration(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment(t)
	env.RegisterActivityWithOptions(&localActivities{}, activity.RegisterOptions{Name: "prefix."})
	env.ExecuteWorkflow(localActivityByName, "prefix.Hello")
	require.NoError(t, env.GetWorkflowError())
	var result string
	require.NoError(t, env.GetWorkflowResult(&result))
	require.Equal(t, "hello:x", result)
}

func TestMockPanicThroughRun(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment(t)
	env.OnWorkflow(nativeChild, mock.Anything, "x").Return("unused", nil).Run(func(mock.Arguments) { panic("mock Run panic") })
	env.ExecuteWorkflow(nativeChild, "x")
	var panicErr *temporal.PanicError
	require.ErrorAs(t, env.GetWorkflowError(), &panicErr)
	require.Equal(t, "mock Run panic", panicErr.Error())
}

func TestWorkflowMockingWithoutRegistration(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment(t)
	env.OnWorkflow(nativeChild, mock.Anything, "Temporal").Return(func(context.Context, string) (string, error) {
		return "Hello Temporal!", nil
	})
	env.ExecuteWorkflow("nativeChild", "Temporal")
	require.NoError(t, env.GetWorkflowError())
	var result string
	require.NoError(t, env.GetWorkflowResult(&result))
	require.Equal(t, "Hello Temporal!", result)
}

//temporal:workflow
func secondLocalActivity(ctx context.Context) error {
	ctx = workflow.WithLocalActivityOptions(ctx, workflow.LocalActivityOptions{
		StartToCloseTimeout: time.Hour,
		RetryPolicy:         &temporal.RetryPolicy{MaximumAttempts: 1},
	})
	return workflow.ExecuteLocalActivity(ctx, "hello", "call2").Get(ctx, nil)
}

func TestMockCallWrapperOrderFailures(t *testing.T) {
	for _, useInOrder := range []bool{false, true} {
		t.Run(map[bool]string{false: "NotBefore", true: "InOrder"}[useInOrder], func(t *testing.T) {
			var suite testsuite.WorkflowTestSuite
			env := suite.NewTestWorkflowEnvironment(t)
			env.RegisterActivity(hello)
			first := env.OnActivity(hello, mock.Anything, "call1").Return("result1", nil)
			second := env.OnActivity(hello, mock.Anything, "call2").Return("result2", nil)
			if useInOrder {
				env.InOrderMockCalls(first, second)
			} else {
				second.NotBefore(first)
			}
			env.ExecuteWorkflow(secondLocalActivity)
			var panicErr *temporal.PanicError
			require.ErrorAs(t, env.GetWorkflowError(), &panicErr)
			require.ErrorContains(t, panicErr, "Must not be called before")
		})
	}
}

//temporal:workflow
func queryableChild(ctx context.Context) error {
	state := "wait for signal"
	if err := workflow.SetQueryHandler(ctx, "state", func(prefix string) (string, error) { return prefix + state, nil }); err != nil {
		return err
	}
	signal := <-workflow.GetSignalChannel[string](ctx, "query-signal")
	state = "wait for activity"
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{StartToCloseTimeout: 2 * time.Hour})
	if err := workflow.ExecuteActivity(ctx, hello, signal).Get(ctx, nil); err != nil {
		return err
	}
	state = "done"
	return nil
}

//temporal:workflow
func queryableChildParent(ctx context.Context) error {
	ctx = workflow.WithChildWorkflowOptions(ctx, workflow.ChildWorkflowOptions{WorkflowID: "test-query-child-workflow"})
	return workflow.ExecuteChildWorkflow(ctx, queryableChild).Get(ctx, nil)
}

// TestQueryChildWorkflow ports queries during signal/activity waits and after
// child completion, using the child's explicit workflow ID.
func TestQueryChildWorkflow(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment(t)
	env.RegisterWorkflow(queryableChild)
	env.RegisterActivity(hello)
	verify := func(expected string) {
		value, err := env.QueryWorkflowByID("test-query-child-workflow", "state", "input:")
		require.NoError(t, err)
		var result string
		require.NoError(t, value.Get(&result))
		require.Equal(t, "input:"+expected, result)
	}
	env.RegisterDelayedCallback(func() {
		verify("wait for signal")
		require.NoError(t, env.SignalWorkflowByID("test-query-child-workflow", "query-signal", "x"))
	}, time.Hour)
	env.OnActivity(hello, mock.Anything, "x").After(time.Hour).Return("mock", nil)
	env.SetOnActivityStartedListener(func(_ *activity.Info, _ context.Context, _ converter.EncodedValues) { verify("wait for activity") })
	env.ExecuteWorkflow(queryableChildParent)
	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())
	verify("done")
	require.True(t, env.AssertExpectations(t))
}
