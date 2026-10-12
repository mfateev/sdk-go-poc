package main

import (
	"context"
	"fmt"
	"isolate"
	"strings"
	"time"

	"github.com/mfateev/sdk-go-poc/example/upstream"
	"github.com/mfateev/sdk-go-poc/temporalbridge"
	"github.com/stretchr/testify/mock"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/testsuite"
	goWorker "go.temporal.io/sdk/worker"
	goWorkflow "go.temporal.io/sdk/workflow"
)

type assertions struct{}

func (assertions) Logf(string, ...any)               {}
func (assertions) Errorf(format string, args ...any) { panic(fmt.Sprintf(format, args...)) }
func (assertions) FailNow()                          { panic("mock expectation failed") }

func Hello(_ context.Context, input string) (string, error) { return "hello_" + input, nil }

func runEnvironment(scenario string) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.SetWorkerOptions(goWorker.Options{DeadlockDetectionTimeout: 10 * time.Second})
	h, ok := isolate.LookupFunction(upstream.EnvironmentWorkflow)
	if !ok {
		panic("environment workflow handle missing")
	}
	child, ok := isolate.LookupFunction(upstream.EnvironmentChild)
	if !ok {
		panic("environment child handle missing")
	}
	env.RegisterWorkflowWithOptions(temporalbridge.Factory{Function: h, ResolveLocalActivity: func(name string) any {
		if name == "Hello" {
			return Hello
		}
		return nil
	}}, goWorkflow.RegisterOptions{Name: "EnvironmentWorkflow"})
	env.RegisterWorkflowWithOptions(temporalbridge.Factory{Function: child}, goWorkflow.RegisterOptions{Name: "EnvironmentChild"})
	expected := "hello_world"
	calls := []string{}
	localStarted, localCompleted := 0, 0
	childCompleted := 0
	mockRun := false
	queryCalled := false
	switch scenario {
	case "mock-function", "mock-by-name":
		mockActivity := func(_ context.Context, input string) (string, error) { return "mock_" + input, nil }
		if scenario == "mock-function" {
			env.OnActivity(Hello, mock.Anything, "world").Return(mockActivity).Once()
		} else {
			env.RegisterActivity(Hello)
			env.OnActivity("Hello", mock.Anything, "world").Return(mockActivity).Once()
		}
		expected = "mock_world"
	case "mock-values", "mock-wait", "mock-wait-function", "mock-run":
		call := env.OnActivity(Hello, mock.Anything, "world").Return("mock_value", nil).Once()
		if scenario == "mock-wait" {
			call.After(time.Second)
		}
		if scenario == "mock-wait-function" {
			call.AfterFn(func() time.Duration { return time.Second })
		}
		if scenario == "mock-run" {
			call.Run(func(args mock.Arguments) { mockRun = true })
		}
		expected = "mock_value"
	case "mock-never":
		env.OnActivity(Hello, mock.Anything, "world").Never()
	case "activity-listener":
		env.RegisterActivity(Hello)
		env.SetOnActivityStartedListener(func(info *activity.Info, _ context.Context, args converter.EncodedValues) {
			var input string
			if err := args.Get(&input); err != nil {
				panic(err)
			}
			calls = append(calls, info.ActivityType.Name+":"+input)
		})
		expected = "ok"
	case "timer-clock":
		expected = "ok"
	case "clock-activity":
		env.RegisterActivity(Hello)
		expected = "hello_controlled_execution"
	case "query-nil":
		expected = "ok"
		env.RegisterDelayedCallback(func() {
			result, err := env.QueryWorkflow("nil_response", nil)
			if err != nil || result == nil {
				panic(fmt.Sprintf("nil query result: %v", err))
			}
			var value *string
			if err := result.Get(&value); err != nil || value != nil {
				panic(fmt.Sprintf("query nil decode: %v", err))
			}
			queryCalled = true
		}, time.Millisecond)
	case "version-mock":
		expected = "ok"
		env.OnGetVersion("change_1", goWorkflow.DefaultVersion, 2).Return(func(string, goWorkflow.Version, goWorkflow.Version) goWorkflow.Version {
			return goWorkflow.DefaultVersion
		}).Once()
		env.OnGetVersion(mock.Anything, goWorkflow.DefaultVersion, 2).Return(goWorkflow.Version(2)).Once()
	case "child-completed-listener":
		env.RegisterActivity(Hello)

		env.SetOnChildWorkflowCompletedListener(func(_ *goWorkflow.Info, result converter.EncodedValue, err error) {
			if err != nil {
				panic(err)
			}
			var value string
			if err := result.Get(&value); err != nil || value != "hello_world" {
				panic("wrong child result")
			}
			childCompleted++
		})
	case "local-listener":
		env.RegisterActivity(Hello)
		env.OnActivity(Hello, mock.Anything, "world").Return("mock_value", nil).Once()
		env.SetOnLocalActivityStartedListener(func(info *activity.Info, _ context.Context, args []any) {
			if info.ActivityType.Name != "Hello" || len(args) != 1 || args[0] != "world" {
				panic("local activity started listener")
			}
			localStarted++
		})
		env.SetOnLocalActivityCompletedListener(func(_ *activity.Info, result converter.EncodedValue, err error) {
			if err != nil {
				panic(err)
			}
			var value string
			if err := result.Get(&value); err != nil || value != "mock_value" {
				panic("local activity completed listener")
			}
			localCompleted++
		})
		expected = "mock_value"
	default:
		panic("unknown environment scenario: " + scenario)
	}
	env.ExecuteWorkflow("EnvironmentWorkflow", scenario)
	if !env.IsWorkflowCompleted() {
		panic("environment workflow did not complete")
	}
	if scenario == "mock-never" {
		if err := env.GetWorkflowError(); err == nil || !strings.Contains(err.Error(), "unexpected call") {
			panic(fmt.Sprintf("never mock error: %v", err))
		}
		return
	}
	if err := env.GetWorkflowError(); err != nil {
		panic(err)
	}
	var result string
	if err := env.GetWorkflowResult(&result); err != nil || result != expected {
		panic(fmt.Sprintf("environment result %q want %q: %v", result, expected, err))
	}
	if !env.AssertExpectations(assertions{}) {
		panic("mock expectations failed")
	}
	if scenario == "mock-values" && (!env.AssertActivityCalled(assertions{}, "Hello", mock.Anything, "world") || !env.AssertActivityNumberOfCalls(assertions{}, "Hello", 1)) {
		panic("mock call assertions")
	}
	if scenario == "mock-run" && !mockRun {
		panic("mock Run was not called")
	}
	if scenario == "activity-listener" {
		if len(calls) != 100 {
			panic("wrong activity listener call count")
		}
		for i, got := range calls {
			if got != fmt.Sprintf("Hello:msg%d", i+1) {
				panic("wrong activity listener order")
			}
		}
	}
	if scenario == "query-nil" && !queryCalled {
		panic("query callback was not called")
	}
	if scenario == "child-completed-listener" && childCompleted != 1 {
		panic(fmt.Sprintf("child completed listener count=%d", childCompleted))
	}
	if scenario == "local-listener" && (localStarted != 1 || localCompleted != 1) {
		panic("local listener counts")
	}
}
