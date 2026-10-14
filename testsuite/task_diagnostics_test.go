package testsuite_test

import (
	"context"
	"errors"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/mfateev/sdk-go-poc/temporalbridge"
	"github.com/mfateev/sdk-go-poc/testsuite"
	"github.com/mfateev/sdk-go-poc/workflow"
	goWorker "go.temporal.io/sdk/worker"
)

//temporal:workflow
func taskDiagnosticStall(ctx context.Context) error {
	if err := workflow.SetQueryHandler(ctx, "state", func() (string, error) { return "ready", nil }); err != nil {
		return err
	}
	for {
		runtime.Gosched()
	}
}

//temporal:workflow
func readOnlyDiagnosticWorkflow(ctx context.Context) error {
	return workflow.SetQueryHandler(ctx, "stall", func() (string, error) {
		for {
			runtime.Gosched()
		}
	})
}

func TestQueryUsesWorkerBudgetAndCapturesDiagnosticsBeforeCleanup(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment(t)
	env.SetWorkerOptions(goWorker.Options{DeadlockDetectionTimeout: 3 * time.Second})
	env.ExecuteWorkflow(readOnlyDiagnosticWorkflow)
	if err := env.GetWorkflowError(); err != nil {
		t.Fatal(err)
	}
	started := time.Now()
	defer func() {
		failure, ok := recover().(*temporalbridge.WorkflowTaskError)
		if !ok || !errors.Is(failure, context.DeadlineExceeded) {
			t.Fatalf("query timeout lost its deadline cause: %v", failure)
		}
		if time.Since(started) < 2*time.Second {
			t.Fatal("query used the hardcoded one-second limit instead of the worker budget")
		}
		for _, want := range []string{"read-only handler deadline exceeded", `name="stall"`, "isolate dispatch:", "read_only=true", "workflow.serveQueries", "isolate.(*Isolate).Suspend"} {
			if !strings.Contains(failure.Error(), want) {
				t.Fatalf("query timeout lost %q before cleanup: %v", want, failure)
			}
		}
	}()
	_, _ = env.QueryWorkflow("stall")
	t.Fatal("stalled query unexpectedly returned")
}

func TestTaskTimeoutCapturesDiagnosticsBeforeCleanup(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment(t)
	env.SetWorkerOptions(goWorker.Options{DeadlockDetectionTimeout: time.Second})
	env.RegisterWorkflow(taskDiagnosticStall)
	env.ExecuteWorkflow(taskDiagnosticStall)
	err := env.GetWorkflowError()
	if err == nil {
		t.Fatal("stalled workflow unexpectedly completed")
	}
	for _, want := range []string{"workflow task deadline exceeded", "last_handled_operation=", "last_handler_duration=", "isolate dispatch:", "normal_queue=", "priority_queue=", "taskDiagnosticStall", "workflow.executionContext", "workflow.serveQueries", "isolate.(*Isolate).Suspend", "temporalbridge.(*definition).OnWorkflowTaskStarted"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("timeout lost %q before cleanup: %s", want, err)
		}
	}
}
