package testsuite_test

import (
	"context"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/mfateev/sdk-go-poc/testsuite"
	"github.com/mfateev/sdk-go-poc/workflow"
	goWorker "go.temporal.io/sdk/worker"
)

//go:isolate
func taskDiagnosticStall(ctx context.Context) error {
	if err := workflow.SetQueryHandler(ctx, "state", func() (string, error) { return "ready", nil }); err != nil {
		return err
	}
	for {
		runtime.Gosched()
	}
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
	for _, want := range []string{"did not suspend", "isolate dispatch:", "normal_queue=", "priority_queue=", "taskDiagnosticStall", "workflow.executionContext", "workflow.serveQueries", "isolate.(*Isolate).Suspend", "temporalbridge.(*definition).OnWorkflowTaskStarted"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("timeout lost %q before cleanup: %s", want, err)
		}
	}
}
