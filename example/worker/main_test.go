package main

import (
	"testing"

	"github.com/mfateev/sdk-go-poc/temporalbridge"
	"go.temporal.io/sdk/testsuite"
	goWorkflow "go.temporal.io/sdk/workflow"
)

func TestOrdinaryWorkflowAlongsideIsolateFactory(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.RegisterWorkflowWithOptions(temporalbridge.Factory{}, goWorkflow.RegisterOptions{Name: "IsolateTypedEcho"})
	env.RegisterWorkflow(PlainEcho)
	env.ExecuteWorkflow(PlainEcho, "hello")
	if err := env.GetWorkflowError(); err != nil {
		t.Fatal(err)
	}
	var got string
	if err := env.GetWorkflowResult(&got); err != nil {
		t.Fatal(err)
	}
	if got != "plain:hello" {
		t.Fatalf("result = %q, want %q", got, "plain:hello")
	}
}
