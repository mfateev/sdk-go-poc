package worker

import (
	"testing"

	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/testsuite"
	goWorker "go.temporal.io/sdk/worker"
	goWorkflow "go.temporal.io/sdk/workflow"
)

type testWorker struct {
	goWorker.Worker
	env *testsuite.TestWorkflowEnvironment
}

func (w *testWorker) RegisterWorkflowWithOptions(fn any, options goWorkflow.RegisterOptions) {
	w.env.RegisterWorkflowWithOptions(fn, options)
}

func ordinary(_ goWorkflow.Context, input string) (string, error) {
	return "ordinary:" + input, nil
}

func TestOrdinaryWorkflowPreservesWorkerOptionsAndCustomConverter(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.SetDataConverter(converter.NewCompositeDataConverter(converter.NewJSONPayloadConverter()))
	w := Wrap(&testWorker{env: env})
	w.RegisterWorkflowWithOptions(ordinary, goWorkflow.RegisterOptions{Name: "custom-name"})
	env.ExecuteWorkflow("custom-name", "hello")
	if err := env.GetWorkflowError(); err != nil {
		t.Fatal(err)
	}
	var result string
	if err := env.GetWorkflowResult(&result); err != nil {
		t.Fatal(err)
	}
	if result != "ordinary:hello" {
		t.Fatalf("result = %q", result)
	}
}
