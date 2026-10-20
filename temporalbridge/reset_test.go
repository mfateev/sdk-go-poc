package temporalbridge

import (
	"isolate"
	"testing"

	"github.com/mfateev/sdk-go-poc/workflow"
	bindings "go.temporal.io/sdk/internalbindings"
	sdkwf "go.temporal.io/sdk/workflow"
)

type resetEnvironment struct{ lifecycleEnvironment }

func (*resetEnvironment) WorkflowInfo() *sdkwf.Info {
	return &sdkwf.Info{OriginalRunID: "original", WorkflowExecution: sdkwf.Execution{RunID: "latest"}}
}

func TestResetQueuesIdentityUntilWorkflowTaskBoundary(t *testing.T) {
	env := new(resetEnvironment)
	d := new(definition)
	d.Execute(env, nil, nil)
	defer d.Close()
	d.updateRandomIdentity()
	if d.randomRunID != "original" {
		t.Fatalf("initial seed used public execution ID: %q", d.randomRunID)
	}
	d.started = true
	d.cancelWaiter = &isolate.Command{Op: workflow.OpWorkflowCancel}
	env.resetHandler(bindings.WorkflowResetInfo{BaseRunID: "original", NewRunID: "reset-1", EventID: 10})
	if d.randomRunID != "original" || len(d.pending) != 0 {
		t.Fatal("reset callback advanced workflow state before the task boundary")
	}
	// Cancellation can arrive in the same history batch. The task boundary must
	// deliver both the cancellation and final reset identity in one control reply.
	env.cancelHandler()
	env.resetHandler(bindings.WorkflowResetInfo{BaseRunID: "reset-1", NewRunID: "reset-2", EventID: 20})
	d.updateRandomIdentity()
	if len(d.pending) != 1 {
		t.Fatalf("control replies = %d", len(d.pending))
	}
	var control workflow.WorkflowControl
	if err := workflow.UnmarshalTransport(d.pending[0].payload, &control); err != nil {
		t.Fatal(err)
	}
	if control.RandomRunID != "reset-2" || !control.Canceled {
		t.Fatalf("control = %+v", control)
	}
}
