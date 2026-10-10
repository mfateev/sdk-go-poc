package temporalbridge

import (
	"testing"

	goWorkflow "go.temporal.io/sdk/workflow"
)

func TestWorkflowRandomSeed(t *testing.T) {
	info := goWorkflow.Info{Namespace: "namespace", WorkflowExecution: goWorkflow.Execution{ID: "workflow", RunID: "live"}, OriginalRunID: "history-run"}
	seed := workflowRandomSeed(&info)
	info.WorkflowExecution.RunID = "standalone-replay"
	info.WorkflowExecution.ID = "replay-workflow"
	info.Namespace = "ReplayNamespace"
	if workflowRandomSeed(&info) != seed {
		t.Fatal("replay's synthetic run ID changed random bytes")
	}
	info.OriginalRunID = "other-history-run"
	if workflowRandomSeed(&info) == seed {
		t.Fatal("different workflow runs reused the stream")
	}
}
