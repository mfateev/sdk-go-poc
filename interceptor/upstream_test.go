package interceptor

import (
	"github.com/mfateev/sdk-go-poc/workflow"
	goWorkflow "go.temporal.io/sdk/workflow"
	"testing"
)

func TestUpstreamIdempotencyKeyGeneration(t *testing.T) {
	for _, tc := range []struct{ run, original, want string }{
		{"bar", "original", "original"}, {"bar", "", "bar"},
	} {
		i := &tracingWorkflowInboundInterceptor{info: &workflow.Info{Info: goWorkflow.Info{OriginalRunID: tc.original, WorkflowExecution: workflow.Execution{ID: "foo", RunID: tc.run}}}}
		if got := i.newIdempotencyKey(); got != "WorkflowInboundInterceptor:"+tc.want+":1" {
			t.Fatalf("first key=%q", got)
		}
		if got := i.newIdempotencyKey(); got != "WorkflowInboundInterceptor:"+tc.want+":2" {
			t.Fatalf("second key=%q", got)
		}
	}
}
