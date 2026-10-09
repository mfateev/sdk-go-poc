package temporalbridge

import (
	"encoding/json"
	"isolate"
	"testing"
	"time"

	"github.com/mfateev/sdk-go-poc/workflow"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	bindings "go.temporal.io/sdk/internalbindings"
)

type childEnvironment struct {
	optionEnvironment
	childParams bindings.ExecuteWorkflowParams
	result      bindings.ResultHandler
	start       func(bindings.WorkflowExecution, error)
}

func (e *childEnvironment) ExecuteChildWorkflow(p bindings.ExecuteWorkflowParams, result bindings.ResultHandler, start func(bindings.WorkflowExecution, error)) {
	e.childParams, e.result, e.start = p, result, start
}
func (e *childEnvironment) RequestCancelChildWorkflow(string, string) { e.cancels++ }

func TestChildOptionsReachSDKAndCloseRetiresCallbacks(t *testing.T) {
	e := new(childEnvironment)
	d := &definition{env: e}
	o := workflow.ChildWorkflowOptions{Namespace: "target", WorkflowID: "business-id", TaskQueue: "children", WorkflowExecutionTimeout: time.Hour, WorkflowRunTimeout: time.Minute, WorkflowTaskTimeout: 10 * time.Second,
		WaitForCancellation: true, WorkflowIDReusePolicy: enumspb.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE,
		RetryPolicy: &workflow.RetryPolicy{MaximumAttempts: 3}, CronSchedule: "@daily", ParentClosePolicy: enumspb.PARENT_CLOSE_POLICY_ABANDON,
		StaticSummary: "summary", StaticDetails: "details", Priority: workflow.Priority{PriorityKey: 2, FairnessKey: "tenant", FairnessWeight: 1.25}}
	p, err := json.Marshal(workflow.ChildRequest{ID: 1, Name: "child", Options: o})
	if err != nil {
		t.Fatal(err)
	}
	if err := d.handle(&isolate.Command{Op: workflow.OpScheduleChild, Payload: p}); err != nil {
		t.Fatal(err)
	}
	actual := e.childParams
	if actual.Namespace != o.Namespace || actual.WorkflowID != o.WorkflowID || actual.TaskQueueName != o.TaskQueue || actual.WorkflowExecutionTimeout != o.WorkflowExecutionTimeout || actual.WorkflowRunTimeout != o.WorkflowRunTimeout || actual.WorkflowTaskTimeout != o.WorkflowTaskTimeout || !actual.WaitForCancellation || actual.WorkflowIDReusePolicy != o.WorkflowIDReusePolicy || actual.CronSchedule != o.CronSchedule || actual.ParentClosePolicy != o.ParentClosePolicy || actual.StaticSummary != o.StaticSummary || actual.StaticDetails != o.StaticDetails || actual.RetryPolicy.GetMaximumAttempts() != 3 || actual.Priority.GetFairnessKey() != "tenant" {
		t.Fatalf("SDK child options=%+v", actual)
	}
	result := &isolate.Command{Op: workflow.OpAwaitChild, Payload: []byte("1")}
	start := &isolate.Command{Op: workflow.OpAwaitChildExecution, Payload: []byte("1")}
	if err := d.handle(result); err != nil {
		t.Fatal(err)
	}
	if err := d.handle(start); err != nil {
		t.Fatal(err)
	}
	s := d.children[1]
	d.Close()
	if !s.retired || s.resultWaiter != nil || s.startWaiter != nil || s.result != nil || s.start != nil || s.cancel != nil || d.children != nil {
		t.Fatal("close retained child command cells")
	}
	e.start(bindings.WorkflowExecution{ID: "late", RunID: "late"}, nil)
	e.result(new(commonpb.Payloads), nil)
	if e.cancels != 0 || len(d.pending) != 0 || len(d.immediate) != 0 {
		t.Fatal("late child callback revived workflow or canceled server child")
	}
}
