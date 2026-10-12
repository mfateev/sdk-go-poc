package workflow

import (
	"context"
	"reflect"
	"testing"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/temporal"
)

// Ports internal/workflow_test.go's all-fields checks. A new SDK option must
// receive a nonzero test value so getter omissions cannot pass unnoticed.
func assertOptionFieldsPopulated(t *testing.T, options any) {
	t.Helper()
	value := reflect.ValueOf(options)
	for i := 0; i < value.NumField(); i++ {
		field := value.Type().Field(i)
		if field.IsExported() && value.Field(i).IsZero() {
			t.Errorf("%T.%s has no test value", options, field.Name)
		}
	}
}

func TestUpstreamGetActivityOptions(t *testing.T) {
	options := ActivityOptions{
		TaskQueue: "activities", ScheduleToCloseTimeout: time.Millisecond,
		ScheduleToStartTimeout: time.Second, StartToCloseTimeout: time.Minute,
		HeartbeatTimeout: time.Hour, WaitForCancellation: true, ActivityID: "activity",
		RetryPolicy: &RetryPolicy{InitialInterval: time.Second, BackoffCoefficient: 2,
			MaximumInterval: time.Minute, MaximumAttempts: 4, NonRetryableErrorTypes: []string{"invalid"}},
		DisableEagerExecution: true, VersioningIntent: temporal.VersioningIntentDefault,
		Summary: "activity summary", Priority: Priority{PriorityKey: 1, FairnessKey: "key", FairnessWeight: 3.14},
	}
	assertOptionFieldsPopulated(t, options)
	got := GetActivityOptions(WithActivityOptions(context.Background(), options))
	if !reflect.DeepEqual(got, options) {
		t.Fatalf("got %+v, want %+v", got, options)
	}
}

func TestUpstreamGetChildWorkflowOptions(t *testing.T) {
	options := ChildWorkflowOptions{
		Namespace: "namespace", WorkflowID: "child", TaskQueue: "children",
		WorkflowExecutionTimeout: time.Hour, WorkflowRunTimeout: time.Minute, WorkflowTaskTimeout: time.Second,
		WaitForCancellation: true, WorkflowIDReusePolicy: enumspb.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE,
		RetryPolicy: &RetryPolicy{MaximumAttempts: 3}, CronSchedule: "@hourly",
		Memo: map[string]any{"memo": "value"}, SearchAttributes: map[string]any{"CustomKeywordField": "value"},
		TypedSearchAttributes: temporal.NewSearchAttributes(temporal.NewSearchAttributeKeyInt64("CustomIntField").ValueSet(7)),
		ParentClosePolicy:     enumspb.PARENT_CLOSE_POLICY_REQUEST_CANCEL, VersioningIntent: temporal.VersioningIntentDefault,
		StaticSummary: "summary", StaticDetails: "details", Priority: Priority{PriorityKey: 1, FairnessKey: "key", FairnessWeight: 3.14},
	}
	assertOptionFieldsPopulated(t, options)
	got := GetChildWorkflowOptions(WithChildWorkflowOptions(context.Background(), options))
	if !reflect.DeepEqual(got, options) {
		t.Fatalf("got %+v, want %+v", got, options)
	}
}

func TestUpstreamGetLocalActivityOptions(t *testing.T) {
	options := LocalActivityOptions{ScheduleToCloseTimeout: time.Minute, StartToCloseTimeout: time.Hour,
		RetryPolicy: &RetryPolicy{MaximumAttempts: 4}, Summary: "local summary"}
	assertOptionFieldsPopulated(t, options)
	got := GetLocalActivityOptions(WithLocalActivityOptions(context.Background(), options))
	if !reflect.DeepEqual(got, options) {
		t.Fatalf("got %+v, want %+v", got, options)
	}
	got.RetryPolicy.MaximumAttempts = 99
	if options.RetryPolicy.MaximumAttempts != 4 {
		t.Fatal("getter aliases retry policy")
	}
}
