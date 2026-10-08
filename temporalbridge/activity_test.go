package temporalbridge

import (
	"encoding/json"
	"isolate"
	"testing"
	"time"

	"github.com/mfateev/sdk-go-poc/workflow"
	commonpb "go.temporal.io/api/common/v1"
	bindings "go.temporal.io/sdk/internalbindings"
	"go.temporal.io/sdk/temporal"
)

type optionEnvironment struct {
	activityEnvironment
	params bindings.ExecuteActivityParams
}

func (e *optionEnvironment) ExecuteActivity(p bindings.ExecuteActivityParams, callback bindings.ResultHandler) bindings.ActivityID {
	e.params = p
	e.callback = callback
	return bindings.ActivityID{}
}
func (e *optionEnvironment) RequestCancelActivity(id bindings.ActivityID) {
	e.cancels++
	if !e.params.WaitForCancellation {
		e.callback(nil, temporal.NewCanceledError())
	}
}
func scheduleActivity(t *testing.T, d *definition, o workflow.ActivityOptions) {
	t.Helper()
	p, err := json.Marshal(workflow.ActivityPayloadRequest{ID: 1, Name: "echo", Options: &o})
	if err != nil {
		t.Fatal(err)
	}
	if err := d.handle(&isolate.Command{Op: workflow.OpScheduleActivity, Payload: p}); err != nil {
		t.Fatal(err)
	}
}
func TestActivityOptionsReachSDKUnchanged(t *testing.T) {
	e := new(optionEnvironment)
	d := &definition{env: e}
	o := workflow.ActivityOptions{TaskQueue: "activities", ActivityID: "business-id", ScheduleToCloseTimeout: time.Hour,
		ScheduleToStartTimeout: time.Minute, StartToCloseTimeout: 10 * time.Second, HeartbeatTimeout: 2 * time.Second, WaitForCancellation: true,
		RetryPolicy:           &workflow.RetryPolicy{InitialInterval: 3 * time.Second, BackoffCoefficient: 1.5, MaximumInterval: time.Minute, MaximumAttempts: 5, NonRetryableErrorTypes: []string{"invalid"}},
		DisableEagerExecution: true, Summary: "summary", Priority: workflow.Priority{PriorityKey: 2, FairnessKey: "tenant", FairnessWeight: 1.25}}
	scheduleActivity(t, d, o)
	p := e.params
	if p.TaskQueueName != o.TaskQueue || p.ActivityID != o.ActivityID || p.ScheduleToCloseTimeout != o.ScheduleToCloseTimeout || p.StartToCloseTimeout != o.StartToCloseTimeout || p.ScheduleToStartTimeout != o.ScheduleToStartTimeout || p.HeartbeatTimeout != o.HeartbeatTimeout || !p.WaitForCancellation || !p.DisableEagerExecution || p.Summary != o.Summary || p.ScheduleID != 1 {
		t.Fatalf("options: %+v", p)
	}
	if p.RetryPolicy.MaximumAttempts != 5 || p.RetryPolicy.InitialInterval.AsDuration() != 3*time.Second || p.Priority.FairnessKey != "tenant" {
		t.Fatalf("retry/priority: %+v", p)
	}
}
func TestActivityCancellationUsesSDKCompletionPolicy(t *testing.T) {
	for _, wait := range []bool{false, true} {
		t.Run(map[bool]string{false: "immediate", true: "wait"}[wait], func(t *testing.T) {
			e := new(optionEnvironment)
			d := &definition{env: e}
			scheduleActivity(t, d, workflow.ActivityOptions{ScheduleToCloseTimeout: time.Minute, WaitForCancellation: wait})
			await := &isolate.Command{Op: workflow.OpAwaitActivity, Payload: []byte("1")}
			if err := d.handle(await); err != nil {
				t.Fatal(err)
			}
			d.immediate = nil
			for range 2 {
				if err := d.handle(&isolate.Command{Op: workflow.OpCancelActivity, Payload: []byte("1")}); err != nil {
					t.Fatal(err)
				}
			}
			if e.cancels != 1 {
				t.Fatalf("cancel requests %d", e.cancels)
			}
			if wait {
				if len(d.immediate) != 2 || len(d.pending) != 0 {
					t.Fatal("waiting activity resolved before SDK callback")
				}
				e.callback(&commonpb.Payloads{}, nil)
				if len(d.pending) != 1 {
					t.Fatal("waiting cancellation did not preserve actual completion")
				}
			} else {
				if len(d.immediate) != 3 || d.immediate[0].command != await {
					t.Fatal("immediate cancellation must reply within same task")
				}
				var result workflow.ActivityOutcome
				if err := json.Unmarshal(d.immediate[0].payload, &result); err != nil || !result.Canceled {
					t.Fatalf("cancellation: %+v %v", result, err)
				}
				e.callback(nil, nil)
				if len(d.pending) != 0 {
					t.Fatal("late callback completed activity again")
				}
			}
		})
	}
}
func TestCloseRetiresIgnoredActivityWithoutCancelingServerWork(t *testing.T) {
	e := new(optionEnvironment)
	d := &definition{env: e}
	scheduleActivity(t, d, workflow.ActivityOptions{StartToCloseTimeout: time.Minute})
	state := d.activities[1]
	d.Close()
	e.callback(nil, nil)
	if e.cancels != 0 || !state.retired || state.cancel != nil || state.waiter != nil || len(d.activities) != 0 || len(d.pending) != 0 {
		t.Fatal("close retained callback or canceled durable work")
	}
}

func TestConsumedActivityRetiresOutcomeFromRetainedSDKCallback(t *testing.T) {
	e := new(optionEnvironment)
	d := &definition{env: e}
	scheduleActivity(t, d, workflow.ActivityOptions{StartToCloseTimeout: time.Minute})
	state := d.activities[1]
	e.callback(&commonpb.Payloads{}, nil)
	if !state.done || state.payload == nil {
		t.Fatal("unread result not retained for future")
	}
	if err := d.handle(&isolate.Command{Op: workflow.OpAwaitActivity, Payload: []byte("1")}); err != nil {
		t.Fatal(err)
	}
	if !state.retired || state.payload != nil || state.cancel != nil || state.waiter != nil {
		t.Fatal("SDK callback retained consumed activity state")
	}
	e.callback(nil, nil)
	if len(d.pending) != 0 {
		t.Fatal("retired callback completed again")
	}
}
