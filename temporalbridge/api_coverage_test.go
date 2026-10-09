package temporalbridge

import (
	"context"
	"encoding/json"
	"errors"
	"isolate"
	"reflect"
	"testing"
	"time"

	"github.com/mfateev/sdk-go-poc/workflow"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/sdk/converter"
	bindings "go.temporal.io/sdk/internalbindings"
	"go.temporal.io/sdk/temporal"
	goWorkflow "go.temporal.io/sdk/workflow"
)

type coverageEnvironment struct {
	activityEnvironment
	local                                bindings.ExecuteLocalActivityParams
	localCallback                        bindings.LocalActivityResultHandler
	nexusParams                          bindings.ExecuteNexusOperationParams
	nexusCallback                        func(*commonpb.Payload, error)
	nexusStarted                         func(string, error)
	externalCallback                     bindings.ResultHandler
	target                               []string
	childOnly                            bool
	localCancels, nexusCancels, abandons int
	open                                 map[string]*goWorkflow.SessionInfo
}

func (*coverageEnvironment) WorkflowInfo() *goWorkflow.Info {
	return &goWorkflow.Info{Namespace: "default", TaskQueueName: "test", WorkflowExecution: goWorkflow.Execution{ID: "workflow"}, OriginalRunID: "original-run"}
}

func (e *coverageEnvironment) SignalExternalWorkflow(ns, id, run, name string, p *commonpb.Payloads, _ any, _ *commonpb.Header, childOnly bool, cb bindings.ResultHandler) {
	e.target, e.childOnly, e.externalCallback = []string{ns, id, run, name}, childOnly, cb
	var value int64
	if err := e.GetDataConverter().FromPayloads(p, &value); err != nil || value != 9007199254740993 {
		panic("external argument precision lost")
	}
}
func (e *coverageEnvironment) RequestCancelExternalWorkflow(ns, id, run string, cb bindings.ResultHandler) {
	e.target, e.externalCallback = []string{ns, id, run}, cb
}
func (e *coverageEnvironment) ExecuteLocalActivity(p bindings.ExecuteLocalActivityParams, cb bindings.LocalActivityResultHandler) bindings.LocalActivityID {
	e.local, e.localCallback = p, cb
	return bindings.LocalActivityID{}
}
func (e *coverageEnvironment) RequestCancelLocalActivity(bindings.LocalActivityID) {
	e.localCancels++
	e.localCallback(&bindings.LocalActivityResultWrapper{Err: temporal.NewCanceledError()})
}
func (e *coverageEnvironment) ExecuteNexusOperation(p bindings.ExecuteNexusOperationParams, cb func(*commonpb.Payload, error), started func(string, error)) int64 {
	e.nexusParams, e.nexusCallback, e.nexusStarted = p, cb, started
	return 17
}
func (e *coverageEnvironment) RequestCancelNexusOperation(seq int64) {
	if seq != 17 {
		panic("wrong Nexus sequence")
	}
	e.nexusCancels++
}
func (e *coverageEnvironment) AbandonNexusOperation(seq int64) {
	if seq != 17 {
		panic("wrong Nexus sequence")
	}
	e.abandons++
}
func (e *coverageEnvironment) AddSession(s *goWorkflow.SessionInfo) {
	if e.open == nil {
		e.open = make(map[string]*goWorkflow.SessionInfo)
	}
	e.open[s.SessionID] = s
}
func (e *coverageEnvironment) RemoveSession(id string) { delete(e.open, id) }
func coverageCommand(t *testing.T, op uint32, request any) *isolate.Command {
	t.Helper()
	p, err := json.Marshal(request)
	if err != nil {
		t.Fatal(err)
	}
	return &isolate.Command{Op: op, Payload: p}
}
func coveragePayload(t *testing.T, values ...any) []byte {
	t.Helper()
	p, err := converter.GetDefaultDataConverter().ToPayloads(values...)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := inboundPayloadBytes(p, converter.GetDefaultDataConverter())
	if err != nil {
		t.Fatal(err)
	}
	return raw
}
func TestExternalTargetAndLateCallbacks(t *testing.T) {
	for _, cancel := range []bool{false, true} {
		e := new(coverageEnvironment)
		d := &definition{env: e}
		r := workflow.ExternalRequest{ID: 1, Namespace: "other", WorkflowID: "target", RunID: "run", SignalName: "signal", Cancel: cancel, Payloads: coveragePayload(t, int64(9007199254740993))}
		if err := d.handle(coverageCommand(t, workflow.OpScheduleExternal, r)); err != nil {
			t.Fatal(err)
		}
		if e.childOnly || !reflect.DeepEqual(e.target[:3], []string{"other", "target", "run"}) {
			t.Fatal("external target changed")
		}
		if err := d.handle(coverageCommand(t, workflow.OpAwaitExternal, uint64(1))); err != nil {
			t.Fatal(err)
		}
		state := d.external[1]
		d.Close()
		e.externalCallback(nil, errors.New("late"))
		if !state.retired || state.waiter != nil || state.payload != nil || len(d.pending) != 0 {
			t.Fatal("late external callback revived state")
		}
	}
}
func TestLocalTypesOptionsBackoffAndCancel(t *testing.T) {
	fn := func(context.Context, int64) (int64, error) { panic("must execute on SDK local worker") }
	e := new(coverageEnvironment)
	d := &definition{env: e, resolveLocalActivity: func(string) any { return fn }, resolveActivity: func(string) string { return "alias" }}
	r := workflow.LocalActivityRequest{ID: 1, Name: "fn", Function: true, Payloads: coveragePayload(t, int64(9007199254740993)), Attempt: 3, ScheduledTime: time.Unix(100, 0), Options: workflow.LocalActivityOptions{StartToCloseTimeout: time.Second, ScheduleToCloseTimeout: time.Minute, RetryPolicy: &temporal.RetryPolicy{MaximumAttempts: 5}, Summary: "local"}}
	if err := d.handle(coverageCommand(t, workflow.OpScheduleLocal, r)); err != nil {
		t.Fatal(err)
	}
	if e.local.ActivityType != "alias" || e.local.InputArgs[0] != int64(9007199254740993) || e.local.Attempt != 3 || !e.local.ScheduledTime.Equal(r.ScheduledTime) || e.local.RetryPolicy.MaximumAttempts != 5 {
		t.Fatal("local types/options lost")
	}
	e.localCallback(&bindings.LocalActivityResultWrapper{Err: temporal.NewApplicationError("retry", "retry"), Backoff: 2 * time.Second, Attempt: 3})
	var outcome workflow.LocalActivityOutcome
	if err := json.Unmarshal(d.locals[1].payload, &outcome); err != nil || outcome.Backoff != 2*time.Second || outcome.Attempt != 3 {
		t.Fatalf("retry outcome: %+v %v", outcome, err)
	}
	r.ID = 2
	if err := d.handle(coverageCommand(t, workflow.OpScheduleLocal, r)); err != nil {
		t.Fatal(err)
	}
	if err := d.handle(coverageCommand(t, workflow.OpCancelLocal, uint64(2))); err != nil {
		t.Fatal(err)
	}
	if err := d.handle(coverageCommand(t, workflow.OpCancelLocal, uint64(2))); err != nil {
		t.Fatal(err)
	}
	if e.localCancels != 1 || !d.locals[2].done {
		t.Fatal("local cancel not exactly once")
	}
	s := d.locals[2]
	d.Close()
	e.localCallback(&bindings.LocalActivityResultWrapper{Backoff: time.Hour, Err: errors.New("late")})
	if !s.retired || s.payload != nil || s.cancel != nil {
		t.Fatal("local state retained after close")
	}
}
func TestNexusTwoFuturesAndCancellationPolicies(t *testing.T) {
	for _, policy := range []workflow.NexusOperationCancellationType{workflow.NexusOperationCancellationTypeAbandon, workflow.NexusOperationCancellationTypeTryCancel, workflow.NexusOperationCancellationTypeWaitRequested, workflow.NexusOperationCancellationTypeWaitCompleted} {
		e := new(coverageEnvironment)
		d := &definition{env: e}
		r := workflow.NexusRequest{ID: 1, Endpoint: "endpoint", Service: "service", Operation: "op", Payloads: coveragePayload(t, "input"), Options: workflow.NexusOperationOptions{CancellationType: policy, ScheduleToCloseTimeout: time.Minute}}
		if err := d.handle(coverageCommand(t, workflow.OpScheduleNexus, r)); err != nil {
			t.Fatal(err)
		}
		e.nexusStarted("token", nil)
		if !d.nexus[1].started || d.nexus[1].done {
			t.Fatal("Nexus start completed result")
		}
		if err := d.handle(coverageCommand(t, workflow.OpCancelNexus, uint64(1))); err != nil {
			t.Fatal(err)
		}
		if err := d.handle(coverageCommand(t, workflow.OpCancelNexus, uint64(1))); err != nil {
			t.Fatal(err)
		}
		if policy == workflow.NexusOperationCancellationTypeAbandon {
			if e.abandons != 1 || e.nexusCancels != 0 || !d.nexus[1].done {
				t.Fatal("abandon policy lost")
			}
		} else {
			if e.nexusCancels != 1 || e.abandons != 0 || d.nexus[1].done {
				t.Fatal("SDK cancellation not delegated")
			}
			e.nexusCallback(nil, temporal.NewCanceledError())
		}
		s := d.nexus[1]
		d.Close()
		e.nexusStarted("late", nil)
		e.nexusCallback(nil, errors.New("late"))
		if !s.retired || s.start != nil || s.result != nil || len(d.pending) != 0 {
			t.Fatal("late Nexus callbacks revived execution")
		}
	}
}
func TestSessionHostMetadata(t *testing.T) {
	e := new(coverageEnvironment)
	d := &definition{env: e}
	if err := d.handle(&isolate.Command{Op: workflow.OpSessionID}); err != nil {
		t.Fatal(err)
	}
	id := string(d.immediate[0].payload)
	if id == "" {
		t.Fatal("empty session identity")
	}
	if err := d.handle(coverageCommand(t, workflow.OpAddSession, workflow.SessionInfo{SessionID: id, HostName: "host", SessionState: workflow.SessionStateOpen})); err != nil {
		t.Fatal(err)
	}
	if e.open[id].HostName != "host" {
		t.Fatal("session metadata lost")
	}
	if err := d.handle(&isolate.Command{Op: workflow.OpRemoveSession, Payload: []byte(id)}); err != nil || len(e.open) != 0 {
		t.Fatal("session removal failed")
	}
}

type sessionIdentityEnvironment struct {
	coverageEnvironment
	workflowID string
}

func (e *sessionIdentityEnvironment) WorkflowInfo() *goWorkflow.Info {
	return &goWorkflow.Info{WorkflowExecution: goWorkflow.Execution{ID: e.workflowID}, OriginalRunID: "original-run"}
}
func TestSessionIdentitySurvivesStandaloneReplay(t *testing.T) {
	var ids []string
	for _, workflowID := range []string{"actual-workflow", "ReplayId"} {
		e := &sessionIdentityEnvironment{workflowID: workflowID}
		d := &definition{env: e}
		if err := d.handle(&isolate.Command{Op: workflow.OpSessionID}); err != nil {
			t.Fatal(err)
		}
		ids = append(ids, string(d.immediate[0].payload))
	}
	if ids[0] != ids[1] {
		t.Fatal("synthetic replay identity changed session signal name")
	}
}
func TestMissingLocalRegistrationResolvesFuture(t *testing.T) {
	e := new(coverageEnvironment)
	d := &definition{env: e, resolveLocalActivity: func(string) any { return nil }}
	r := workflow.LocalActivityRequest{ID: 1, Name: "missing", Attempt: 1, Options: workflow.LocalActivityOptions{StartToCloseTimeout: time.Second, ScheduleToCloseTimeout: time.Second}}
	if err := d.handle(coverageCommand(t, workflow.OpScheduleLocal, r)); err != nil {
		t.Fatal(err)
	}
	var outcome workflow.LocalActivityOutcome
	if err := json.Unmarshal(d.locals[1].payload, &outcome); err != nil || !outcome.Failed {
		t.Fatal("missing implementation did not resolve error future")
	}
	if d.closed || d.completed || e.local.ActivityType != "" {
		t.Fatal("missing activity ended workflow or scheduled host work")
	}
}
