package temporalbridge

import (
	"context"
	"encoding/json"
	"errors"
	goWorkflow "go.temporal.io/sdk/workflow"
	"isolate"
	"testing"
	"time"

	"github.com/mfateev/sdk-go-poc/workflow"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/sdk/converter"
	bindings "go.temporal.io/sdk/internalbindings"
)

type converterEnvironment struct {
	bindings.WorkflowEnvironment
	converter converter.DataConverter
	err       error
}

func (e *converterEnvironment) GetDataConverter() converter.DataConverter { return e.converter }
func (e *converterEnvironment) Complete(_ *commonpb.Payloads, err error)  { e.err = err }

func TestCustomWorkerDataConverterPreservesRawPayloads(t *testing.T) {
	dc := converter.NewCompositeDataConverter(converter.NewJSONPayloadConverter())
	plain, err := dc.ToPayloads(int64(9007199254740993))
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := encodeTransport(plain, dc)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := decodeTransport(encoded, dc)
	if err != nil {
		t.Fatal(err)
	}
	var got int64
	if err := dc.FromPayloads(decoded, &got); err != nil || got != 9007199254740993 {
		t.Fatalf("value=%d err=%v", got, err)
	}
}

func TestNamedSignalMatching(t *testing.T) {
	d := definition{
		signals: []workflow.Signal{{Name: "other"}, {Name: "complete"}},
		wantSignals: []signalWaiter{
			{name: "complete"},
		},
	}
	if got := d.signalIndex("complete"); got != 1 {
		t.Fatalf("complete signal index = %d, want 1", got)
	}
	if got := d.signalIndex(""); got != 0 {
		t.Fatalf("any signal index = %d, want 0", got)
	}
	if got := d.waiterIndex("other"); got != -1 {
		t.Fatalf("other signal matched complete waiter at index %d", got)
	}
	if !d.hasDeliverableSignal() {
		t.Fatal("complete signal should wake its waiter")
	}
	d.signals = d.signals[:1]
	if d.hasDeliverableSignal() {
		t.Fatal("unrelated signal should remain queued")
	}
}

// Exercise the request flag: a literal name must not be rewritten even if it
// happens to match a registered function alias key.
type activityEnvironment struct {
	bindings.WorkflowEnvironment
	name     string
	callback bindings.ResultHandler
	cancels  int
}

func (e *activityEnvironment) WorkflowInfo() *goWorkflow.Info {
	return &goWorkflow.Info{TaskQueueName: "test"}
}
func (e *activityEnvironment) GenerateSequence() int64 { return 1 }
func (e *activityEnvironment) GetDataConverter() converter.DataConverter {
	return converter.GetDefaultDataConverter()
}
func (e *activityEnvironment) ExecuteActivity(p bindings.ExecuteActivityParams, callback bindings.ResultHandler) bindings.ActivityID {
	e.name = p.ActivityType.Name
	e.callback = callback
	return bindings.ActivityID{}
}
func TestOnlyFunctionReferencesResolveActivityAliases(t *testing.T) {
	for _, function := range []bool{false, true} {
		env := new(activityEnvironment)
		calls := 0
		d := &definition{env: env, resolveActivity: func(name string) string {
			calls++
			if name != "Foo" {
				t.Fatalf("resolver received %q", name)
			}
			return "custom-name"
		}}
		request, err := json.Marshal(workflow.ActivityPayloadRequest{Name: "Foo", Function: function, StartToCloseTimeout: time.Minute})
		if err != nil {
			t.Fatal(err)
		}
		if err := d.handle(&isolate.Command{Op: workflow.OpActivityPayloads, Payload: request}); err != nil {
			t.Fatal(err)
		}
		want := "Foo"
		wantCalls := 0
		if function {
			want = "custom-name"
			wantCalls = 1
		}
		if env.name != want || calls != wantCalls {
			t.Fatalf("function=%t: name=%q, resolver calls=%d", function, env.name, calls)
		}
	}
}

func (e *activityEnvironment) RequestCancelActivity(_ bindings.ActivityID) { e.cancels++ }
func TestCancelActivityRepliesOnceAndIgnoresLateCallback(t *testing.T) {
	env := new(activityEnvironment)
	d := &definition{env: env}
	payload, _ := json.Marshal(workflow.ActivityPayloadRequest{Name: "Foo", StartToCloseTimeout: time.Minute})
	request, _ := json.Marshal(workflow.CallRequest{ID: 1, Op: workflow.OpActivityPayloads, Payload: payload})
	original := &isolate.Command{Op: workflow.OpCancellableCall, Payload: request}
	if err := d.handle(original); err != nil {
		t.Fatal(err)
	}
	cancel := &isolate.Command{Op: workflow.OpCancelCall, Payload: []byte("1")}
	if err := d.handle(cancel); err != nil {
		t.Fatal(err)
	}
	if env.cancels != 1 || len(d.immediate) != 2 || d.immediate[0].command != original || !errors.Is(d.immediate[0].err, context.Canceled) {
		t.Fatalf("cancel replies=%+v requests=%d", d.immediate, env.cancels)
	}
	env.callback(nil, nil)
	if len(d.pending) != 0 || len(d.immediate) != 2 {
		t.Fatal("late activity callback delivered a second result")
	}
}

func TestCancelListenerRegisteredAfterCompletionIsReleased(t *testing.T) {
	d := &definition{completed: true}
	command := &isolate.Command{Op: workflow.OpWorkflowCancel}
	if err := d.handle(command); err != nil {
		t.Fatal(err)
	}
	if d.cancelWaiter != nil || len(d.immediate) != 1 || d.immediate[0].command != command {
		t.Fatal("completed workflow retained a late cancellation listener")
	}
}
