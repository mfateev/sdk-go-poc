package temporalbridge

import (
	"encoding/json"
	"errors"
	"isolate"
	"testing"
	"time"

	"github.com/mfateev/sdk-go-poc/workflow"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/sdk/converter"
	bindings "go.temporal.io/sdk/internalbindings"
	"google.golang.org/protobuf/proto"
)

type continuationEnvironment struct {
	bindings.WorkflowEnvironment
	err     error
	request workflow.VersionRequest
}

func (e *continuationEnvironment) Complete(_ *commonpb.Payloads, err error) { e.err = err }
func (e *continuationEnvironment) GetVersion(id string, min, max workflow.Version) workflow.Version {
	e.request = workflow.VersionRequest{ChangeID: id, Min: min, Max: max}
	return 2
}
func (*continuationEnvironment) IsReplaying() bool { return true }
func (*continuationEnvironment) GetDataConverter() converter.DataConverter {
	return converter.GetDefaultDataConverter()
}

func TestVersionDelegatesHistoryAndBoundsToSDK(t *testing.T) {
	e := new(continuationEnvironment)
	d := &definition{env: e}
	raw, _ := json.Marshal(workflow.VersionRequest{ChangeID: "change", Min: workflow.DefaultVersion, Max: 2})
	if err := d.handle(&isolate.Command{Op: workflow.OpGetVersion, Payload: raw}); err != nil {
		t.Fatal(err)
	}
	if e.request.ChangeID != "change" || e.request.Min != workflow.DefaultVersion || string(d.immediate[0].payload) != "2" {
		t.Fatalf("version request: %+v", e.request)
	}
	if err := d.handle(&isolate.Command{Op: workflow.OpIsReplaying}); err != nil {
		t.Fatal(err)
	}
	if string(d.immediate[1].payload) != "true" {
		t.Fatal("replay state not delegated")
	}
}
func TestContinueAsNewRetiresDefinitionAndPreservesSDKFields(t *testing.T) {
	dc := converter.GetDefaultDataConverter()
	input, _ := dc.ToPayloads(42)
	raw, _ := json.Marshal(workflow.PayloadCompletion{ContinueAsNew: &workflow.ContinueAsNewRequest{Name: "target", Payloads: mustPayloads(t, input), Options: workflow.RunOptions{TaskQueue: "next", WorkflowRunTimeout: time.Minute, WorkflowTaskTimeout: time.Second}, RetryPolicy: &workflow.RetryPolicy{MaximumAttempts: 3}, BackoffStartInterval: int64(time.Second)}})
	e := new(continuationEnvironment)
	d := &definition{env: e}
	if err := d.handle(&isolate.Command{Op: workflow.OpCompletePayloads, Payload: raw}); err != nil {
		t.Fatal(err)
	}
	var continuation *bindings.ContinueAsNewError
	if !errors.As(e.err, &continuation) || !d.closed || !d.completed || continuation.WorkflowType.Name != "target" || continuation.TaskQueueName != "next" || continuation.RetryPolicy.MaximumAttempts != 3 || continuation.BackoffStartInterval != time.Second {
		t.Fatalf("continuation: %+v", e.err)
	}
	var value int
	if err := dc.FromPayloads(continuation.Input, &value); err != nil || value != 42 {
		t.Fatalf("arguments: %d %v", value, err)
	}
}
func TestEmptyErrorRemainsFailureAcrossCompletionBoundary(t *testing.T) {
	e := new(continuationEnvironment)
	d := &definition{env: e}
	if err := d.handle(&isolate.Command{Op: workflow.OpCompletePayloads, Payload: []byte(`{"failed":true}`)}); err != nil {
		t.Fatal(err)
	}
	if e.err == nil || e.err.Error() != "" {
		t.Fatalf("empty-message failure changed: %v", e.err)
	}
}

func mustPayloads(t *testing.T, p *commonpb.Payloads) []byte {
	t.Helper()
	raw, err := proto.Marshal(p)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}
