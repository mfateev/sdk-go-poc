package temporalbridge

import (
	"strings"
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

func TestRejectsCustomWorkerDataConverter(t *testing.T) {
	env := &converterEnvironment{converter: converter.NewCompositeDataConverter(converter.NewJSONPayloadConverter())}
	d := &definition{env: env}
	d.OnWorkflowTaskStarted(time.Second)
	if !d.completed || env.err == nil || !strings.Contains(env.err.Error(), "default data converter") {
		t.Fatalf("custom converter result: completed=%t, error=%v", d.completed, env.err)
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
