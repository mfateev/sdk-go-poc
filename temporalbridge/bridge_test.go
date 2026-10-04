package temporalbridge

import (
	"testing"

	"github.com/mfateev/sdk-go-poc/workflow"
)

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
