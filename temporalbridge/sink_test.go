package temporalbridge

import (
	"bytes"
	"isolate"
	"testing"
)

func TestSinkRoutingAndReplayPolicy(t *testing.T) {
	logger := &recordingLogger{}
	env := &logEnvironment{logger: logger}
	var events []SinkEvent
	d := &definition{env: env, resolveSink: func(op uint32) (SinkHandler, SinkOptions) {
		return func(e SinkEvent) { events = append(events, e) }, SinkOptions{Name: "telemetry", EnableReplay: op == 0x10001}
	}}
	for _, replay := range []bool{false, true} {
		env.replay = replay
		for _, op := range []uint32{0x10000, 0x10001} {
			d.handleWrite(&isolate.Message{Op: op, Payload: []byte{0, 255, 7}})
		}
	}
	if len(events) != 3 || events[0].Replay || events[1].Replay || !events[2].Replay || events[2].Operation != 0x10001 {
		t.Fatalf("replay delivery: %+v", events)
	}
	for _, event := range events {
		if event.Name != "telemetry" || event.WorkflowID != "workflow-id" || event.RunID != "run-id" || event.WorkflowType != "Work" || !bytes.Equal(event.Payload, []byte{0, 255, 7}) {
			t.Fatalf("lost bytes or metadata: %+v", event)
		}
	}
	if len(d.immediate) != 0 || len(d.pending) != 0 || d.completed || d.closed {
		t.Fatal("sink delivery changed workflow state")
	}
}

func TestSinkFailuresRemainHostDiagnostics(t *testing.T) {
	logger := &recordingLogger{}
	env := &logEnvironment{logger: logger}
	d := &definition{env: env}
	d.handleWrite(&isolate.Message{Op: 0x10000}) // Unregistered operation.
	d.resolveSink = func(uint32) (SinkHandler, SinkOptions) {
		return func(SinkEvent) { panic("backend unavailable") }, SinkOptions{}
	}
	d.handleWrite(&isolate.Message{Op: 0x10000})
	if len(logger.warnings) != 2 || d.completed || d.closed || len(d.immediate) != 0 || len(d.pending) != 0 {
		t.Fatalf("sink failure changed workflow state: warnings=%v", logger.warnings)
	}
	// The next message must still reach its handler after a backend panic.
	delivered := false
	d.resolveSink = func(uint32) (SinkHandler, SinkOptions) {
		return func(SinkEvent) { delivered = true }, SinkOptions{}
	}
	d.handleWrite(&isolate.Message{Op: 0x10000})
	if !delivered {
		t.Fatal("backend panic stopped subsequent delivery")
	}
}
