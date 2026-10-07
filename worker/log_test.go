package worker

import "testing"

func TestIsolateLogConfiguration(t *testing.T) {
	w := Wrap(&testWorker{}).(*isolateWorker)
	resolve := w.logs.resolve
	calls := 0
	if err := SetIsolateLogHandler(w, func(e LogEvent) { calls++ }); err != nil {
		t.Fatal(err)
	}
	resolve()(LogEvent{})
	if calls != 1 {
		t.Fatal("configuration was not visible after registration")
	}
	if err := SetIsolateLogHandler(w, nil); err != nil {
		t.Fatal(err)
	}
	if resolve() != nil {
		t.Fatal("nil handler did not restore SDK logging")
	}
	r := NewWorkflowReplayer()
	if err := SetIsolateLogHandler(r, func(LogEvent) {}); err != nil {
		t.Fatal(err)
	}
	if err := SetIsolateLogHandler(&testWorker{}, nil); err == nil {
		t.Fatal("accepted an unconfigured SDK worker")
	}
}
