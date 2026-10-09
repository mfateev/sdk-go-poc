package worker

import (
	"sync"
	"testing"
)

func TestSinkRegistration(t *testing.T) {
	w := Wrap(&testWorker{}).(*isolateWorker)
	resolve := w.sinks.resolve // Capture before registration, as the factory does.
	for _, op := range []uint32{0x10000, 0x10001} {
		op := op
		if err := RegisterSink(w, op, func(e SinkEvent) {
			if e.Operation != op {
				t.Errorf("wrong handler for %#x", e.Operation)
			}
			// The configuration lock must be released before calling a handler.
			resolve(op)
		}, SinkOptions{Name: "telemetry", EnableReplay: true}); err != nil {
			t.Fatal(err)
		}
		handler, opts := resolve(op)
		if handler == nil || opts.Name != "telemetry" || !opts.EnableReplay {
			t.Fatalf("registration not visible: %+v", opts)
		}
		handler(SinkEvent{Operation: op})
	}
	if handler, _ := resolve(0x10002); handler != nil {
		t.Fatal("unknown operation resolved")
	}
	r := NewWorkflowReplayer()
	if err := RegisterSink(r, 0x10000, func(SinkEvent) {}); err != nil {
		t.Fatal(err)
	}
	if handler, opts := r.(*isolateReplayer).sinks.resolve(0x10000); handler == nil || opts.EnableReplay {
		t.Fatal("replayer registration or default replay policy incorrect")
	}
}

func TestSinkRegistrationRejectsInvalidConfiguration(t *testing.T) {
	w := Wrap(&testWorker{})
	handler := func(SinkEvent) {}
	for _, op := range []uint32{0, 1, 0xffff, 0xffff0000, 0xffff0001, 0xffffffff} {
		if err := RegisterSink(w, op, handler); err == nil {
			t.Errorf("accepted reserved operation %#x", op)
		}
	}
	if err := RegisterSink(w, 0x10000, nil); err == nil {
		t.Fatal("accepted nil handler")
	}
	if err := RegisterSink(w, 0x10000, handler, SinkOptions{}, SinkOptions{}); err == nil {
		t.Fatal("accepted multiple options")
	}
	if err := RegisterSink(&testWorker{}, 0x10000, handler); err == nil {
		t.Fatal("accepted an ordinary SDK worker")
	}
	if err := RegisterSink(w, 0x10000, handler); err != nil {
		t.Fatal(err)
	}
	if err := RegisterSink(w, 0x10000, handler); err == nil {
		t.Fatal("accepted a duplicate operation")
	}
}

func TestSinkConcurrentRegistrationAndResolution(t *testing.T) {
	w := Wrap(&testWorker{}).(*isolateWorker)
	var wg sync.WaitGroup
	for i := range 32 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			op := uint32(0x10000 + i)
			if err := RegisterSink(w, op, func(SinkEvent) {}); err != nil {
				t.Error(err)
			}
			if handler, _ := w.sinks.resolve(op); handler == nil {
				t.Error("concurrent registration lost")
			}
		}()
	}
	wg.Wait()
}
