package workflow

import (
	"context"
	"errors"
	"testing"
)

func TestUpstreamFutureUnmarshalPointerToPointer(t *testing.T) {
	type record struct{ Value string }
	payload, err := encodeActivityArgs([]any{&record{Value: "MyValue"}})
	if err != nil {
		t.Fatal(err)
	}
	f := &activityFuture{done: make(chan struct{}), payload: payload}
	close(f.done)
	var value record
	var pointer *record
	if err := f.Get(context.Background(), &value); err != nil || value.Value != "MyValue" {
		t.Fatalf("value: %+v %v", value, err)
	}
	if err := f.Get(context.Background(), &pointer); err != nil || pointer == nil || pointer.Value != "MyValue" {
		t.Fatalf("pointer: %+v %v", pointer, err)
	}
	pointer.Value = "changed"
	var again *record
	if err := f.Get(context.Background(), &again); err != nil || again.Value != "MyValue" {
		t.Fatalf("repeated result: %+v %v", again, err)
	}
}

func TestUpstreamFutureFailure(t *testing.T) {
	cause := errors.New("future failure")
	f := &activityFuture{done: make(chan struct{}), err: cause}
	close(f.done)
	for range 2 {
		if err := f.Get(context.Background(), nil); err != cause {
			t.Fatalf("failure identity: %v", err)
		}
		result := receiveFutureResult(t, f.ToChannel())
		if result.Err != cause || result.Value != nil {
			t.Fatalf("channel failure: %+v", result)
		}
	}
}

func TestUpstreamFutureWaitingContextCancellation(t *testing.T) {
	f := &activityFuture{done: make(chan struct{})}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	started, result := make(chan struct{}), make(chan error, 1)
	var value int
	go func() { close(started); result <- f.Get(ctx, &value) }()
	<-started
	select {
	case err := <-result:
		t.Fatalf("canceled waiting context abandoned future: %v", err)
	default:
	}
	if f.IsReady() {
		t.Fatal("canceling a wait completed the operation")
	}
	payload, err := encodeActivityArgs([]any{42})
	if err != nil {
		t.Fatal(err)
	}
	f.payload = payload
	close(f.done)
	if err := <-result; err != nil || value != 42 {
		t.Fatalf("later completion: %d %v", value, err)
	}
}
