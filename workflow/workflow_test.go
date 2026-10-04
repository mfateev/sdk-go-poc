package workflow

import (
	"testing"
	"time"
)

func TestExecuteActivityAsyncReturnsOneErrorAndCloses(t *testing.T) {
	results := ExecuteActivityAsync("", nil, time.Second)
	select {
	case result, ok := <-results:
		if !ok || result.Err == nil || result.Result != nil {
			t.Fatalf("unexpected activity result: %+v, open: %t", result, ok)
		}
	case <-time.After(time.Second):
		t.Fatal("activity result did not arrive")
	}
	if _, ok := <-results; ok {
		t.Fatal("activity result channel did not close")
	}
}
