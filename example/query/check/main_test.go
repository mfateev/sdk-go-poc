package main

import (
	"testing"

	"github.com/mfateev/sdk-go-poc/internal/checktest"
)

func TestCompiledQueriesAndCompletedState(t *testing.T) {
	binary := checktest.Build(t, "query-check")
	run := checktest.Command(t, binary)
	if output, err := run.CombinedOutput(); err != nil {
		t.Fatalf("query check: %v\n%s", err, output)
	}
}

// TestDispatchClose ports internal/internal_coroutines_test.go::TestDispatchClose:
// one root and ten children remain blocked, all appear in the dump, only the
// root has advanced history, and closing drains at least eleven goroutines.
func TestDispatchClose(t *testing.T) {
	binary := checktest.Build(t, "dispatch-close-check")
	if output, err := checktest.Command(t, binary, "-scenario", "dispatch-close").CombinedOutput(); err != nil {
		t.Fatalf("dispatch-close port: %v\n%s", err, output)
	}
}

// TestStackTraceInvalidDepthBounded ports the native equivalent of
// internal/internal_workflow_test.go::TestStackTraceInvalidDepthBounded. Native
// stacks replace the private coroutine frame-trimming API and its three-line
// formatting; shallow and deep stacks must retain useful frames and stay bounded.
func TestStackTraceInvalidDepthBounded(t *testing.T) {
	binary := checktest.Build(t, "stack-depth-check")
	if output, err := checktest.Command(t, binary, "-scenario", "stack-depth").CombinedOutput(); err != nil {
		t.Fatalf("stack-depth port: %v\n%s", err, output)
	}
}
