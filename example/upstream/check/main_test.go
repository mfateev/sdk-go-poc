package main

import (
	"github.com/mfateev/sdk-go-poc/internal/checktest"
	"testing"
)

func TestUpstreamNativeSDKPorts(t *testing.T) {
	binary := checktest.Build(t, "upstream-check")
	for _, scenario := range []string{
		"unbuffered", "buffered", "blocked-receive", "closed", "send-closed", "async-send-closed", "blocked-send-closed",
		"select-default", "select-blocking", "select-send", "select-async-send", "select-default-blocking",
		"cancel", "cancel-children", "cancel-detach", "cancel-race", "waitgroup", "waitgroup-multiple", "waitgroup-negative",
		"timer", "buffered-timeout", "unbuffered-timeout", "timer-condition", "timer-context-cancel", "timer-cancel",
		"preferred-version", "returned-panic", "future", "future-select", "future-split-join", "future-failure", "defer", "recursive-children",
	} {
		t.Run(scenario, func(t *testing.T) {
			if output, err := checktest.Command(t, binary, "-scenario", scenario).CombinedOutput(); err != nil {
				t.Fatalf("upstream port: %v\n%s", err, output)
			}
		})
	}
}

func TestUpstreamEnvironmentSDKPorts(t *testing.T) {
	binary := checktest.Build(t, "upstream-environment-check")
	for _, scenario := range []string{
		"mock-function", "mock-by-name", "mock-values", "mock-wait", "mock-wait-function", "mock-run", "mock-never",
		"activity-listener", "timer-clock", "clock-activity", "query-nil", "version-mock", "child-completed-listener", "local-listener",
	} {
		t.Run(scenario, func(t *testing.T) {
			if output, err := checktest.Command(t, binary, "-scenario", "environment-"+scenario).CombinedOutput(); err != nil {
				t.Fatalf("upstream environment port: %v\n%s", err, output)
			}
		})
	}
}
