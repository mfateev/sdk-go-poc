package main

import (
	"path/filepath"
	"testing"

	"github.com/mfateev/sdk-go-poc/internal/checktest"
)

func TestUpstreamNativeContextPropagation(t *testing.T) {
	binary := checktest.Build(t, "context-propagation-check")
	for _, scenario := range []string{"header-root", "child-derived", "mock-header", "local-header", "context-aware-converter", "per-context-converter", "per-context-only", "factory-error", "nil-propagator", "extract-error"} {
		t.Run(scenario, func(t *testing.T) {
			if out, err := checktest.Command(t, binary, "-scenario", scenario).CombinedOutput(); err != nil {
				t.Fatalf("context propagation: %v\n%s", err, out)
			}
		})
	}
}

func TestNativeContextHistoryReplay(t *testing.T) {
	binary := checktest.Build(t, "context-history-check")
	dir, err := filepath.Abs("../testdata")
	if err != nil {
		t.Fatal(err)
	}
	if out, err := checktest.Command(t, binary, "-history-dir", dir).CombinedOutput(); err != nil {
		t.Fatalf("context history replay: %v\n%s", err, out)
	}
}
