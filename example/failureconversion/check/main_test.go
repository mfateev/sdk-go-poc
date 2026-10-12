package main

import (
	"github.com/mfateev/sdk-go-poc/internal/checktest"
	"path/filepath"
	"testing"
)

func TestUpstreamCustomFailureConverter(t *testing.T) {
	binary := checktest.Build(t, "custom-failure-check")
	for _, scenario := range []string{"activity", "local", "child", "workflow", "updates", "updates-context", "fallback", "factory-error", "nil-converter"} {
		t.Run(scenario, func(t *testing.T) {
			if out, err := checktest.Command(t, binary, "-scenario", scenario).CombinedOutput(); err != nil {
				t.Fatalf("custom failure conversion: %v\n%s", err, out)
			}
		})
	}
}

func TestCustomFailureHistoryReplay(t *testing.T) {
	binary := checktest.Build(t, "failure-history-check")
	dir, err := filepath.Abs("../testdata")
	if err != nil {
		t.Fatal(err)
	}
	if out, err := checktest.Command(t, binary, "-history-dir", dir).CombinedOutput(); err != nil {
		t.Fatalf("failure replay: %v\n%s", err, out)
	}
}
