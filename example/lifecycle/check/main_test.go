package main

import (
	"testing"

	"github.com/mfateev/sdk-go-poc/internal/checktest"
)

func TestWorkerLifecycleAndSDKReplay(t *testing.T) {
	binary := checktest.Build(t, "lifecycle-check")
	run := checktest.Command(t, binary)
	run.Dir = t.TempDir()
	if output, err := run.CombinedOutput(); err != nil {
		t.Fatalf("lifecycle check: %v\n%s", err, output)
	}
}
