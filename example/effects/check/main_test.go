package main

import (
	"testing"

	"github.com/mfateev/sdk-go-poc/internal/checktest"
)

func TestWorkerEffectsAndSDKReplay(t *testing.T) {
	binary := checktest.Build(t, "effects-check")
	run := checktest.Command(t, binary)
	run.Dir = t.TempDir()
	if output, err := run.CombinedOutput(); err != nil {
		t.Fatalf("effect/logging check: %v\n%s", err, output)
	}
}
