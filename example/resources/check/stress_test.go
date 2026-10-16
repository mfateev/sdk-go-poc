package main

import (
	"context"
	"os"
	"os/exec"
	"testing"
	"time"

	"github.com/mfateev/sdk-go-poc/internal/checktest"
)

func TestIsolateStress(t *testing.T) {
	if os.Getenv("ISOLATE_STRESS") != "1" {
		t.Skip("set ISOLATE_STRESS=1 for the 10-minute compiled isolate soak")
	}
	binary := checktest.Build(t, "stress")
	ctx, cancel := context.WithTimeout(t.Context(), 12*time.Minute)
	defer cancel()
	cmd := exec.CommandContext(ctx, binary, "-stress", "-duration=10m")
	cmd.Stdout, cmd.Stderr = os.Stdout, os.Stderr
	if err := cmd.Run(); err != nil {
		t.Fatalf("isolate stress: %v (context %v)", err, ctx.Err())
	}
}
