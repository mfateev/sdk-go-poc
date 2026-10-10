package main

import (
	"context"
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"
	"time"
)

func TestCompiledAPICoverageAndReplay(t *testing.T) {
	binary := filepath.Join(t.TempDir(), "api-check")
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	if out, err := exec.CommandContext(ctx, filepath.Join(runtime.GOROOT(), "bin", "go"), "build", "-o", binary, ".").CombinedOutput(); err != nil {
		t.Fatalf("build: %v\n%s", err, out)
	}
	dir, err := filepath.Abs("../testdata")
	if err != nil {
		t.Fatal(err)
	}
	for _, adapter := range []string{"", "v1", "v2", "datadog"} {
		t.Run("tracing-"+adapter, func(t *testing.T) {
			if out, err := exec.CommandContext(ctx, binary, "-history-dir", dir, "-tracing", adapter).CombinedOutput(); err != nil {
				t.Fatalf("API coverage/replay: %v\n%s", err, out)
			}
		})
	}
}
