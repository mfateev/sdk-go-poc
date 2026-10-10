package main

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"
	"time"
)

func TestNativeTracingInterceptors(t *testing.T) {
	binary := filepath.Join(t.TempDir(), "tracing-check")
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	build := exec.CommandContext(ctx, filepath.Join(runtime.GOROOT(), "bin", "go"), "build", "-o", binary, ".")
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build: %v\n%s", err, output)
	}
	for _, procs := range []string{"1", "2", "8"} {
		t.Run("dispatch-"+procs, func(t *testing.T) {
			run := exec.CommandContext(ctx, binary)
			run.Env = append(os.Environ(), "GOMAXPROCS="+procs)
			run.Dir = t.TempDir()
			if output, err := run.CombinedOutput(); err != nil {
				t.Fatalf("check: %v\n%s", err, output)
			}
		})
	}
	for _, version := range []string{"v1", "v2", "datadog", "opentracing"} {
		t.Run("replay-"+version, func(t *testing.T) {
			history, err := filepath.Abs(filepath.Join("..", "testdata", version+".json"))
			if err != nil {
				t.Fatal(err)
			}
			run := exec.CommandContext(ctx, binary, "-history", history)
			run.Dir = t.TempDir()
			if output, err := run.CombinedOutput(); err != nil {
				t.Fatalf("replay: %v\n%s", err, output)
			}
		})
	}
}
