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

func TestMetadataAPIAndReplay(t *testing.T) {
	binary := filepath.Join(t.TempDir(), "metadata-check")
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	build := exec.CommandContext(ctx, filepath.Join(runtime.GOROOT(), "bin", "go"), "build", "-o", binary, ".")
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build: %v\n%s", err, output)
	}
	for _, procs := range []string{"1", "2", "8"} {
		t.Run("dispatch-"+procs, func(t *testing.T) {
			run := exec.CommandContext(ctx, binary)
			run.Dir = t.TempDir()
			run.Env = append(os.Environ(), "GOMAXPROCS="+procs)
			if output, err := run.CombinedOutput(); err != nil {
				t.Fatalf("check: %v\n%s", err, output)
			}
		})
	}
	if output, err := exec.CommandContext(ctx, binary, "-history", "../testdata/metadata.json").CombinedOutput(); err != nil {
		t.Fatalf("replay: %v\n%s", err, output)
	}
}
