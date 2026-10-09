package main

import (
	"context"
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"
	"time"
)

func TestCompiledQueriesAndCompletedState(t *testing.T) {
	binary := filepath.Join(t.TempDir(), "query-check")
	build := exec.Command(filepath.Join(runtime.GOROOT(), "bin", "go"), "build", "-o", binary, ".")
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build: %v\n%s", err, output)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	run := exec.CommandContext(ctx, binary)
	if output, err := run.CombinedOutput(); err != nil {
		t.Fatalf("query check: %v\n%s", err, output)
	}
}
