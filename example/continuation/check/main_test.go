package main

import (
	"context"
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"
	"time"
)

func TestCompiledContinuationAndSavedVersionHistory(t *testing.T) {
	binary := filepath.Join(t.TempDir(), "continuation-check")
	build := exec.Command(filepath.Join(runtime.GOROOT(), "bin", "go"), "build", "-o", binary, ".")
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build: %v\n%s", err, output)
	}
	dir, err := filepath.Abs("../testdata")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	run := exec.CommandContext(ctx, binary, "-history-dir", dir)
	run.Dir = t.TempDir()
	if output, err := run.CombinedOutput(); err != nil {
		t.Fatalf("continuation check: %v\n%s", err, output)
	}
}
