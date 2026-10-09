package main

import (
	"context"
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"
	"time"
)

func TestCompiledUpdatesAndReadOnlyValidators(t *testing.T) {
	binary := filepath.Join(t.TempDir(), "update-check")
	build := exec.Command(filepath.Join(runtime.GOROOT(), "bin", "go"), "build", "-o", binary, ".")
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build: %v\n%s", err, output)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	history, err := filepath.Abs("../testdata/history.json")
	if err != nil {
		t.Fatal(err)
	}
	if output, err := exec.CommandContext(ctx, binary, "-history", history).CombinedOutput(); err != nil {
		t.Fatalf("update check: %v\n%s", err, output)
	}
}
