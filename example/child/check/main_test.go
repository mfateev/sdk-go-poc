package main

import (
	"context"
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"
	"time"
)

func TestCompiledChildWorkflows(t *testing.T) {
	binary := filepath.Join(t.TempDir(), "child-check")
	if out, err := exec.Command(filepath.Join(runtime.GOROOT(), "bin", "go"), "build", "-o", binary, ".").CombinedOutput(); err != nil {
		t.Fatalf("build: %v\n%s", err, out)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	dir, err := filepath.Abs("../testdata")
	if err != nil {
		t.Fatal(err)
	}
	args := []string{"-history-dir", dir}
	if out, err := exec.CommandContext(ctx, binary, args...).CombinedOutput(); err != nil {
		t.Fatalf("child check: %v\n%s", err, out)
	}
}
