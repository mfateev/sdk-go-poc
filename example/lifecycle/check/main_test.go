package main

import (
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"
)

func TestWorkerLifecycleAndSDKReplay(t *testing.T) {
	binary := filepath.Join(t.TempDir(), "lifecycle-check")
	build := exec.Command(filepath.Join(runtime.GOROOT(), "bin", "go"), "build", "-o", binary, ".")
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build: %v\n%s", err, output)
	}
	run := exec.Command(binary)
	run.Dir = t.TempDir()
	if output, err := run.CombinedOutput(); err != nil {
		t.Fatalf("lifecycle check: %v\n%s", err, output)
	}
}
