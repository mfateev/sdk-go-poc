package main

import (
	"bytes"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"
)

func TestCompiledDeterministicRandom(t *testing.T) {
	binary := filepath.Join(t.TempDir(), "random-check")
	if out, err := exec.Command(filepath.Join(runtime.GOROOT(), "bin", "go"), "build", "-o", binary, ".").CombinedOutput(); err != nil {
		t.Fatalf("build: %v\n%s", err, out)
	}
	var baseline []byte
	for _, procs := range []string{"1", "2", "8"} {
		cmd := exec.Command(binary)
		cmd.Dir = t.TempDir()
		cmd.Env = append(os.Environ(), "GOMAXPROCS="+procs, "GOGC=1")
		out, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("procs=%s: %v\n%s", procs, err, out)
		}
		if baseline == nil {
			baseline = out
		} else if !bytes.Equal(out, baseline) {
			t.Fatalf("process trace changed at procs=%s", procs)
		}
		filename, err := filepath.Abs("../testdata/history.json")
		if err != nil {
			t.Fatal(err)
		}
		replay := exec.Command(binary, "-history", filename)
		replay.Dir = t.TempDir()
		replay.Env = cmd.Env
		if output, err := replay.CombinedOutput(); err != nil {
			t.Fatalf("saved history procs=%s: %v\n%s", procs, err, output)
		}
	}
}
