package main

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/mfateev/sdk-go-poc/internal/checktest"
)

func TestCompiledDeterministicRandom(t *testing.T) {
	binary := checktest.Build(t, "random-check")
	var baseline []byte
	for _, procs := range []string{"1", "2", "8"} {
		cmd := checktest.Command(t, binary)
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
		replay := checktest.Command(t, binary, "-history", filename)
		replay.Dir = t.TempDir()
		replay.Env = cmd.Env
		if output, err := replay.CombinedOutput(); err != nil {
			t.Fatalf("saved history procs=%s: %v\n%s", procs, err, output)
		}
	}
}
