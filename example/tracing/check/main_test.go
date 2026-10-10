package main

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/mfateev/sdk-go-poc/internal/checktest"
)

func TestNativeTracingInterceptors(t *testing.T) {
	binary := checktest.Build(t, "tracing-check")
	for _, procs := range []string{"1", "2", "8"} {
		t.Run("dispatch-"+procs, func(t *testing.T) {
			run := checktest.Command(t, binary)
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
			run := checktest.Command(t, binary, "-history", history)
			run.Dir = t.TempDir()
			if output, err := run.CombinedOutput(); err != nil {
				t.Fatalf("replay: %v\n%s", err, output)
			}
		})
	}
}
