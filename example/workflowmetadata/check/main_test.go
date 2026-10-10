package main

import (
	"os"
	"testing"

	"github.com/mfateev/sdk-go-poc/internal/checktest"
)

func TestMetadataAPIAndReplay(t *testing.T) {
	binary := checktest.Build(t, "metadata-check")
	for _, procs := range []string{"1", "2", "8"} {
		t.Run("dispatch-"+procs, func(t *testing.T) {
			run := checktest.Command(t, binary)
			run.Dir = t.TempDir()
			run.Env = append(os.Environ(), "GOMAXPROCS="+procs)
			if output, err := run.CombinedOutput(); err != nil {
				t.Fatalf("check: %v\n%s", err, output)
			}
		})
	}
	if output, err := checktest.Command(t, binary, "-history", "../testdata/metadata.json").CombinedOutput(); err != nil {
		t.Fatalf("replay: %v\n%s", err, output)
	}
}
