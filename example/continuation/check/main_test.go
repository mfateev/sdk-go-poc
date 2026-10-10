package main

import (
	"path/filepath"
	"testing"

	"github.com/mfateev/sdk-go-poc/internal/checktest"
)

func TestCompiledContinuationAndSavedVersionHistory(t *testing.T) {
	binary := checktest.Build(t, "continuation-check")
	dir, err := filepath.Abs("../testdata")
	if err != nil {
		t.Fatal(err)
	}
	run := checktest.Command(t, binary, "-history-dir", dir)
	run.Dir = t.TempDir()
	if output, err := run.CombinedOutput(); err != nil {
		t.Fatalf("continuation check: %v\n%s", err, output)
	}
}
