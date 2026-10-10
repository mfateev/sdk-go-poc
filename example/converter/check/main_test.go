package main

import (
	"path/filepath"
	"testing"

	"github.com/mfateev/sdk-go-poc/internal/checktest"
)

func TestCompiledWorkerConverter(t *testing.T) {
	binary := checktest.Build(t, "converter-check")
	dir, err := filepath.Abs("../testdata")
	if err != nil {
		t.Fatal(err)
	}
	if out, err := checktest.Command(t, binary, "-history-dir", dir).CombinedOutput(); err != nil {
		t.Fatalf("run: %v\n%s", err, out)
	}
}
