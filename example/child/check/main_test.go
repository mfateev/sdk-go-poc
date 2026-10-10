package main

import (
	"path/filepath"
	"testing"

	"github.com/mfateev/sdk-go-poc/internal/checktest"
)

func TestCompiledChildWorkflows(t *testing.T) {
	binary := checktest.Build(t, "child-check")
	dir, err := filepath.Abs("../testdata")
	if err != nil {
		t.Fatal(err)
	}
	args := []string{"-history-dir", dir}
	if out, err := checktest.Command(t, binary, args...).CombinedOutput(); err != nil {
		t.Fatalf("child check: %v\n%s", err, out)
	}
}
