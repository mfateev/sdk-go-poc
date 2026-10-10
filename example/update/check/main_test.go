package main

import (
	"path/filepath"
	"testing"

	"github.com/mfateev/sdk-go-poc/internal/checktest"
)

func TestCompiledUpdatesAndReadOnlyValidators(t *testing.T) {
	binary := checktest.Build(t, "update-check")
	history, err := filepath.Abs("../testdata/history.json")
	if err != nil {
		t.Fatal(err)
	}
	if output, err := checktest.Command(t, binary, "-history", history).CombinedOutput(); err != nil {
		t.Fatalf("update check: %v\n%s", err, output)
	}
}
