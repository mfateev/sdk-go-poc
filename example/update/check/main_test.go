package main

import (
	"os"
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

func TestLivePriorityUpdatesAndReplay(t *testing.T) {
	if os.Getenv("UPDATES_DEV_SERVER") != "1" {
		t.Skip("set UPDATES_DEV_SERVER=1 to run the local Temporal dev server check")
	}
	binary := checktest.Build(t, "updates-live-check")
	if output, err := checktest.Command(t, binary, "-dev-server").CombinedOutput(); err != nil {
		t.Fatalf("live priority updates: %v\n%s", err, output)
	}
}
