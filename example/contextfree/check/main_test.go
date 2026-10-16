package main

import (
	"github.com/mfateev/sdk-go-poc/internal/checktest"
	"os"
	"testing"
)

func TestMain(m *testing.M) { checktest.Main(m) }
func TestLiveOptionalActivityContext(t *testing.T) {
	if os.Getenv("CONTEXT_FREE_DEV_SERVER") != "1" {
		t.Skip("set CONTEXT_FREE_DEV_SERVER=1 for paired SDK/native remote/local activity execution and replay")
	}
	binary := checktest.Build(t, "context-free-activities")
	if output, err := checktest.Command(t, binary).CombinedOutput(); err != nil {
		t.Fatalf("optional activity context: %v\n%s", err, output)
	}
}
