package main

import (
	"os"
	"testing"

	"github.com/mfateev/sdk-go-poc/internal/checktest"
)

func TestMain(m *testing.M) { checktest.Main(m) }
func TestLivePluginRegistrations(t *testing.T) {
	if os.Getenv("PLUGIN_DEV_SERVER") != "1" {
		t.Skip("set PLUGIN_DEV_SERVER=1 for plugin native execution and replay")
	}
	binary := checktest.Build(t, "native-plugins")
	if output, err := checktest.Command(t, binary).CombinedOutput(); err != nil {
		t.Fatalf("plugin native execution and replay: %v\n%s", err, output)
	}
}
