package main

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/mfateev/sdk-go-poc/internal/checktest"
)

func TestDynamicHistoryReplay(t *testing.T) {
	binary := checktest.Build(t, "dynamic-replay")
	histories, err := filepath.Abs("../testdata")
	if err != nil {
		t.Fatal(err)
	}
	for _, procs := range []int{1, 2, 8} {
		t.Run(fmt.Sprint(procs), func(t *testing.T) {
			command := checktest.Command(t, binary, "-history-dir", histories)
			command.Env = append(os.Environ(), fmt.Sprintf("GOMAXPROCS=%d", procs))
			if output, err := command.CombinedOutput(); err != nil {
				t.Fatalf("dynamic replay: %v\n%s", err, output)
			}
		})
	}
}
func TestLiveDynamicHandlers(t *testing.T) {
	if os.Getenv("DYNAMIC_DEV_SERVER") != "1" {
		t.Skip("set DYNAMIC_DEV_SERVER=1 for live execution and replay")
	}
	binary := checktest.Build(t, "dynamic-live")
	if output, err := checktest.Command(t, binary, "-live", "-history-dir", t.TempDir()).CombinedOutput(); err != nil {
		t.Fatalf("live dynamic handlers: %v\n%s", err, output)
	}
}
