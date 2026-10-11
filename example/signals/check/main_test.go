package main

import (
	"fmt"
	"os"
	"testing"

	"github.com/mfateev/sdk-go-poc/internal/checktest"
)

func TestCompiledProactiveSignalDelivery(t *testing.T) {
	binary := checktest.Build(t, "signals-check")
	for _, procs := range []int{1, 2, 8} {
		t.Run(fmt.Sprint(procs), func(t *testing.T) {
			command := checktest.Command(t, binary)
			command.Env = append(os.Environ(), fmt.Sprintf("GOMAXPROCS=%d", procs))
			if output, err := command.CombinedOutput(); err != nil {
				t.Fatalf("signal delivery: %v\n%s", err, output)
			}
		})
	}
}

func TestLiveProactiveSignalsAndContinueAsNew(t *testing.T) {
	if os.Getenv("SIGNALS_DEV_SERVER") != "1" {
		t.Skip("set SIGNALS_DEV_SERVER=1 to run the local Temporal dev server check")
	}
	binary := checktest.Build(t, "signals-live-check")
	if output, err := checktest.Command(t, binary, "-live").CombinedOutput(); err != nil {
		t.Fatalf("live signal delivery: %v\n%s", err, output)
	}
}
