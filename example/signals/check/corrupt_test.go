package main

import (
	"fmt"
	"os"
	"testing"

	"github.com/mfateev/sdk-go-poc/internal/checktest"
)

func runCorruptPort(t *testing.T, mode string) {
	t.Helper()
	binary := checktest.Build(t, "corrupt-signals-check")
	for _, procs := range []int{1, 2, 8} {
		t.Run(fmt.Sprint(procs), func(t *testing.T) {
			command := checktest.Command(t, binary, "-corrupt-mode", mode)
			command.Env = append(os.Environ(), fmt.Sprintf("GOMAXPROCS=%d", procs))
			if output, err := command.CombinedOutput(); err != nil {
				t.Fatalf("corrupt signal port %s: %v\n%s", mode, err, output)
			}
		})
	}
}

func TestUpstreamCorruptedSignalReceive(t *testing.T)            { runCorruptPort(t, "receive") }
func TestUpstreamCorruptedSignalSelect(t *testing.T)             { runCorruptPort(t, "select") }
func TestUpstreamCorruptedSignalReceiveAsync(t *testing.T)       { runCorruptPort(t, "async") }
func TestUpstreamCorruptedSignalClosedReceiveAsync(t *testing.T) { runCorruptPort(t, "closed-async") }
func TestUpstreamCorruptedSignalClosedReceive(t *testing.T)      { runCorruptPort(t, "closed-receive") }
func TestCorruptedSignalLateRegistration(t *testing.T)           { runCorruptPort(t, "late") }
func TestCorruptedNextSignal(t *testing.T)                       { runCorruptPort(t, "next") }
func TestCorruptedSignalWireOnClosedAdapter(t *testing.T)        { runCorruptPort(t, "closed-wire") }
