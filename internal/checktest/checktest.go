// Package checktest supplies independent build and execution budgets for compiled
// isolate integration fixtures. It is used only by host-side tests.
package checktest

import (
	"context"
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"
	"time"
)

const (
	buildBudget = 10 * time.Minute
	runBudget   = 2 * time.Minute
)

// Build compiles the checker in the current package with the toolchain running
// the test. Its deadline ends before any checker execution begins.
func Build(t *testing.T, name string) string {
	t.Helper()
	binary := filepath.Join(t.TempDir(), name)
	ctx, cancel := context.WithTimeout(t.Context(), buildBudget)
	defer cancel()
	start := time.Now()
	cmd := exec.CommandContext(ctx, filepath.Join(runtime.GOROOT(), "bin", "go"), "build", "-o", binary, ".")
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("checker build after %s (budget %s, context %v): %v\n%s", time.Since(start), buildBudget, ctx.Err(), err, output)
	}
	t.Logf("checker build completed in %s", time.Since(start))
	return binary
}

// Command gives each checker invocation a fresh execution budget. Callers can
// set Dir and Env before running it. Test cancellation still cancels the command.
func Command(t *testing.T, binary string, args ...string) *exec.Cmd {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), runBudget)
	t.Cleanup(cancel)
	t.Logf("checker execution budget %s: %s %v", runBudget, binary, args)
	return exec.CommandContext(ctx, binary, args...)
}
