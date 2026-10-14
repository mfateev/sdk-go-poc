// Package checktest supplies independent build and execution budgets for compiled
// isolate integration fixtures. It is used only by host-side tests.
package checktest

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sync"
	"testing"
	"time"
)

type checkerBuild struct {
	binary string
	output []byte
	err    error
	reused bool
}

type buildCache struct {
	mu      sync.Mutex
	dir     string
	entries map[string]checkerBuild
}

var packageBuilds *buildCache

// Main owns checker executables for the whole test package, including parallel
// tests and subtest cleanup. Each Command still launches a fresh process.
func Main(m *testing.M) {
	packageBuilds = &buildCache{entries: make(map[string]checkerBuild)}
	code := m.Run()
	if err := packageBuilds.close(); err != nil {
		fmt.Fprintf(os.Stderr, "checker cleanup: %v\n", err)
		code = 1
	}
	os.Exit(code)
}

func (c *buildCache) close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.dir == "" {
		return nil
	}
	return os.RemoveAll(c.dir)
}

func (c *buildCache) build(ctx context.Context, dir, name string) checkerBuild {
	c.mu.Lock()
	defer c.mu.Unlock()
	if previous, ok := c.entries[dir]; ok {
		previous.reused = true
		return previous
	}
	if c.dir == "" {
		var err error
		c.dir, err = os.MkdirTemp("", "isolate-checkers-")
		if err != nil {
			return checkerBuild{err: err}
		}
	}
	binary := filepath.Join(c.dir, fmt.Sprintf("%d-%s", len(c.entries), name))
	cmd := exec.CommandContext(ctx, filepath.Join(runtime.GOROOT(), "bin", "tgo"), "build", "-o", binary, ".")
	cmd.Dir = dir
	output, err := cmd.CombinedOutput()
	result := checkerBuild{binary: binary, output: output, err: err}
	c.entries[dir] = result
	return result
}

const (
	buildBudget = 10 * time.Minute
	runBudget   = 2 * time.Minute
)

// Build compiles the checker once per directory in a test package. Call Main
// from TestMain; checker sources and the build environment stay fixed during
// the package's tests. Its deadline ends before checker execution begins.
func Build(t *testing.T, name string) string {
	t.Helper()
	if packageBuilds == nil {
		t.Fatal("checker builds require checktest.Main in TestMain")
	}
	dir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), buildBudget)
	defer cancel()
	start := time.Now()
	result := packageBuilds.build(ctx, dir, name)
	if result.err != nil {
		t.Fatalf("checker build after %s (budget %s, context %v): %v\n%s", time.Since(start), buildBudget, ctx.Err(), result.err, result.output)
	}
	if result.reused {
		t.Logf("reused checker %s", result.binary)
	} else {
		t.Logf("checker build completed in %s", time.Since(start))
	}
	return result.binary
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
