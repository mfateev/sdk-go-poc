package checktest

import (
	"os"
	"path/filepath"
	"sync"
	"testing"
)

func TestMain(m *testing.M) { Main(m) }

func TestCheckerSurvivesSubtestCleanup(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "go.mod"), []byte("module example.com/checker\ngo 1.26\n"), 0600); err != nil {
		t.Fatal(err)
	}
	source := filepath.Join(dir, "main.go")
	if err := os.WriteFile(source, []byte("package main\nfunc main() {}\n"), 0600); err != nil {
		t.Fatal(err)
	}
	t.Chdir(dir)
	var binary string
	t.Run("first", func(t *testing.T) {
		binary = Build(t, "first")
		if out, err := Command(t, binary).CombinedOutput(); err != nil {
			t.Fatalf("first checker: %v\n%s", err, out)
		}
	})
	if _, err := os.Stat(binary); err != nil {
		t.Fatalf("subtest cleanup removed shared checker: %v", err)
	}
	// A second build would fail. Scenario names must reuse the existing binary.
	if err := os.WriteFile(source, []byte("invalid Go source"), 0600); err != nil {
		t.Fatal(err)
	}
	var wait sync.WaitGroup
	for range 8 {
		wait.Go(func() {
			if got := Build(t, "another-scenario"); got != binary {
				t.Errorf("checker changed: %s != %s", got, binary)
			}
		})
	}
	wait.Wait()
	if out, err := Command(t, binary).CombinedOutput(); err != nil {
		t.Fatalf("reused checker: %v\n%s", err, out)
	}
}

func TestCheckerDirectoryCleanup(t *testing.T) {
	dir := t.TempDir()
	cache := &buildCache{dir: filepath.Join(dir, "owned-checkers")}
	if err := os.Mkdir(cache.dir, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(cache.dir, "checker"), []byte("artifact"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := cache.close(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(cache.dir); !os.IsNotExist(err) {
		t.Fatalf("checker directory was not removed: %v", err)
	}
	if _, err := os.Stat(dir); err != nil {
		t.Fatalf("cleanup removed parent directory: %v", err)
	}
}
