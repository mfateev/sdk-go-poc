package toolchain

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

func TestDriverHelper(t *testing.T) {
	if os.Getenv("TGO_TEST_DRIVER_HELPER") != "1" {
		return
	}
	for i, arg := range os.Args {
		if arg == "--" {
			os.Exit(Main(os.Args[i+1:]))
		}
	}
	os.Exit(2)
}

// Keep the dependency-source regression fixture with the SDK that owns this
// policy. It uses local fake modules and never downloads dependencies.
func TestMetadataSources(t *testing.T) {
	root := os.Getenv("TGO_GOROOT")
	if root == "" {
		t.Skip("set TGO_GOROOT to run toolchain integration checks")
	}
	root, err := filepath.Abs(root)
	if err != nil {
		t.Fatal(err)
	}
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile("testdata/metadata.txt")
	if err != nil {
		t.Fatal(err)
	}
	boundary := regexp.MustCompile(`(?m)^-- ([^\n]+) --\n`)
	matches := boundary.FindAllStringSubmatchIndex(string(data), -1)
	directory := t.TempDir()
	for i, m := range matches {
		end := len(data)
		if i+1 < len(matches) {
			end = matches[i+1][0]
		}
		target := filepath.Join(directory, string(data[m[2]:m[3]]))
		if err := os.MkdirAll(filepath.Dir(target), 0755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(target, data[m[1]:end], 0600); err != nil {
			t.Fatal(err)
		}
	}
	stderr := ""
	for _, line := range strings.Split(string(data[:matches[0][0]]), "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") || line == "[short] skip" {
			continue
		}
		if strings.HasPrefix(line, "stderr ") {
			pattern := strings.Trim(strings.TrimPrefix(line, "stderr "), "'")
			if ok, err := regexp.MatchString(pattern, stderr); err != nil || !ok {
				t.Fatalf("%s: got %s, %v", line, stderr, err)
			}
			continue
		}
		fields := strings.Fields(line)
		if fields[0] == "cp" {
			content, err := os.ReadFile(filepath.Join(directory, fields[1]))
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(directory, fields[2]), content, 0600); err != nil {
				t.Fatal(err)
			}
			continue
		}
		negative := fields[0] == "!"
		if negative {
			fields = fields[1:]
		}
		if fields[0] != "go" {
			t.Fatalf("unsupported fixture command %s", line)
		}
		args := append([]string{"-test.run=^TestDriverHelper$", "--"}, fields[1:]...)
		command := exec.Command(executable, args...)
		command.Dir = directory
		command.Env = append(os.Environ(), "TGO_TEST_DRIVER_HELPER=1", "TGO_GOROOT="+root, "GOFLAGS=", "GOWORK=off", "GOTOOLCHAIN=local")
		output, err := command.CombinedOutput()
		stderr = string(output)
		if (err != nil) != negative {
			t.Fatalf("%s: %v\n%s", line, err, output)
		}
	}
}

func TestDistributionCLI(t *testing.T) {
	root := os.Getenv("TGO_GOROOT")
	if root == "" {
		t.Skip("set TGO_GOROOT to run toolchain integration checks")
	}
	root, err := filepath.Abs(root)
	if err != nil {
		t.Fatal(err)
	}
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	directory := filepath.Join(t.TempDir(), "application with spaces")
	fixture := "../../toolchain/testdata/distribution"
	err = filepath.WalkDir(fixture, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		relative, err := filepath.Rel(fixture, path)
		if err != nil {
			return err
		}
		target := filepath.Join(directory, relative)
		if entry.IsDir() {
			return os.MkdirAll(target, 0755)
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		return os.WriteFile(target, data, 0600)
	})
	if err != nil {
		t.Fatal(err)
	}
	run := func(args ...string) string {
		t.Helper()
		command := exec.Command(executable, append([]string{"-test.run=^TestDriverHelper$", "--"}, args...)...)
		command.Env = append(os.Environ(), "TGO_TEST_DRIVER_HELPER=1", "TGO_GOROOT="+root, "GOROOT=/does-not-exist", "GOWORK=off", "GOFLAGS=")
		output, err := command.CombinedOutput()
		if err != nil {
			t.Fatalf("%v: %v\n%s", args, err, output)
		}
		return string(output)
	}
	output := run("-C", directory, "run", "./host")
	if !strings.Contains(output, "tgo distribution works") {
		t.Fatal(output)
	}
	if output := run("-C", directory, "env", "GOROOT"); strings.TrimSpace(output) != root {
		t.Fatalf("forwarded GOROOT = %q, want %s", output, root)
	}
	jobFile := filepath.Join(directory, "jobs", "jobs.go")
	original, err := os.ReadFile(jobFile)
	if err != nil {
		t.Fatal(err)
	}
	replacement := filepath.Join(directory, "overlay source.txt")
	if err := os.WriteFile(replacement, original, 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(jobFile, []byte(strings.ReplaceAll(string(original), "//temporal:workflow", "// entry supplied by overlay")), 0600); err != nil {
		t.Fatal(err)
	}
	overlay, err := json.Marshal(map[string]any{"Replace": map[string]string{jobFile: replacement}})
	if err != nil {
		t.Fatal(err)
	}
	filename := filepath.Join(directory, "overlay.json")
	if err := os.WriteFile(filename, overlay, 0600); err != nil {
		t.Fatal(err)
	}
	if output := run("run", "-C", directory, "-overlay="+filename, "./host"); !strings.Contains(output, "tgo distribution works") {
		t.Fatal(output)
	}
	if err := os.WriteFile(jobFile, original, 0600); err != nil {
		t.Fatal(err)
	}

	run("-C", directory, "test", "./...")
}
