package toolchain

import (
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
