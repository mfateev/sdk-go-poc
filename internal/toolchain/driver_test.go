package toolchain

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestDirectiveDiagnostics(t *testing.T) {
	for _, test := range []struct{ name, source, want string }{
		{"generic", "//temporal:workflow\nfunc Work[T any](value T) error{return nil}", "requires a concrete, top-level function"},
		{"variadic", "//temporal:workflow\nfunc Work(values ...string) error{return nil}", "does not support variadic arguments"},
		{"placement", "//temporal:workflow\nvar Work=1", "must immediately precede a function"},
		{"arguments", "//temporal:workflow extra\nfunc Work()error{return nil}", "expected one //temporal:workflow"},
		{"duplicate", "//temporal:workflow\n//temporal:workflow\nfunc Work()error{return nil}", "expected one //temporal:workflow"},
		{"result", "//temporal:workflow\nfunc Work()int{return 0}", "requires no return values, error, or (result, error)"},
	} {
		t.Run(test.name, func(t *testing.T) {
			directory := t.TempDir()
			if err := os.WriteFile(filepath.Join(directory, "entry.go"), []byte("package jobs\n"+test.source), 0600); err != nil {
				t.Fatal(err)
			}
			_, err := discoverIsolateFunctions([]*Package{{ImportPath: "example.org/jobs", Name: "jobs", Dir: directory, GoFiles: []string{"entry.go"}}})
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("got %v, want %s", err, test.want)
			}
		})
	}
}

func TestListArguments(t *testing.T) {
	for _, test := range []struct {
		args []string
		want string
	}{
		{[]string{"test", "-race", "-tags=integration", "-run", "TestOnly", "-timeout=2m", "./a", "./b", "-args", "-application"}, "list -deps -json -test -race -tags=integration ./a ./b"},
		{[]string{"build", "-mod", "mod", "-o", "with spaces/output", "-gcflags=all=-N -l", "./host"}, "list -deps -json -mod mod ./host"},
		{[]string{"run", "./host", "arg"}, "list -deps -json ./host"},
		{[]string{"test", "-c", "-o", "a.test", "./a"}, "list -deps -json -test ./a"},
	} {
		got, err := listArguments(test.args)
		if err != nil || strings.Join(got, " ") != test.want {
			t.Fatalf("%v: got %v, %v; want %s", test.args, got, err, test.want)
		}
	}
}

func TestTestPackageAdapters(t *testing.T) {
	source := `{"ImportPath":"example.org/a","Name":"a","GoFiles":["a.go"]}
{"ImportPath":"example.org/a [example.org/a.test]","Name":"a","ForTest":"example.org/a","GoFiles":["a.go","a_test.go"]}
{"ImportPath":"example.org/a_test [example.org/a.test]","Name":"a_test","ForTest":"example.org/a","GoFiles":["external_test.go"],"Imports":["example.org/a [example.org/a.test]"]}`
	packages, err := decodePackages(strings.NewReader(source))
	if err != nil {
		t.Fatal(err)
	}
	if len(packages) != 3 {
		t.Fatal(packages)
	}
	if packageKey(packages[0]) == packageKey(packages[1]) {
		t.Fatal("test declarations would be injected into an ordinary dependency")
	}
	if packages[2].dependencies[0] != packages[1] {
		t.Fatal("external test import lost its internal test variant")
	}
}
