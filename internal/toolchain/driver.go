// Package toolchain implements the SDK-owned tgo build driver.
package toolchain

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
)

type Module struct {
	Path, Version string
	Replace       *Module
}

type Package struct {
	ImportPath, Name, Dir, ForTest string
	Standard, DepOnly              bool
	GoFiles, CgoFiles, Imports     []string
	Module                         *Module
	Error                          *struct{ Err string }
	dependencies                   []*Package
}

func packageList(roots []*Package) []*Package {
	seen := make(map[*Package]bool)
	var result []*Package
	var visit func(*Package)
	visit = func(p *Package) {
		if seen[p] {
			return
		}
		seen[p] = true
		for _, dep := range p.dependencies {
			visit(dep)
		}
		result = append(result, p)
	}
	for _, root := range roots {
		visit(root)
	}
	return result
}

type config struct {
	FormatVersion    int               `json:"format_version"`
	Entries          []configEntry     `json:"entries"`
	Sources          map[string]string `json:"sources"`
	StatePackages    []string          `json:"state_packages"`
	ReadOnlyPackages []string          `json:"readonly_packages"`
	SupportPackages  []string          `json:"support_packages"`
}

type configEntry struct {
	Package  string `json:"package"`
	ForTest  string `json:"for_test,omitempty"`
	Function string `json:"function"`
	Value    string `json:"value"`
	Invoke   string `json:"invoke"`
}

// Main returns the underlying Go command's exit status. TGO_GOROOT explicitly
// selects a local development toolchain; installed drivers find bin/go beside
// themselves. GOROOT from the caller never redirects the bundled toolchain.
func Main(args []string) int {
	root := os.Getenv("TGO_GOROOT")
	if root == "" {
		executable, err := os.Executable()
		if err != nil {
			return fail(err)
		}
		executable, err = filepath.EvalSymlinks(executable)
		if err != nil {
			return fail(err)
		}
		root = filepath.Dir(filepath.Dir(executable))
	}
	root, err := filepath.Abs(root)
	if err != nil {
		return fail(err)
	}
	goCommand := filepath.Join(root, "bin", "go")
	if _, err = os.Stat(goCommand); err != nil {
		return fail(fmt.Errorf("cannot find isolate toolchain at %s; set TGO_GOROOT to a built Go fork", root))
	}
	env := append(os.Environ(), "GOROOT="+root, "GOTOOLCHAIN=local", "TGO_GOROOT="+root)
	// Keep -C semantics authoritative for discovery and for all later commands.
	if len(args) >= 2 && args[0] == "-C" {
		if err := os.Chdir(args[1]); err != nil {
			return fail(err)
		}
		args = args[2:]
	} else if len(args) > 0 && strings.HasPrefix(args[0], "-C=") {
		if err := os.Chdir(strings.TrimPrefix(args[0], "-C=")); err != nil {
			return fail(err)
		}
		args = args[1:]
	}
	if len(args) == 0 || args[0] != "build" && args[0] != "test" && args[0] != "run" {
		return execute(goCommand, args, env)
	}
	listArgs, err := listArguments(args)
	if err != nil {
		return fail(err)
	}
	command := exec.Command(goCommand, listArgs...)
	command.Env = env
	var output, diagnostics bytes.Buffer
	command.Stdout = &output
	command.Stderr = &diagnostics
	if err = command.Run(); err != nil {
		// Preserve Go's diagnostics without invoking the build twice.
		fmt.Fprint(os.Stderr, diagnostics.String())
		return status(err)
	}
	packages, err := decodePackages(&output)
	if err != nil {
		return fail(err)
	}
	functions, err := discoverIsolateFunctions(packages)
	if err != nil {
		return fail(err)
	}
	if len(functions) == 0 {
		return execute(goCommand, args, env)
	}
	if err = validateDependencies(packages); err != nil {
		return fail(err)
	}
	manifest, err := plan(packages, functions)
	if err != nil {
		return fail(err)
	}
	temporary, err := os.MkdirTemp("", "tgo-build-")
	if err != nil {
		return fail(err)
	}
	defer os.RemoveAll(temporary)
	data, err := json.Marshal(manifest)
	if err != nil {
		return fail(err)
	}
	filename := filepath.Join(temporary, "isolate.json")
	if err = os.WriteFile(filename, data, 0600); err != nil {
		return fail(err)
	}
	if args[0] == "run" {
		// Building a marked executable requires the generic registration entry. Go's
		// run command does not build that entry; run it through the same build path.
		return runProgram(goCommand, args, filename, temporary, env)
	}
	forwarded := append([]string{args[0], "-isolate-config=" + filename}, args[1:]...)
	return execute(goCommand, forwarded, env)
}

func execute(name string, args, env []string) int {
	command := exec.Command(name, args...)
	command.Env = env
	command.Stdin = os.Stdin
	command.Stdout = os.Stdout
	command.Stderr = os.Stderr
	return status(command.Run())
}
func status(err error) int {
	if err == nil {
		return 0
	}
	var exit *exec.ExitError
	if errors.As(err, &exit) {
		return exit.ExitCode()
	}
	fmt.Fprintln(os.Stderr, "tgo:", err)
	return 1
}
func fail(err error) int { fmt.Fprintln(os.Stderr, "tgo:", err); return 1 }

// listArguments passes graph-affecting flags to go list and leaves execution,
// linking and test flags to the command itself. Unknown flags are left to Go.
func listArguments(args []string) ([]string, error) {
	result := []string{"list", "-deps", "-json"}
	if args[0] == "test" {
		result = append(result, "-test")
	}
	graph := map[string]bool{"tags": true, "mod": true, "modfile": true, "overlay": true, "pgo": true, "pkgdir": true, "compiler": true, "buildmode": true, "toolexec": true}
	values := map[string]bool{"o": true, "p": true, "gcflags": true, "asmflags": true, "ldflags": true, "gccgoflags": true, "installsuffix": true, "coverpkg": true, "covermode": true, "exec": true, "isolate-report": true, "timeout": true, "run": true, "bench": true, "benchtime": true, "count": true, "cpu": true, "parallel": true, "shuffle": true, "skip": true, "vet": true, "fuzz": true, "fuzztime": true, "fuzzminimizetime": true, "coverprofile": true, "cpuprofile": true, "memprofile": true, "memprofilerate": true, "blockprofile": true, "blockprofilerate": true, "mutexprofile": true, "mutexprofilefraction": true, "trace": true, "outputdir": true}
	var patterns []string
	for i := 1; i < len(args); i++ {
		arg := args[i]
		if arg == "-args" {
			break
		}
		if !strings.HasPrefix(arg, "-") {
			patterns = append(patterns, arg)
			if args[0] == "run" && !strings.HasSuffix(arg, ".go") {
				break
			}
			continue
		}
		name, _, assigned := strings.Cut(strings.TrimLeft(arg, "-"), "=")
		if name == "isolate-config" {
			return nil, fmt.Errorf("-isolate-config is owned by tgo; use go directly for a generic build manifest")
		}
		if graph[name] || values[name] {
			value := ""
			if !assigned {
				if i+1 == len(args) {
					return nil, fmt.Errorf("flag needs an argument: %s", arg)
				}
				i++
				value = args[i]
			}
			if graph[name] {
				result = append(result, arg)
				if !assigned {
					result = append(result, value)
				}
			}
		} else if slices.Contains([]string{"race", "msan", "asan", "trimpath", "linkshared"}, name) {
			result = append(result, arg)
		}
	}
	if len(patterns) == 0 {
		patterns = []string{"."}
	}
	return append(result, patterns...), nil
}

func decodePackages(reader io.Reader) ([]*Package, error) {
	decoder := json.NewDecoder(reader)
	var packages []*Package
	byPath := make(map[string]*Package)
	for {
		var p Package
		err := decoder.Decode(&p)
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		if p.Error != nil {
			return nil, errors.New(p.Error.Err)
		}
		// Test variants share a source package path. Prefer their expanded files
		// for discovery, but preserve the original import edges for graph selection.
		byPath[p.ImportPath] = &p
		packages = append(packages, &p)
	}
	for _, p := range packages {
		for _, path := range p.Imports {
			if dep := byPath[path]; dep != nil {
				p.dependencies = append(p.dependencies, dep)
			}
		}
		if i := strings.Index(p.ImportPath, " ["); i >= 0 {
			p.ImportPath = p.ImportPath[:i]
		}
	}
	var roots []*Package
	for _, p := range packages {
		if !strings.HasSuffix(p.ImportPath, ".test") {
			roots = append(roots, p)
		}
	}

	return roots, nil
}

func plan(packages []*Package, functions []isolateFunction) (*config, error) {
	result := &config{FormatVersion: 1, Sources: make(map[string]string)}
	var roots, support []*Package
	for _, p := range packages {
		if p.ImportPath == "github.com/mfateev/sdk-go-poc/workflow" {
			support = append(support, p)
			result.SupportPackages = append(result.SupportPackages, p.ImportPath)
		}
	}
	for _, fn := range functions {
		result.Entries = append(result.Entries, configEntry{fn.Package.ImportPath, fn.Package.ForTest, fn.Name, fn.valueName(), fn.invokeName()})
		if !slices.Contains(roots, fn.Package) {
			roots = append(roots, fn.Package)
		}
	}
	for _, p := range roots {
		source, err := isolateInvokerSource(p, functions)
		if err != nil {
			return nil, err
		}
		result.Sources[packageKey(p)] = string(source)
	}
	process := make(map[string]bool)
	all := packageList(append(slices.Clone(roots), support...))
	for _, p := range all {
		switch p.ImportPath {
		case "go.temporal.io/sdk/converter", "go.opentelemetry.io/otel/trace", "go.temporal.io/sdk/activity", "go.temporal.io/sdk/temporal", "go.temporal.io/sdk/workflow", "go.temporal.io/sdk/client", "go.temporal.io/sdk/interceptor", "go.temporal.io/sdk/interceptor/tracing", "go.temporal.io/sdk/testsuite", "go.temporal.io/sdk/worker", "github.com/stretchr/testify/suite":
			for _, dep := range packageList([]*Package{p}) {
				if !dep.Standard {
					process[dep.ImportPath] = true
				}
			}
		}
	}
	for _, p := range all {
		if !slices.Contains(roots, p) && slices.Contains([]string{"github.com/mfateev/sdk-go-poc/testsuite", "github.com/mfateev/sdk-go-poc/worker", "github.com/mfateev/sdk-go-poc/temporalbridge"}, p.ImportPath) {
			continue
		}
		if process[p.ImportPath] && p.ImportPath != "go.temporal.io/sdk/converter" && p.ImportPath != "github.com/google/uuid" && !isolateOwnedObservabilityPackage(p.ImportPath) && !isolateOwnedProtobufPackage(p.ImportPath) {
			continue
		}
		if p.Standard && !isolateOwnedStandardPackages[p.ImportPath] {
			continue
		}
		result.StatePackages = append(result.StatePackages, p.ImportPath)
		if p.Standard || p.ImportPath == "github.com/google/uuid" || isolateOwnedObservabilityPackage(p.ImportPath) || isolateOwnedProtobufPackage(p.ImportPath) || p.ImportPath == "go.temporal.io/sdk/converter" {
			result.ReadOnlyPackages = append(result.ReadOnlyPackages, p.ImportPath)
		}
	}
	slices.Sort(result.StatePackages)
	result.StatePackages = slices.Compact(result.StatePackages)
	slices.Sort(result.ReadOnlyPackages)
	result.ReadOnlyPackages = slices.Compact(result.ReadOnlyPackages)
	return result, nil
}

func runProgram(goCommand string, args []string, manifest, temporary string, env []string) int {
	// Split the package/file list from arguments using Go run's documented shape.
	end := 1
	for end < len(args) && strings.HasPrefix(args[end], "-") {
		flag := args[end]
		end++
		if !strings.Contains(flag, "=") && slices.Contains([]string{"-tags", "-mod", "-modfile", "-overlay", "-gcflags", "-asmflags", "-ldflags", "-p", "-exec"}, flag) {
			end++
		}
	}
	if end >= len(args) {
		return fail(errors.New("run requires a package or Go files"))
	}
	end++
	if strings.HasSuffix(args[end-1], ".go") {
		for end < len(args) && strings.HasSuffix(args[end], ".go") {
			end++
		}
	}
	executable := filepath.Join(temporary, "program")
	build := []string{"build", "-isolate-config=" + manifest, "-o", executable}
	build = append(build, args[1:end]...)
	if code := execute(goCommand, build, env); code != 0 {
		return code
	}
	return execute(executable, args[end:], env)
}

func packageKey(p *Package) string {
	if p.ForTest != "" {
		return p.ImportPath + " [" + p.ForTest + ".test]"
	}
	return p.ImportPath
}
