// Package testports validates the checked-in Temporal unit-test migration list.
package testports

import (
	"crypto/sha256"
	_ "embed"
	"encoding/json"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

//go:embed inventory.json
var inventory []byte

type Entry struct {
	Source  string   `json:"source"`
	Name    string   `json:"name"`
	Status  string   `json:"status"`
	Reason  string   `json:"reason"`
	Targets []string `json:"targets,omitempty"`
	Feature string   `json:"feature,omitempty"`
}
type Feature struct {
	Description string `json:"description"`
	Decision    string `json:"decision"`
	Request     string `json:"request"`
}
type Manifest struct {
	SourceCommit   string             `json:"source_commit"`
	SDKVersion     string             `json:"sdk_version"`
	SDKReplacement string             `json:"sdk_replacement"`
	Files          map[string]string  `json:"files"`
	Features       map[string]Feature `json:"features"`
	Tests          []Entry            `json:"tests"`
}

func Load() (Manifest, error) {
	var m Manifest
	err := json.Unmarshal(inventory, &m)
	return m, err
}

// Scan includes suite test methods, which go test -list does not enumerate.
// The upstream test/ module contains server integration tests, outside this
// unit-test inventory. Nested contrib/tool unit tests are inventoried too.
func Scan(root string, upstream bool) (map[string]bool, map[string]string, error) {
	tests := map[string]bool{}
	hashes := map[string]string{}
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		relative, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		if d.IsDir() {
			if strings.HasPrefix(d.Name(), ".") && relative != "." || d.Name() == "vendor" || d.Name() == "testdata" || upstream && relative == "test" {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, "_test.go") {
			return nil
		}
		raw, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		file, err := parser.ParseFile(token.NewFileSet(), path, raw, 0)
		if err != nil {
			return err
		}
		relative = filepath.ToSlash(relative)
		hashes[relative] = fmt.Sprintf("%x", sha256.Sum256(raw))
		testingImports := map[string]bool{}
		for _, imported := range file.Imports {
			if imported.Path.Value == `"testing"` {
				name := "testing"
				if imported.Name != nil {
					name = imported.Name.Name
				}
				testingImports[name] = true
			}
		}
		for _, decl := range file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || !strings.HasPrefix(fn.Name.Name, "Test") {
				continue
			}
			name := fn.Name.Name
			if fn.Recv != nil {
				if !upstream {
					continue
				}
				receiver := fn.Recv.List[0].Type
				if ptr, ok := receiver.(*ast.StarExpr); ok {
					receiver = ptr.X
				}
				if id, ok := receiver.(*ast.Ident); ok {
					name = id.Name + "." + name
				} else {
					return fmt.Errorf("unknown receiver in %s: %s", path, name)
				}
			} else {
				if fn.Type.Params.NumFields() != 1 || fn.Type.Results.NumFields() != 0 {
					continue
				}
				ptr, ok := fn.Type.Params.List[0].Type.(*ast.StarExpr)
				if !ok {
					continue
				}
				selector, ok := ptr.X.(*ast.SelectorExpr)
				if !ok || selector.Sel.Name != "T" {
					continue
				}
				imported, ok := selector.X.(*ast.Ident)
				if !ok || !testingImports[imported.Name] {
					continue
				}
			}
			tests[relative+"::"+name] = true
		}
		return nil
	})
	return tests, hashes, err
}

func Audit(m Manifest, root, upstream string, requireNoBlocked bool) (map[string]int, error) {
	targets, _, err := Scan(root, false)
	if err != nil {
		return nil, err
	}
	problems := []string{}
	counts := map[string]int{}
	seen := map[string]bool{}
	if m.SourceCommit == "" || m.SDKVersion == "" {
		problems = append(problems, "missing upstream revision or SDK version")
	}

	if raw, err := os.ReadFile(filepath.Join(root, "go.mod")); err == nil {
		version, replacement := "", ""
		for _, line := range strings.Split(string(raw), "\n") {
			fields := strings.Fields(line)
			if len(fields) >= 2 && fields[0] == "go.temporal.io/sdk" {
				version = fields[1]
			}
			if len(fields) >= 3 && fields[0] == "require" && fields[1] == "go.temporal.io/sdk" {
				version = fields[2]
			}
			if len(fields) >= 5 && fields[0] == "replace" && fields[1] == "go.temporal.io/sdk" && fields[2] == "=>" {
				replacement = strings.Join(fields[3:5], " ")
			}
		}
		if version != m.SDKVersion || replacement != m.SDKReplacement {
			problems = append(problems, "SDK dependency changed; refresh the upstream test inventory")
		}
	}
	for _, entry := range m.Tests {
		id := entry.Source + "::" + entry.Name
		if seen[id] {
			problems = append(problems, "duplicate source test: "+id)
		}
		seen[id] = true
		if _, ok := m.Files[entry.Source]; !ok {
			problems = append(problems, "source file is not pinned: "+id)
		}
		counts[entry.Status]++
		if strings.TrimSpace(entry.Reason) == "" {
			problems = append(problems, "missing disposition reason: "+id)
		}
		switch entry.Status {
		case "covered":
			if len(entry.Targets) == 0 {
				problems = append(problems, "covered test has no target: "+id)
			}
		case "retained", "obsolete", "out_of_scope", "suite":
		case "blocked":
			feature, ok := m.Features[entry.Feature]
			if !ok || feature.Description == "" || feature.Request == "" || (feature.Decision != "user_deferred" && feature.Decision != "awaiting_user") {
				problems = append(problems, "blocked test needs a recorded feature question or user decision: "+id)
			}
			if requireNoBlocked {
				problems = append(problems, "blocked test: "+id)
			}
		case "pending":
			problems = append(problems, "unfinished supported-feature port: "+id)
		default:
			problems = append(problems, "invalid status for "+id+": "+entry.Status)
		}
		for _, target := range entry.Targets {
			if !targets[target] {
				problems = append(problems, "missing runnable coverage target for "+id+": "+target)
			}
		}
	}
	if len(m.Tests) == 0 {
		problems = append(problems, "empty migration list")
	}
	if upstream != "" {
		sourceTests, hashes, err := Scan(upstream, true)
		if err != nil {
			return nil, err
		}
		for id := range sourceTests {
			if !seen[id] {
				problems = append(problems, "unlisted upstream test: "+id)
			}
		}
		for id := range seen {
			if !sourceTests[id] {
				problems = append(problems, "upstream test no longer exists: "+id)
			}
		}
		for path, hash := range hashes {
			if m.Files[path] != hash {
				problems = append(problems, "upstream test file changed; review its entries: "+path)
			}
		}
		for path := range m.Files {
			if _, ok := hashes[path]; !ok {
				problems = append(problems, "upstream test file removed: "+path)
			}
		}
	}
	if len(problems) != 0 {
		sort.Strings(problems)
		return counts, fmt.Errorf("Temporal test-port audit found %d problem(s):\n%s", len(problems), strings.Join(problems, "\n"))
	}
	return counts, nil
}
