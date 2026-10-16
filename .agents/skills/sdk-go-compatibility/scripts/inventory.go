// Source inventory only: no build, execution, or inferred compatibility.
package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"flag"
	"fmt"
	"go/ast"
	"go/format"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

type paths []string

func (p *paths) String() string     { return strings.Join(*p, ",") }
func (p *paths) Set(s string) error { *p = append(*p, s); return nil }

type snapshot struct {
	Role     string `json:"role"`
	Path     string `json:"path"`
	Revision string `json:"revision"`
	Status   string `json:"status"`
	Digest   string `json:"digest"`
}
type source struct {
	root      int
	pkg, file string
	imports   map[string]string
	ast       *ast.File
}
type declaration struct {
	s   *source
	t   *ast.TypeSpec
	doc string
}
type method struct {
	s *source
	f *ast.FuncDecl
}
type item struct {
	ID          string `json:"id"`
	Kind        string `json:"kind"`
	Signature   string `json:"signature"`
	Doc         string `json:"doc,omitempty"`
	Source      string `json:"source"`
	Line        int    `json:"line"`
	Fingerprint string `json:"fingerprint"`
}
type inventory struct {
	Schema             int        `json:"schema"`
	Snapshots          []snapshot `json:"snapshots"`
	Items              []item     `json:"items"`
	Issues             []string   `json:"issues"`
	ExternalReferences []string   `json:"external_references"`
}
type scanner struct {
	fset             *token.FileSet
	types            map[string][]declaration
	methods          map[string][]method
	files            []*source
	items            map[string]item
	issues, external map[string]bool
}

func fail(err error) { fmt.Fprintln(os.Stderr, err); os.Exit(1) }
func printed(n any) string {
	var b bytes.Buffer
	if field, ok := n.(*ast.Field); ok {
		n = &ast.StructType{Fields: &ast.FieldList{List: []*ast.Field{field}}}
	}
	if err := format.Node(&b, token.NewFileSet(), n); err != nil {
		fail(err)
	}
	return b.String()
}
func digest(s string) string { return fmt.Sprintf("%x", sha256.Sum256([]byte(s))) }
func git(root string, args ...string) string {
	c := exec.Command("git", append([]string{"-C", root}, args...)...)
	b, err := c.Output()
	if err != nil {
		return "unavailable"
	}
	return strings.TrimSpace(string(b))
}
func module(path string) (string, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	for _, l := range strings.Split(string(b), "\n") {
		p := strings.Fields(l)
		if len(p) >= 2 && p[0] == "module" {
			return strings.Trim(p[1], "\""), nil
		}
	}
	return "", fmt.Errorf("no module directive in %s", path)
}
func skipped(name string) bool {
	return strings.HasPrefix(name, ".") || name == "vendor" || name == "node_modules" || name == "bin"
}
func publicPackage(p string) bool {
	for _, x := range strings.Split(p, "/") {
		if x == "internal" || x == "testdata" {
			return false
		}
	}
	return true
}

func (s *scanner) load(root string, index int, role string) (snapshot, error) {
	abs, err := filepath.Abs(root)
	if err != nil {
		return snapshot{}, err
	}
	if _, err := module(filepath.Join(abs, "go.mod")); err != nil {
		return snapshot{}, err
	}
	mods := map[string]string{}
	var files []string
	h := sha256.New()
	err = filepath.WalkDir(abs, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			if path != abs && skipped(d.Name()) {
				return filepath.SkipDir
			}
			return nil
		}
		if d.Type()&os.ModeSymlink != 0 {
			return nil
		}
		rel, _ := filepath.Rel(abs, path)
		if d.Name() == "go.mod" {
			m, err := module(path)
			if err != nil {
				return err
			}
			mods[filepath.Dir(path)] = m
		}
		if strings.HasSuffix(path, ".go") || strings.HasSuffix(path, ".md") || strings.HasSuffix(path, ".json") || strings.HasSuffix(path, ".mod") || strings.HasSuffix(path, ".sum") || d.Name() == "go.work" {
			b, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			fmt.Fprintf(h, "%s\x00%x\n", filepath.ToSlash(rel), sha256.Sum256(b))
		}
		if strings.HasSuffix(path, ".go") && !strings.HasSuffix(path, "_test.go") && !strings.Contains("/"+filepath.ToSlash(rel)+"/", "/testdata/") {
			files = append(files, path)
		}
		return nil
	})
	if err != nil {
		return snapshot{}, err
	}
	for _, path := range files {
		dir := filepath.Dir(path)
		for mods[dir] == "" && dir != filepath.Dir(dir) {
			dir = filepath.Dir(dir)
		}
		if mods[dir] == "" {
			return snapshot{}, fmt.Errorf("no owning module: %s", path)
		}
		rel, _ := filepath.Rel(dir, filepath.Dir(path))
		pkg := mods[dir]
		if rel != "." {
			pkg += "/" + filepath.ToSlash(rel)
		}
		a, err := parser.ParseFile(s.fset, path, nil, parser.ParseComments|parser.AllErrors)
		if err != nil {
			s.issues["parse error (use a parser supporting this build variant): "+err.Error()] = true
			if a == nil {
				continue
			}
		}
		rel, _ = filepath.Rel(abs, path)
		f := &source{root: index, pkg: pkg, file: filepath.ToSlash(rel), imports: map[string]string{}, ast: a}
		for _, im := range a.Imports {
			p, err := strconv.Unquote(im.Path.Value)
			if err != nil {
				return snapshot{}, err
			}
			name := filepath.Base(p)
			if im.Name != nil {
				name = im.Name.Name
			}
			f.imports[name] = p
			if name == "." {
				s.issues[pkg+"@"+f.file+": dot import requires type-checked inventory"] = true
			}
		}
		s.files = append(s.files, f)
		for _, d := range a.Decls {
			switch d := d.(type) {
			case *ast.GenDecl:
				for _, spec := range d.Specs {
					if t, ok := spec.(*ast.TypeSpec); ok {
						doc := d.Doc.Text() + t.Doc.Text() + t.Comment.Text()
						s.types[pkg+"."+t.Name.Name] = append(s.types[pkg+"."+t.Name.Name], declaration{f, t, doc})
					}
				}
			case *ast.FuncDecl:
				if d.Recv != nil && ast.IsExported(d.Name.Name) {
					key := s.named(d.Recv.List[0].Type, f)
					s.methods[key] = append(s.methods[key], method{f, d})
				}
			}
		}
	}
	return snapshot{role, abs, git(abs, "rev-parse", "HEAD"), git(abs, "status", "--porcelain"), fmt.Sprintf("%x", h.Sum(nil))}, nil
}

func (s *scanner) named(e ast.Expr, f *source) string {
	switch e := e.(type) {
	case *ast.StarExpr:
		return s.named(e.X, f)
	case *ast.IndexExpr:
		return s.named(e.X, f)
	case *ast.IndexListExpr:
		return s.named(e.X, f)
	case *ast.ParenExpr:
		return s.named(e.X, f)
	case *ast.Ident:
		return f.pkg + "." + e.Name
	case *ast.SelectorExpr:
		if id, ok := e.X.(*ast.Ident); ok && f.imports[id.Name] != "" {
			return f.imports[id.Name] + "." + e.Sel.Name
		}
	}
	return ""
}
func (s *scanner) canonical(n ast.Node, f *source) string {
	// Rewrite import selectors only while printing; restore the parsed tree.
	old := map[*ast.Ident]string{}
	ast.Inspect(n, func(n ast.Node) bool {
		if sel, ok := n.(*ast.SelectorExpr); ok {
			if id, ok := sel.X.(*ast.Ident); ok && f.imports[id.Name] != "" {
				old[id] = id.Name
				id.Name = f.imports[id.Name]
			}
		}
		return true
	})
	result := printed(n)
	for id, name := range old {
		id.Name = name
	}
	return result
}
func (s *scanner) add(id, kind string, n ast.Node, doc string, f *source) {
	key := id + "@" + f.pkg + "/" + f.file
	sig := s.canonical(n, f)
	// Build directives and filename are retained even on a different host OS.
	var constraints []string
	for _, cg := range f.ast.Comments {
		for _, c := range cg.List {
			if strings.HasPrefix(c.Text, "//go:build") || strings.HasPrefix(c.Text, "// +build") {
				constraints = append(constraints, c.Text)
			}
		}
	}
	x := item{key, kind, sig, doc, f.pkg + "/" + f.file, s.fset.Position(n.Pos()).Line, digest(kind + "\n" + sig + "\n" + doc + "\n" + strings.Join(constraints, "\n"))}
	if prev, ok := s.items[key]; ok && prev.Fingerprint != x.Fingerprint {
		s.issues["conflicting inventory item: "+key] = true
	}
	s.items[key] = x
}
func embedName(e ast.Expr) string {
	switch e := e.(type) {
	case *ast.StarExpr:
		return embedName(e.X)
	case *ast.IndexExpr:
		return embedName(e.X)
	case *ast.IndexListExpr:
		return embedName(e.X)
	case *ast.Ident:
		return e.Name
	case *ast.SelectorExpr:
		return e.Sel.Name
	}
	return printed(e)
}
func (s *scanner) expand(id string, e ast.Expr, f *source, seen map[string]bool, required bool) {
	switch e := e.(type) {
	case *ast.StructType:
		for _, field := range e.Fields.List {
			names := field.Names
			if len(names) == 0 {
				name := embedName(field.Type)
				// Unexported embeds can promote exported methods and fields.
				s.add(id+"."+name, "embedded-field", field, field.Doc.Text()+field.Comment.Text(), f)
				s.expand(id+"."+name, field.Type, f, seen, true)
				continue
			}
			for _, name := range names {
				if ast.IsExported(name.Name) {
					child := id + "." + name.Name
					s.add(child, "field", field, field.Doc.Text()+field.Comment.Text(), f)
					s.expand(child, field.Type, f, seen, false)
				}
			}
		}
	case *ast.InterfaceType:
		for _, field := range e.Methods.List {
			if len(field.Names) == 0 {
				s.add(id+".["+embedName(field.Type)+"]", "embedded-interface", field, field.Doc.Text(), f)
				s.expand(id, field.Type, f, seen, true)
				continue
			}
			for _, name := range field.Names {
				s.add(id+"."+name.Name, "interface-method", field, field.Doc.Text(), f)
			}
		}
	case *ast.StarExpr:
		s.expand(id, e.X, f, seen, required)
	case *ast.ArrayType:
		s.expand(id+"[]", e.Elt, f, seen, false)
	case *ast.MapType:
		s.expand(id+"[value]", e.Value, f, seen, false)
	case *ast.ParenExpr:
		s.expand(id, e.X, f, seen, required)
	default:
		key := s.named(e, f)
		if key == "" {
			return
		}
		decls := s.types[key]
		if len(decls) == 0 {
			if _, ok := e.(*ast.Ident); ok {
				return
			} // Builtins and type parameters.
			s.external[key] = true
			if required {
				s.issues[id+": unresolved alias or embed "+key+"; supply its source with -dependency"] = true
			}
			return
		}
		if seen[key] {
			return
		}
		next := map[string]bool{}
		for k, v := range seen {
			next[k] = v
		}
		next[key] = true
		for _, d := range decls {
			s.expand(id, d.t.Type, d.s, next, required)
		}
		for _, m := range s.methods[key] {
			s.add(id+"."+m.f.Name.Name, "method", m.f.Type, m.f.Doc.Text(), m.s)
		}
	}
}
func sortedKeys(m map[string]bool) []string {
	r := []string{}
	for k := range m {
		r = append(r, k)
	}
	sort.Strings(r)
	return r
}

func main() {
	root := flag.String("root", "", "source tree to inventory")
	out := flag.String("out", "", "JSON output file (required; refuses overwrite)")
	var deps paths
	flag.Var(&deps, "dependency", "source tree for resolving aliases; repeatable")
	flag.Parse()
	if *root == "" || *out == "" {
		flag.Usage()
		os.Exit(2)
	}
	s := scanner{fset: token.NewFileSet(), types: map[string][]declaration{}, methods: map[string][]method{}, items: map[string]item{}, issues: map[string]bool{}, external: map[string]bool{}}
	result := inventory{Schema: 1, Items: []item{}}
	for i, p := range append([]string{*root}, deps...) {
		role := "dependency"
		if i == 0 {
			role = "primary"
		}
		snap, err := s.load(p, i, role)
		if err != nil {
			fail(err)
		}
		result.Snapshots = append(result.Snapshots, snap)
	}
	packageNames := map[string]string{}
	for _, f := range s.files {
		packageNames[f.pkg] = f.ast.Name.Name
	}
	for _, f := range s.files {
		for _, im := range f.ast.Imports {
			p, _ := strconv.Unquote(im.Path.Value)
			if im.Name == nil && packageNames[p] != "" && packageNames[p] != filepath.Base(p) {
				delete(f.imports, filepath.Base(p))
				f.imports[packageNames[p]] = p
			}
		}
	}
	for _, f := range s.files {
		if f.root != 0 || !publicPackage(f.pkg) || f.ast.Name.Name == "main" {
			continue
		}
		for _, d := range f.ast.Decls {
			switch d := d.(type) {
			case *ast.GenDecl:
				for _, spec := range d.Specs {
					switch t := spec.(type) {
					case *ast.TypeSpec:
						if !ast.IsExported(t.Name.Name) {
							continue
						}
						id := f.pkg + "." + t.Name.Name
						s.add(id, "type", t, d.Doc.Text()+t.Doc.Text()+t.Comment.Text(), f)
						s.expand(id, t.Type, f, map[string]bool{f.pkg + "." + t.Name.Name: true}, true)
						for _, m := range s.methods[id] {
							s.add(id+"."+m.f.Name.Name, "method", m.f.Type, m.f.Doc.Text(), m.s)
						}
					case *ast.ValueSpec:
						for _, name := range t.Names {
							if ast.IsExported(name.Name) {
								s.add(f.pkg+"."+name.Name, d.Tok.String(), d, d.Doc.Text()+t.Doc.Text()+t.Comment.Text(), f)
							}
						}
					}
				}
			case *ast.FuncDecl:
				if d.Recv == nil && ast.IsExported(d.Name.Name) {
					s.add(f.pkg+"."+d.Name.Name, "function", d.Type, d.Doc.Text(), f)
				}
			}
		}
	}
	for _, x := range s.items {
		result.Items = append(result.Items, x)
	}
	sort.Slice(result.Items, func(i, j int) bool { return result.Items[i].ID < result.Items[j].ID })
	result.Issues = sortedKeys(s.issues)
	result.ExternalReferences = sortedKeys(s.external)
	b, err := json.MarshalIndent(result, "", "  ")
	if err != nil {
		fail(err)
	}
	f, err := os.OpenFile(*out, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0644)
	if err != nil {
		fail(err)
	}
	if _, err = f.Write(append(b, '\n')); err != nil {
		f.Close()
		fail(err)
	}
	if err = f.Close(); err != nil {
		fail(err)
	}
	fmt.Printf("%d items, %d unresolved issues, %d external references; wrote %s\n", len(result.Items), len(result.Issues), len(result.ExternalReferences), *out)
}
