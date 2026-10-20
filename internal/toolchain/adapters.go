package toolchain

import (
	"bytes"
	"encoding/json"
	"fmt"
	"go/ast"
	"go/format"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"golang.org/x/mod/modfile"
)

// libraryOverlay applies SDK determinism policy without host mediation.
// Protobuf and generated messages execute as ordinary private dependencies.
func libraryOverlay(packages []*Package, directory, workspace string, existing map[string]string) (string, string, error) {
	replacements := make(map[string]string)
	for k, v := range existing {
		replacements[k] = v
	}
	seen := make(map[string]bool)
	mirrors := make(map[string]string)
	for _, p := range packages {
		if p.Standard || !(p.ImportPath == OTelTraceModule || p.ImportPath == ProtobufModule+"/internal/detrand" || p.ImportPath == ProtobufModule+"/internal/impl" || p.ImportPath == ProtobufModule+"/reflect/protoregistry" || p.ImportPath == ProtobufModule+"/proto") {
			continue
		}
		for _, name := range p.GoFiles {
			original := filepath.Join(p.Dir, name)
			if seen[original] {
				continue
			}
			seen[original] = true
			data, err := p.readSource(original)
			if err != nil {
				return "", "", err
			}
			adapted, err := adaptLocalLibrary(p.ImportPath, name, data)
			if err != nil {
				return "", "", fmt.Errorf("adapting %s: %w", original, err)
			}
			if bytes.Equal(adapted, data) {
				continue
			}
			if p.Module != nil && !p.Module.Main && p.Module.Dir != "" && (p.Module.Replace == nil || p.Module.Replace.Version != "") {
				mirror := mirrors[p.Module.Path]
				if mirror == "" {
					mirror = filepath.Join(directory, fmt.Sprintf("module-%d", len(mirrors)))
					if err = copyModule(p.Module.Dir, mirror, existing); err != nil {
						return "", "", err
					}
					mirrors[p.Module.Path] = mirror
				}
				relative, err := filepath.Rel(p.Module.Dir, original)
				if err != nil {
					return "", "", err
				}
				if err = os.WriteFile(filepath.Join(mirror, relative), adapted, 0600); err != nil {
					return "", "", err
				}
				delete(replacements, original)
			} else {
				target := filepath.Join(directory, fmt.Sprintf("adapter-%d.go", len(replacements)))
				if err = os.WriteFile(target, adapted, 0600); err != nil {
					return "", "", err
				}
				replacements[original] = target
			}

		}
	}
	filename := filepath.Join(directory, "overlay.json")
	data, err := json.Marshal(map[string]any{"Replace": replacements})
	if err != nil {
		return "", "", err
	}
	if err = os.WriteFile(filename, data, 0600); err != nil {
		return "", "", err
	}
	alternate := ""
	if len(mirrors) > 0 && workspace != "" && workspace != "off" {
		alternate, err = mirrorWorkspace(workspace, directory, mirrors)
		if err != nil {
			return "", "", err
		}
	} else if len(mirrors) > 0 {
		var moduleFile string
		for _, p := range packages {
			if p.Module != nil && p.Module.Main {
				moduleFile = p.Module.GoMod
				break
			}
		}
		if moduleFile == "" {
			return "", "", fmt.Errorf("library adapters require a main module")
		}
		data, err = os.ReadFile(moduleFile)
		if err != nil {
			return "", "", err
		}
		parsed, err := modfile.Parse(moduleFile, data, nil)
		if err != nil {
			return "", "", err
		}
		for _, r := range parsed.Replace {
			if modfile.IsDirectoryPath(r.New.Path) && !filepath.IsAbs(r.New.Path) {
				r.New.Path = filepath.Join(filepath.Dir(moduleFile), r.New.Path)
				r.Syntax.Token[len(r.Syntax.Token)-1] = strconv.Quote(r.New.Path)
			}
		}
		for path, mirror := range mirrors {
			if err = parsed.AddReplace(path, "", mirror, ""); err != nil {
				return "", "", err
			}
		}
		data, err = parsed.Format()
		if err != nil {
			return "", "", err
		}
		alternate = filepath.Join(directory, "go.mod")
		if err = os.WriteFile(alternate, data, 0600); err != nil {
			return "", "", err
		}
		if sum, err := os.ReadFile(strings.TrimSuffix(moduleFile, ".mod") + ".sum"); err == nil {
			if err = os.WriteFile(filepath.Join(directory, "go.sum"), sum, 0600); err != nil {
				return "", "", err
			}
		}
	}
	return filename, alternate, nil
}

// mirrorWorkspace preserves all main modules and workspace replacements while
// redirecting adapted dependencies into the temporary build directory.
func mirrorWorkspace(workspace, directory string, mirrors map[string]string) (string, error) {
	data, err := os.ReadFile(workspace)
	if err != nil {
		return "", err
	}
	parsed, err := modfile.ParseWork(workspace, data, nil)
	if err != nil {
		return "", err
	}
	var uses []*modfile.Use
	for _, use := range parsed.Use {
		path := use.Path
		if !filepath.IsAbs(path) {
			path = filepath.Join(filepath.Dir(workspace), path)
		}
		uses = append(uses, &modfile.Use{Path: path, ModulePath: use.ModulePath})
	}
	parsed.SetUse(uses)
	for _, r := range parsed.Replace {
		if modfile.IsDirectoryPath(r.New.Path) && !filepath.IsAbs(r.New.Path) {
			r.New.Path = filepath.Join(filepath.Dir(workspace), r.New.Path)
			r.Syntax.Token[len(r.Syntax.Token)-1] = strconv.Quote(r.New.Path)
		}
	}
	for path, mirror := range mirrors {
		if err := parsed.AddReplace(path, "", mirror, ""); err != nil {
			return "", err
		}
	}
	parsed.Cleanup()
	alternate := filepath.Join(directory, "go.work")
	if err := os.WriteFile(alternate, modfile.Format(parsed.Syntax), 0600); err != nil {
		return "", err
	}
	if sum, err := os.ReadFile(workspace + ".sum"); err == nil {
		if err := os.WriteFile(alternate+".sum", sum, 0600); err != nil {
			return "", err
		}
	}
	return alternate, nil
}

func copyModule(source, target string, overlay map[string]string) error {
	return filepath.WalkDir(source, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		relative, err := filepath.Rel(source, path)
		if err != nil {
			return err
		}
		destination := filepath.Join(target, relative)
		if entry.IsDir() {
			return os.MkdirAll(destination, 0700)
		}
		if replacement, ok := overlay[path]; ok {
			if replacement == "" {
				return nil
			}
			path = replacement
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		return os.WriteFile(destination, data, 0600)
	})
}

// adaptLocalLibrary retains only determinism and value-sentinel rewrites.
func adaptLocalLibrary(pkg, name string, data []byte) ([]byte, error) {
	if pkg != ProtobufModule+"/internal/detrand" && pkg != OTelTraceModule && pkg != ProtobufModule+"/internal/impl" && pkg != ProtobufModule+"/reflect/protoregistry" && pkg != ProtobufModule+"/proto" {
		return data, nil
	}
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, name, data, parser.ParseComments)
	if err != nil {
		return nil, err
	}
	changed := false
	// A captured message may cache the workflow's MessageInfo. Snapshot data
	// before Clone constructs destination messages under query package state.
	if pkg == ProtobufModule+"/proto" {
		for _, decl := range file.Decls {
			if fn, ok := decl.(*ast.FuncDecl); ok && fn.Name.Name == "Clone" && fn.Recv == nil {
				helper, e := parser.ParseFile(token.NewFileSet(), "clone.go", "package p;func f(){if __tgoCloneIsolate.IsReadOnly(){m=__tgoSnapshot.Clone(m)}}", 0)
				if e != nil {
					return nil, e
				}
				fn.Body.List = append(helper.Decls[0].(*ast.FuncDecl).Body.List, fn.Body.List...)
				changed = true
				for _, imp := range []struct{ name, path string }{{"__tgoCloneIsolate", "isolate"}, {"__tgoSnapshot", "github.com/mfateev/sdk-go-poc/protobufcopy"}} {
					spec := &ast.ImportSpec{Name: ast.NewIdent(imp.name), Path: &ast.BasicLit{Kind: token.STRING, Value: strconv.Quote(imp.path)}}
					file.Decls = append([]ast.Decl{&ast.GenDecl{Tok: token.IMPORT, Specs: []ast.Spec{spec}}}, file.Decls...)
				}
			}
		}
	}
	// Process environment must not select per-isolate protobuf behavior.
	// Preserve host configuration and use library defaults during private execution.
	envUsed := false
	if pkg == ProtobufModule+"/internal/impl" || pkg == ProtobufModule+"/reflect/protoregistry" {
		ast.Inspect(file, func(n ast.Node) bool {
			if call, ok := n.(*ast.CallExpr); ok {
				if sel, ok := call.Fun.(*ast.SelectorExpr); ok {
					if id, ok := sel.X.(*ast.Ident); ok && id.Name == "os" && sel.Sel.Name == "Getenv" {
						call.Fun = ast.NewIdent("__tgoGetenv")
						envUsed = true
					}
				}
			}
			return true
		})
		if envUsed {
			changed = true
			helper, e := parser.ParseFile(token.NewFileSet(), "env.go", "package p;func __tgoGetenv(key string) string {if __tgoIsolate.IsActive(){return \"\"};return os.Getenv(key)}", 0)
			if e != nil {
				return nil, e
			}
			file.Decls = append(file.Decls, helper.Decls...)
			imp := &ast.ImportSpec{Name: ast.NewIdent("__tgoIsolate"), Path: &ast.BasicLit{Kind: token.STRING, Value: strconv.Quote("isolate")}}
			file.Decls = append([]ast.Decl{&ast.GenDecl{Tok: token.IMPORT, Specs: []ast.Spec{imp}}}, file.Decls...)
		}
	}
	if pkg == ProtobufModule+"/internal/detrand" {
		ast.Inspect(file, func(n ast.Node) bool {
			if v, ok := n.(*ast.ValueSpec); ok && len(v.Names) == 1 && v.Names[0].Name == "randSeed" {
				v.Type = ast.NewIdent("uint64")
				v.Values = nil
				changed = true
			}
			return true
		})
	}
	for _, decl := range file.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok || fn.Body == nil {
			continue
		}
		if pkg == ProtobufModule+"/internal/detrand" {
			source := ""
			switch fn.Name.Name {
			case "Bool":
				source = "return false"
			case "Intn":
				param := fn.Type.Params.List[0].Names[0].Name
				source = "if " + param + "<=0{panic(\"invalid argument to Intn\")};return 0"
			}
			if source != "" {
				changed = true
				f, e := parser.ParseFile(token.NewFileSet(), "body.go", "package p;func f(){"+source+"}", 0)
				if e != nil {
					return nil, e
				}
				fn.Body = f.Decls[0].(*ast.FuncDecl).Body
			}
		} else {
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				if b, ok := n.(*ast.BinaryExpr); ok && (b.Op == token.EQL || b.Op == token.NEQ) {
					if id, ok := b.Y.(*ast.Ident); ok {
						switch id.Name {
						case "nilTraceID":
							b.Y = &ast.CompositeLit{Type: ast.NewIdent("TraceID")}
							changed = true
						case "nilSpanID":
							b.Y = &ast.CompositeLit{Type: ast.NewIdent("SpanID")}
							changed = true
						}
					}
				}
				return true
			})
		}
	}
	if !changed {
		return data, nil
	}
	var out bytes.Buffer
	err = format.Node(&out, fset, file)
	return out.Bytes(), err
}
