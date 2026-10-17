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

const hostAdapterPackage = "github.com/mfateev/sdk-go-poc/hostintegration"

// libraryOverlay keeps dependency layout and source policy in the SDK. Every
// adapter is ordinary Go using the public isolate host/construction APIs.
func libraryOverlay(packages []*Package, directory string, existing map[string]string) (string, string, error) {
	replacements := make(map[string]string)
	for k, v := range existing {
		replacements[k] = v
	}
	seen := make(map[string]bool)
	mirrors := make(map[string]string)
	for _, p := range packages {
		if p.Standard {
			continue
		}
		for _, name := range p.GoFiles {
			original := filepath.Join(p.Dir, name)
			if seen[original] {
				continue
			}
			seen[original] = true
			protobuf := strings.HasPrefix(p.ImportPath, ProtobufModule+"/")
			generated := strings.HasSuffix(name, ".pb.go")
			telemetry := p.ImportPath == OTelTraceModule
			data, err := p.readSource(original)
			if err != nil {
				return "", "", err
			}
			generated = generated && bytes.Contains(data, []byte(ProtobufModule+"/runtime/protoimpl"))
			rewritten, err := registryReads(data)
			if err != nil {
				return "", "", err
			}
			adapted := rewritten
			if protobuf || generated || telemetry {
				adapted, err = adaptLibrary(p.ImportPath, name, rewritten, protobuf, generated, telemetry)
			}
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
	if len(mirrors) > 0 {
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

func adaptLibrary(pkg, name string, data []byte, protobuf, generated, telemetry bool) ([]byte, error) {
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, name, data, parser.ParseComments)
	if err != nil {
		return nil, err
	}
	render := func(n any) string { var b bytes.Buffer; format.Node(&b, fset, n); return b.String() }

	var additions []ast.Decl
	used := false
	parseDecls := func(source string) ([]ast.Decl, error) {
		f, e := parser.ParseFile(token.NewFileSet(), "adapter.go", "package "+file.Name.Name+"\n"+source, 0)
		if e != nil {
			return nil, e
		}
		return f.Decls, nil
	}
	body := func(source string) (*ast.BlockStmt, error) {
		ds, e := parseDecls("func _() " + source)
		if e != nil {
			return nil, e
		}
		return ds[0].(*ast.FuncDecl).Body, nil
	}
	// Generated message methods use the canonical process MessageInfo through
	// the host implementation. Avoid writing MessageState just to obtain a view.
	for _, d := range file.Decls {
		fn, ok := d.(*ast.FuncDecl)
		if !ok || fn.Body == nil {
			continue
		}
		if generated && fn.Name.Name == "ProtoReflect" {
			if last, ok := fn.Body.List[len(fn.Body.List)-1].(*ast.ReturnStmt); ok {
				if call, ok := last.Results[0].(*ast.CallExpr); ok {
					if sel, ok := call.Fun.(*ast.SelectorExpr); ok && sel.Sel.Name == "MessageOf" {
						// mi is normally introduced by the first declaration.
						fn.Body.List = []ast.Stmt{fn.Body.List[0], last}
					}
				}
			}
		}
		if generated && fn.Name.Name == "Reset" && len(fn.Body.List) > 0 {
			fn.Body.List = fn.Body.List[:1]
		}
	}
	for _, d := range file.Decls {
		fn, ok := d.(*ast.FuncDecl)
		if !ok || fn.Body == nil || fn.Type.TypeParams != nil {
			continue
		}
		method := fn.Name.Name
		// Plain generated getters must keep application package defaults and aliases.
		if generated && fn.Recv != nil && strings.HasPrefix(method, "Get") {
			continue
		}

		// Obtain workflow time before the constructor enters host execution.
		if pkg == ProtobufModule+"/types/known/timestamppb" && method == "Now" {
			continue
		}

		if pkg == ProtobufModule+"/internal/detrand" && method == "Bool" {
			fn.Body, _ = body("{return false}")
		}
		if pkg == ProtobufModule+"/internal/detrand" && method == "Intn" {
			param := fn.Type.Params.List[0].Names[0].Name
			fn.Body, _ = body("{if " + param + "<=0 {panic(\"invalid argument to Intn\")};return 0}")
		}

		if pkg == ProtobufModule+"/internal/impl" && method == "MessageOf" && fn.Recv != nil && strings.Contains(render(fn.Recv.List[0].Type), "MessageInfo") {
			var statements []ast.Stmt
			for _, stmt := range fn.Body.List {
				if branch, ok := stmt.(*ast.IfStmt); ok {
					if strings.Contains(render(branch.Cond), "p.IsNil()") {
						continue
					}
				}
				statements = append(statements, stmt)
			}
			fn.Body.List = statements
		}

		if telemetry {
			// These zero sentinels are values, not shared mutable library services.
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				if b, ok := n.(*ast.BinaryExpr); ok && (b.Op == token.EQL || b.Op == token.NEQ) {
					if id, ok := b.Y.(*ast.Ident); ok {
						if id.Name == "nilTraceID" {
							b.Y = &ast.CompositeLit{Type: ast.NewIdent("TraceID")}
							used = true
						}
						if id.Name == "nilSpanID" {
							b.Y = &ast.CompositeLit{Type: ast.NewIdent("SpanID")}
							used = true
						}
					}
				}
				return true
			})
			continue
		}
		if method == "init" && fn.Recv == nil {
			continue
		}
		if generated && fn.Recv == nil && strings.HasPrefix(method, "file_") && strings.HasSuffix(method, "_init") {
			original := *fn
			original.Name = ast.NewIdent("__tgoOriginal" + method)
			original.Doc = nil
			wrapper, e := body("{if __tgoHost.IsActive(){return};" + original.Name.Name + "()}")
			if e != nil {
				return nil, e
			}
			fn.Body = wrapper
			additions = append(additions, &original)
			used = true
			continue
		}

		// MessageInfo lazy construction must allocate into the host even when a
		// value constructor discovers a previously unused message type.
		cache := pkg == ProtobufModule+"/internal/impl" && fn.Recv != nil && (method == "init" || method == "initOnce") && strings.Contains(render(fn.Recv.List[0].Type), "MessageInfo")
		if cache {
			original := *fn
			original.Name = ast.NewIdent("__tgoOriginal" + method)
			original.Doc = nil
			recv := fn.Recv.List[0].Names[0].Name
			replacement, e := body("{__tgoHost.Host(func() struct{} {" + recv + "." + original.Name.Name + "();return struct{}{} });}")
			if e != nil {
				return nil, e
			}
			fn.Body = replacement
			additions = append(additions, &original)
			used = true
			continue
		}

		sharedGenerated := generated && fn.Recv == nil && strings.HasPrefix(method, "file_") && strings.HasSuffix(method, "_rawDescGZIP")
		if !(ast.IsExported(method) && (protobuf || generated)) && !sharedGenerated {
			continue
		}
		// Generic receiver methods require a separate instantiation strategy.
		if fn.Recv != nil && strings.Contains(render(fn.Recv.List[0].Type), "[") {
			continue
		}
		originalName := "__tgoOriginal" + method
		var declarations, values, callargs []string
		receiver := ""
		if fn.Recv != nil {
			field := fn.Recv.List[0]
			if len(field.Names) == 0 {
				field.Names = []*ast.Ident{ast.NewIdent("__receiver")}
			}
			receiver = field.Names[0].Name
			values = append(values, receiver)
			declarations = append(declarations, fmt.Sprintf("args[%d].(%s)", len(declarations), render(field.Type)))
		}
		argumentIndex := 0
		if fn.Type.Params != nil {
			for _, field := range fn.Type.Params.List {
				if len(field.Names) == 0 {
					field.Names = []*ast.Ident{ast.NewIdent(fmt.Sprintf("__arg%d", argumentIndex))}
				}
				for j, id := range field.Names {
					if id.Name == "_" {
						id = ast.NewIdent(fmt.Sprintf("__arg%d", argumentIndex))
						field.Names[j] = id
					}
					argumentIndex++
					t := render(field.Type)
					variadic := strings.HasPrefix(t, "...")
					if variadic {
						t = "[]" + strings.TrimPrefix(t, "...")
					}
					values = append(values, id.Name)
					expression := fmt.Sprintf("__tgoHost.Bind[%s](ctx,args[%d])", t, len(declarations))
					if variadic {
						expression += "..."
					}
					declarations = append(declarations, expression)
				}
			}
		}
		originalCall := originalName + "(" + strings.Join(declarations, ",") + ")"
		if receiver != "" {
			originalCall = declarations[0] + "." + originalName + "(" + strings.Join(declarations[1:], ",") + ")"
		}
		var resultTypes []string
		if fn.Type.Results != nil {
			for _, field := range fn.Type.Results.List {
				n := len(field.Names)
				if n == 0 {
					n = 1
				}
				for range n {
					resultTypes = append(resultTypes, render(field.Type))
				}
			}
		}
		key := pkg + "." + method
		if fn.Recv != nil {
			key = pkg + ".(" + render(fn.Recv.List[0].Type) + ")." + method
		}
		var handler, returns []string
		var copies []string
		if pkg == ProtobufModule+"/proto" || pkg == ProtobufModule+"/encoding/protojson" || pkg == ProtobufModule+"/encoding/prototext" {
			handler = append(handler, "__tgoHost.CheckMessages(args...)")
		}

		if (pkg == ProtobufModule+"/proto" || pkg == ProtobufModule+"/encoding/protojson") && fn.Recv != nil && strings.Contains(render(fn.Recv.List[0].Type), "marshalOptions") {
			handler = append(handler, "__tgoHost.CheckResolver(args[0])")
		}
		if (pkg == ProtobufModule+"/proto" || pkg == ProtobufModule+"/encoding/protojson") && fn.Recv != nil && strings.Contains(render(fn.Recv.List[0].Type), "MarshalOptions") {
			handler = append(handler, "__tgoHost.CheckResolver(args[0])")
		}

		for i, t := range resultTypes {
			callargs = append(callargs, fmt.Sprintf("r%d", i))
			returns = append(returns, fmt.Sprintf("__tgoHost.Argument[%s](result[%d])", t, i))
		}
		if len(resultTypes) > 0 {
			handler = append(handler, strings.Join(callargs, ",")+":="+originalCall)
		} else {
			handler = append(handler, originalCall)
		}
		// Tagged scalar defaults are copied by protobuf-aware library code, after
		// shared metadata work has restored the caller's construction allocator.
		for i, t := range resultTypes {
			if t == "error" {
				copies = append(copies, fmt.Sprintf("if e,ok:=values[%d].(error);ok&&e!=nil{values[%d]=__tgoHost.CopyError(e)}", i, i))
			}
			if strings.Contains(pkg, "/internal/filedesc") && (t == "protoreflect.Value" || pkg == ProtobufModule+"/reflect/protoreflect" && t == "Value") {
				prefix := "protoreflect."
				if t == "Value" {
					prefix = ""
				}
				copies = append(copies, fmt.Sprintf("r:=__tgoHost.Argument[%s](values[%d]); switch x:=r.Interface().(type) {case string:r=%sValueOfString(__tgoHost.CopyString(x));case []byte:r=%sValueOfBytes(append([]byte(nil),x...))};values[%d]=r", t, i, prefix, prefix, i))
			}
		}
		handler = append(handler, "return []any{"+strings.Join(callargs, ",")+"}")
		hostOnly := sharedGenerated || strings.Contains(pkg, "/internal/filedesc") || strings.Contains(pkg, "/reflect/protoregistry")
		reject := false
		if strings.Contains(pkg, "/reflect/protoregistry") && (strings.HasPrefix(method, "Register") || strings.HasPrefix(method, "Range")) {
			reject = true
		}
		if strings.Contains(pkg, "/internal/filedesc") && (method == "Options" || method == "ExtensionRangeOptions" || method == "OptionImports") {
			reject = true
		}
		copyFunction := "nil"
		if len(copies) > 0 {
			copyFunction = "func(values []any) []any {" + strings.Join(copies, ";") + ";return values}"
		}
		registration := fmt.Sprintf("func init(){__tgoHost.Register(%q,__tgoHost.Operation{Function:func(ctx __tgoHost.Context,args []any) []any {%s},Copy:%s,HostOnly:%t,Reject:%t})}", key, strings.Join(handler, ";"), copyFunction, hostOnly, reject)

		// The registered adapter takes []any so it can preserve nil interface values.
		ds, e := parseDecls(registration)
		if e != nil {
			return nil, e
		}
		additions = append(additions, ds...)
		call := fmt.Sprintf("__tgoHost.Call(%q,%s)", key, strings.Join(values, ","))
		if len(values) == 0 {
			call = fmt.Sprintf("__tgoHost.Call(%q)", key)
		}
		bridgeBody := "{ result:=" + call + "; return " + strings.Join(returns, ",") + " }"
		if len(returns) == 0 {
			bridgeBody = "{ " + call + "; return }"
		}
		block, e := body(bridgeBody)
		if e != nil {
			return nil, e
		}
		old := *fn
		old.Name = ast.NewIdent(originalName)
		old.Doc = nil
		// Native/unsafe guards stay on the host implementation, after the explicit
		// boundary. The application wrapper contains no unsafe operations.
		active, e := body("{if __tgoHost.IsActive() " + render(block) + "}")
		if e != nil {
			return nil, e
		}
		directArgs := values
		if receiver != "" {
			directArgs = values[1:]
		}
		if fn.Type.Params != nil && len(fn.Type.Params.List) > 0 {
			if _, ok := fn.Type.Params.List[len(fn.Type.Params.List)-1].Type.(*ast.Ellipsis); ok {
				directArgs = append([]string(nil), directArgs...)
				directArgs[len(directArgs)-1] += "..."
			}
		}
		direct := originalName + "(" + strings.Join(directArgs, ",") + ")"
		if receiver != "" {
			direct = receiver + "." + direct
		}
		if hostOnly {
			var fields, locals, returnValues []string
			for i, t := range resultTypes {
				fields = append(fields, fmt.Sprintf("R%d %s", i, t))
				locals = append(locals, fmt.Sprintf("v%d", i))
				returnValues = append(returnValues, fmt.Sprintf("__tgoHost.MetadataCopy(result.R%d)", i))
			}
			typ := "struct{" + strings.Join(fields, ";") + "}"
			inner := direct + ";return " + typ + "{}"
			if len(resultTypes) > 0 {
				inner = strings.Join(locals, ",") + ":=" + direct + ";return " + typ + "{" + strings.Join(locals, ",") + "}"
			}
			direct = "result:=__tgoHost.Host(func() " + typ + " {" + inner + "});"
			for i, t := range resultTypes {
				if t == "protoreflect.Value" {
					direct += fmt.Sprintf("{r:=result.R%d;switch x:=r.Interface().(type){case string:r=protoreflect.ValueOfString(__tgoHost.CopyString(x));case []byte:r=protoreflect.ValueOfBytes(append([]byte(nil),x...))};result.R%d=r};", i, i)
				}
			}
			if len(resultTypes) > 0 {
				direct += "return " + strings.Join(returnValues, ",")
			} else {
				direct = "__tgoHost.Host(func() struct{} {" + inner + "})"
			}
		} else if len(resultTypes) > 0 {
			direct = "return " + direct
		}
		tail, e := body("{" + direct + "}")
		if e != nil {
			return nil, e
		}
		fn.Body = &ast.BlockStmt{List: append(active.List, tail.List...)}
		additions = append(additions, &old)
		// Avoid registering synthetic wrappers twice through generated test variants.
		used = true
	}
	if pkg == ProtobufModule+"/reflect/protoregistry" && name == "registry.go" {
		declarations, e := parseDecls(`
func IsolateGlobalTypes() *Types {if __tgoHost.IsActive(){return __tgoHost.Argument[*Types](__tgoHost.Call("protobuf.GlobalTypes")[0])};return GlobalTypes}
func IsolateGlobalFiles() *Files {if __tgoHost.IsActive(){return __tgoHost.Argument[*Files](__tgoHost.Call("protobuf.GlobalFiles")[0])};return GlobalFiles}
func init(){
 __tgoHost.Register("protobuf.GlobalTypes",__tgoHost.Operation{Function:func(_ __tgoHost.Context,_ []any)[]any{return []any{GlobalTypes}},HostOnly:true})
 __tgoHost.Register("protobuf.GlobalFiles",__tgoHost.Operation{Function:func(_ __tgoHost.Context,_ []any)[]any{return []any{GlobalFiles}},HostOnly:true})
 __tgoHost.RegisterDefaultResolver(GlobalTypes)
}
`)
		if e != nil {
			return nil, e
		}
		additions = append(additions, declarations...)
		used = true
	}
	if !used {
		return data, nil
	}
	if !telemetry {
		imp := &ast.ImportSpec{Name: ast.NewIdent("__tgoHost"), Path: &ast.BasicLit{Kind: token.STRING, Value: strconv.Quote(hostAdapterPackage)}}
		file.Decls = append([]ast.Decl{&ast.GenDecl{Tok: token.IMPORT, Specs: []ast.Spec{imp}}}, file.Decls...)
	}
	file.Decls = append(file.Decls, additions...)
	if generated {
		registered := make(map[string]bool)
		for _, d := range file.Decls {
			fn, ok := d.(*ast.FuncDecl)
			if !ok || fn.Name.Name != "ProtoReflect" || fn.Recv == nil {
				continue
			}
			t := render(fn.Recv.List[0].Type)
			if registered[t] {
				continue
			}
			registered[t] = true
			ds, e := parseDecls("func init(){__tgoHost.RegisterGenerated((" + t + ")(nil))}")
			if e != nil {
				return nil, e
			}
			file.Decls = append(file.Decls, ds...)
		}
	}
	var out bytes.Buffer
	if err = format.Node(&out, fset, file); err != nil {
		return nil, err
	}
	return out.Bytes(), nil
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

// Reads of exported registry bindings become ordinary library calls. Writes
// keep their original compiler/runtime ownership checks, including host setup.
func registryReads(data []byte) ([]byte, error) {
	if !bytes.Contains(data, []byte(ProtobufModule+"/reflect/protoregistry")) {
		return data, nil
	}
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "source.go", data, 0)
	if err != nil {
		return nil, err
	}
	alias := ""
	for _, spec := range file.Imports {
		path, _ := strconv.Unquote(spec.Path.Value)
		if path == ProtobufModule+"/reflect/protoregistry" {
			alias = "protoregistry"
			if spec.Name != nil {
				alias = spec.Name.Name
			}
		}
	}
	if alias == "" || alias == "_" || alias == "." {
		return data, nil
	}
	writes := make(map[ast.Expr]bool)
	ast.Inspect(file, func(node ast.Node) bool {
		if assignment, ok := node.(*ast.AssignStmt); ok {
			for _, lhs := range assignment.Lhs {
				writes[lhs] = true
			}
		}
		if unary, ok := node.(*ast.UnaryExpr); ok && unary.Op == token.AND {
			writes[unary.X] = true
		}
		return true
	})
	type edit struct {
		start, end  int
		replacement string
	}
	var edits []edit
	ast.Inspect(file, func(node ast.Node) bool {
		selector, ok := node.(*ast.SelectorExpr)
		if !ok || writes[selector] {
			return true
		}
		id, ok := selector.X.(*ast.Ident)
		if !ok || id.Name != alias || id.Obj != nil && id.Obj.Kind != ast.Pkg {
			return true
		}
		if selector.Sel.Name != "GlobalTypes" && selector.Sel.Name != "GlobalFiles" {
			return true
		}
		edits = append(edits, edit{fset.Position(selector.Pos()).Offset, fset.Position(selector.End()).Offset, alias + ".Isolate" + selector.Sel.Name + "()"})
		return true
	})
	for i := len(edits) - 1; i >= 0; i-- {
		e := edits[i]
		data = append(append(append([]byte(nil), data[:e.start]...), e.replacement...), data[e.end:]...)
	}
	return data, nil
}
