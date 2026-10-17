package toolchain

import (
	"go/ast"
	"go/parser"
	"go/token"
	"go/types"
	"testing"
)

type registryImporter struct{ pkg *types.Package }

func (i registryImporter) Import(string) (*types.Package, error) { return i.pkg, nil }

// Source adaptation must preserve host assignments and local variables that
// shadow an import, while binding reads still compile against the library API.
func TestRegistryReadAdaptationPreservesGoBindings(t *testing.T) {
	source := []byte(`package jobs
import reg "google.golang.org/protobuf/reflect/protoregistry"
type Holder struct{GlobalTypes int}
func Global() *reg.Types {return reg.GlobalTypes}
func HostSetup(value *reg.Types){reg.GlobalTypes=value}
func Shadow() int {reg:=Holder{GlobalTypes:42};return reg.GlobalTypes}
var Text="reg.GlobalTypes"
`)
	adapted, err := registryReads(source)
	if err != nil {
		t.Fatal(err)
	}
	pkg := types.NewPackage(ProtobufModule+"/reflect/protoregistry", "protoregistry")
	for _, name := range []string{"Types", "Files"} {
		obj := types.NewTypeName(token.NoPos, pkg, name, nil)
		typ := types.NewNamed(obj, types.NewStruct(nil, nil), nil)
		pkg.Scope().Insert(obj)
		ptr := types.NewPointer(typ)
		pkg.Scope().Insert(types.NewVar(token.NoPos, pkg, "Global"+name, ptr))
		signature := types.NewSignatureType(nil, nil, nil, types.NewTuple(), types.NewTuple(types.NewVar(token.NoPos, pkg, "", ptr)), false)
		pkg.Scope().Insert(types.NewFunc(token.NoPos, pkg, "IsolateGlobal"+name, signature))
	}
	pkg.MarkComplete()
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "jobs.go", adapted, 0)
	if err != nil {
		t.Fatal(err)
	}
	config := types.Config{Importer: registryImporter{pkg}}
	if _, err = config.Check("example.org/jobs", fset, []*ast.File{file}, nil); err != nil {
		t.Fatalf("adapted source changed Go bindings: %v\n%s", err, adapted)
	}
}
