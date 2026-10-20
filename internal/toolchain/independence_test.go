package toolchain

import (
	"encoding/json"
	"os"
	"path/filepath"
	"slices"
	"testing"

	"golang.org/x/mod/modfile"
)

func generatedPackage(t *testing.T, path string) *Package {
	t.Helper()
	directory := t.TempDir()
	if err := os.WriteFile(filepath.Join(directory, "model.pb.go"), []byte("package model\nimport _ \"google.golang.org/protobuf/runtime/protoimpl\"\n"), 0600); err != nil {
		t.Fatal(err)
	}
	return &Package{ImportPath: path, Name: "model", Dir: directory, GoFiles: []string{"model.pb.go"}}
}
func TestPrivateProtoGraphExcludesHostOnlyDescriptors(t *testing.T) {
	common := generatedPackage(t, "go.temporal.io/api/common/v1")
	hostOnly := generatedPackage(t, "go.temporal.io/api/workflowservice/v1")
	hostOnlyBuiltin := generatedPackage(t, "google.golang.org/protobuf/types/descriptorpb")
	hostSDK := &Package{ImportPath: "go.temporal.io/sdk/workflow", dependencies: []*Package{common, hostOnly, hostOnlyBuiltin}}
	support := &Package{ImportPath: "github.com/mfateev/sdk-go-poc/workflow", dependencies: []*Package{common, hostSDK}}
	root := &Package{ImportPath: "example.org/jobs", Name: "jobs", dependencies: []*Package{support}}
	manifest, err := plan([]*Package{root, support, common, hostSDK, hostOnly, hostOnlyBuiltin}, []isolateFunction{{Package: root, Name: "Work"}})
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Contains(manifest.StatePackages, common.ImportPath) || slices.Contains(manifest.StatePackages, hostOnly.ImportPath) || slices.Contains(manifest.StatePackages, hostOnlyBuiltin.ImportPath) {
		t.Fatalf("wrong private graph: %v", manifest.StatePackages)
	}
	if !slices.Contains(manifest.ReadOnlyPackages, common.ImportPath) || !slices.Contains(manifest.AuditedFiles, common.ImportPath+"/model.pb.go") {
		t.Fatal("generated scratch state/source audit missing")
	}
}
func TestOverlayDoesNotRewriteGeneratedMessagesOrRegistryBindings(t *testing.T) {
	p := generatedPackage(t, "example.org/model")
	source := []byte("package model\nimport reg \"google.golang.org/protobuf/reflect/protoregistry\"\nfunc Global()*reg.Types{return reg.GlobalTypes}\n")
	if err := os.WriteFile(filepath.Join(p.Dir, "model.pb.go"), source, 0600); err != nil {
		t.Fatal(err)
	}
	overlay, modfile, err := libraryOverlay([]*Package{p}, t.TempDir(), "", map[string]string{"existing.go": "replacement.go"})
	if err != nil {
		t.Fatal(err)
	}
	if modfile != "" {
		t.Fatal("application module was mirrored")
	}
	data, err := os.ReadFile(overlay)
	if err != nil {
		t.Fatal(err)
	}
	var actual struct{ Replace map[string]string }
	if err = json.Unmarshal(data, &actual); err != nil {
		t.Fatal(err)
	}
	if len(actual.Replace) != 1 || actual.Replace["existing.go"] != "replacement.go" {
		t.Fatalf("application source was rewritten: %s", data)
	}
}

func TestMirrorWorkspacePreservesMainModulesAndReplacements(t *testing.T) {
	directory := t.TempDir()
	source := filepath.Join(directory, "go.work")
	original := []byte("go 1.26\nuse (\n ./app\n ./other\n)\nreplace example.org/local => ./local\n")
	if err := os.WriteFile(source, original, 0600); err != nil {
		t.Fatal(err)
	}
	target, err := mirrorWorkspace(source, t.TempDir(), map[string]string{"google.golang.org/protobuf": "/tmp/private-protobuf"})
	if err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(target)
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := modfile.ParseWork(target, data, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(parsed.Use) != 2 || parsed.Use[0].Path != filepath.Join(directory, "app") || parsed.Use[1].Path != filepath.Join(directory, "other") {
		t.Fatalf("workspace modules: %s", data)
	}
	if len(parsed.Replace) != 2 || parsed.Replace[0].New.Path != filepath.Join(directory, "local") || parsed.Replace[1].New.Path != "/tmp/private-protobuf" {
		t.Fatalf("workspace replacements: %s", data)
	}
	untouched, _ := os.ReadFile(source)
	if string(untouched) != string(original) {
		t.Fatal("source workspace changed")
	}
}

func TestNestedBuildRestoresWorkspace(t *testing.T) {
	parent := []string{"OTHER=value", "GOWORK=/tmp/adapted.work", "TGO_WORKSPACE_OVERLAY=/tmp/adapted.work", "TGO_WORKSPACE_ORIGIN=/source/go.work"}
	restored := restoreWorkspaceEnvironment(parent)
	if !slices.Equal(restored, []string{"OTHER=value", "GOWORK=/tmp/adapted.work", "GOWORK=/source/go.work"}) {
		t.Fatal(restored)
	}
	overridden := restoreWorkspaceEnvironment(append(parent, "GOWORK=off"))
	if overridden[len(overridden)-1] != "GOWORK=off" {
		t.Fatal(overridden)
	}
}
