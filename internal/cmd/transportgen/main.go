package main

import (
	"encoding/json"
	"flag"
	"os"
	"reflect"

	"github.com/mfateev/sdk-go-poc/workflow"
	failurepb "go.temporal.io/api/failure/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
)

type field struct {
	Name     string
	Type     string
	Embedded bool
}
type typ struct {
	Kind, Name, Pkg, Elem, Key, Proto, File string
	Fields                                  []field
}

var types = map[string]*typ{}
var files = map[string]protoreflect.FileDescriptor{}

func visit(t reflect.Type) string {
	key := t.PkgPath() + ":" + t.String()
	if _, ok := types[key]; ok {
		return key
	}
	d := &typ{Kind: t.Kind().String(), Name: t.Name(), Pkg: t.PkgPath()}
	types[key] = d
	if t.Kind() != reflect.Pointer && t.Kind() != reflect.Interface && reflect.PointerTo(t).Implements(reflect.TypeFor[proto.Message]()) {
		m := reflect.New(t).Interface().(proto.Message).ProtoReflect().Descriptor()
		d.Proto = string(m.FullName())
		d.File = m.ParentFile().Path()
		files[d.File] = m.ParentFile()
		return key
	}
	switch t.Kind() {
	case reflect.Pointer, reflect.Slice, reflect.Array:
		d.Elem = visit(t.Elem())
	case reflect.Map:
		d.Key = visit(t.Key())
		d.Elem = visit(t.Elem())
	case reflect.Struct:
		if t.PkgPath() == "time" && t.Name() == "Time" {
			return key
		}
		for i := 0; i < t.NumField(); i++ {
			f := t.Field(i)
			if f.PkgPath != "" || f.Tag.Get("json") == "-" {
				continue
			}
			if t.Name() == "ChildWorkflowOptions" && (f.Name == "Memo" || f.Name == "SearchAttributes" || f.Name == "TypedSearchAttributes") {
				continue
			}
			d.Fields = append(d.Fields, field{f.Name, visit(f.Type), f.Anonymous})
		}
	}
	return key
}
func main() {
	typesOut := flag.String("types", "", "type metadata output")
	descriptorsOut := flag.String("descriptors", "", "protobuf descriptor output")
	flag.Parse()
	if *typesOut == "" || *descriptorsOut == "" {
		panic("output paths are required")
	}
	visit(reflect.TypeFor[failurepb.Failure]())
	roots := []any{workflow.InterceptedSignals{}, workflow.Start{}, workflow.PayloadStart{}, workflow.ActivityRequest{}, workflow.ActivityPayloadRequest{}, workflow.Signal{}, workflow.Completion{}, workflow.PayloadCompletion{}, workflow.ActivityOutcome{}, workflow.LocalActivityOutcome{}, workflow.LocalActivityRequest{}, workflow.ChildRequest{}, workflow.ChildSignalRequest{}, workflow.ExternalRequest{}, workflow.NexusRequest{}, workflow.QueryRegistration{}, workflow.QueryRequest{}, workflow.QueryResponse{}, workflow.UpdateRegistration{}, workflow.UpdateRequest{}, workflow.UpdateCompletion{}, workflow.WorkflowControl{}, workflow.VersionRequest{}, workflow.CallRequest{}, workflow.InfoSnapshot{}, workflow.LastCompletionSnapshot{}, workflow.SignalRegistration{}, workflow.SessionInfo{}, workflow.LoggerWrite{}, workflow.MetricWrite{}}
	var keys []string
	for _, v := range roots {
		keys = append(keys, visit(reflect.TypeOf(v)))
	}
	// Imports provide descriptor definitions to protoc without fetching another
	// copy of the Temporal API source tree.
	var set descriptorpb.FileDescriptorSet
	seen := map[string]bool{}
	var add func(protoreflect.FileDescriptor)
	add = func(f protoreflect.FileDescriptor) {
		if seen[f.Path()] {
			return
		}
		seen[f.Path()] = true
		for i := 0; i < f.Imports().Len(); i++ {
			add(f.Imports().Get(i).FileDescriptor)
		}
		set.File = append(set.File, protodesc.ToFileDescriptorProto(f))
	}
	for _, f := range files {
		add(f)
	}
	raw, err := proto.Marshal(&set)
	if err != nil {
		panic(err)
	}
	if err = os.WriteFile(*descriptorsOut, raw, 0600); err != nil {
		panic(err)
	}
	out := map[string]any{"types": types, "roots": keys}
	data, err := json.MarshalIndent(out, "", "  ")
	if err != nil {
		panic(err)
	}
	if err = os.WriteFile(*typesOut, data, 0600); err != nil {
		panic(err)
	}
}
