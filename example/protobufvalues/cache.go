package protobufvalues

import (
	"context"
	"fmt"
	"unicode"

	"github.com/mfateev/sdk-go-poc/example/protobufvalues/model"
	"github.com/mfateev/sdk-go-poc/workflow"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
)

var rangeCallbacks int

// SharedMetadata exercises independent per-isolate reflection/coder tables
// with distinct mutable messages. Query caches belong to the scratch domain.
//
//temporal:workflow
func SharedMetadata(ctx context.Context) (string, error) {
	defaults := model.Default_Settings_Data
	model.Default_Settings_Data = []byte("private-default")
	if string((&model.Settings{}).GetData()) != "private-default" {
		return "", fmt.Errorf("generated getter lost private package defaults")
	}
	model.Default_Settings_Data = defaults
	if rangeCallbacks != 0 {
		return "", fmt.Errorf("callback package state leaked across isolates")
	}

	if err := workflow.SetQueryHandler(ctx, "mutateLibrary", func() (string, error) {
		unicode.L.R16[0].Lo++
		return "mutation accepted", nil
	}); err != nil {
		return "", err
	}
	if err := workflow.SetQueryHandler(ctx, "mutateLibraryMap", func() (string, error) {
		delete(unicode.Categories, "L")
		return "mutation accepted", nil
	}); err != nil {
		return "", err
	}
	captured := &model.Record{Sequence: 42}
	if _, err := proto.Marshal(captured); err != nil {
		return "", err
	}
	if err := workflow.SetQueryHandler(ctx, "mutateMetadata", func() (string, error) {
		captured.ProtoReflect().ProtoMethods().Flags = 0
		return "mutation accepted", nil
	}); err != nil {
		return "", err
	}
	if err := workflow.SetQueryHandler(ctx, "metadata", func() (string, error) {
		return fmt.Sprintf("%p", (&model.Record{}).ProtoReflect().Type()), nil
	}); err != nil {
		return "", err
	}
	if err := workflow.SetQueryHandler(ctx, "localRegistry", func() (string, error) {
		registry := new(protoregistry.Types)
		typ := (&model.Record{}).ProtoReflect().Type()
		if err := registry.RegisterMessage(typ); err != nil {
			return "", err
		}
		found, err := registry.FindMessageByName(typ.Descriptor().FullName())
		if err != nil {
			return "", err
		}
		return string(found.Descriptor().FullName()), nil
	}); err != nil {
		return "", err
	}
	identity := ""
	for i := range 100 {
		message := &model.Record{Sequence: int64(i), Entries: map[string]*model.Record{"nested": {Sequence: int64(i + 1)}}, Choice: &model.Record_Title{Title: "cache"}}
		view := message.ProtoReflect()
		if i == 0 {
			view.Range(func(_ protoreflect.FieldDescriptor, _ protoreflect.Value) bool { rangeCallbacks++; return true })
			if rangeCallbacks == 0 {
				return "", fmt.Errorf("protobuf callback did not reenter application state")
			}
		}
		current := fmt.Sprintf("%p", view.Type())
		if identity != "" && identity != current {
			return "", fmt.Errorf("message metadata was rebuilt")
		}
		identity = current
		view.Set(view.Descriptor().Fields().ByNumber(1), protoreflect.ValueOfInt64(int64(i+10)))
		bytes, err := (proto.MarshalOptions{Deterministic: true}).Marshal(message)
		if err != nil {
			return "", err
		}
		var decoded model.Record
		if err := proto.Unmarshal(bytes, &decoded); err != nil {
			return "", err
		}
		if decoded.Sequence != int64(i+10) || decoded.Entries["nested"].Sequence != int64(i+1) || decoded.GetTitle() != "cache" {
			return "", fmt.Errorf("shared tables changed message values")
		}
	}
	if err := workflow.SetQueryHandler(ctx, "workflowMetadata", func() (string, error) { return identity, nil }); err != nil {
		return "", err
	}
	<-workflow.GetSignalChannel[bool](ctx, "finish")
	return identity, nil
}
