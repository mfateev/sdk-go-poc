package protobufvalues

import (
	"context"
	"fmt"
	"unicode"

	"github.com/mfateev/sdk-go-poc/example/protobufvalues/model"
	"github.com/mfateev/sdk-go-poc/workflow"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// SharedMetadata exercises repeated use of a type's shared reflection/coder
// tables with distinct, mutable message values. Settings remains unused.
//
//temporal:workflow
func SharedMetadata(ctx context.Context) (string, error) {
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
	if err := workflow.SetQueryHandler(ctx, "mutateMetadata", func() (string, error) {
		(&model.Record{}).ProtoReflect().ProtoMethods().Flags = 0
		return "mutation accepted", nil
	}); err != nil {
		return "", err
	}
	if err := workflow.SetQueryHandler(ctx, "metadata", func() (string, error) {
		return fmt.Sprintf("%p", (&model.Record{}).ProtoReflect().Type()), nil
	}); err != nil {
		return "", err
	}
	identity := ""
	for i := range 100 {
		message := &model.Record{Sequence: int64(i), Entries: map[string]*model.Record{"nested": {Sequence: int64(i + 1)}}, Choice: &model.Record_Title{Title: "cache"}}
		view := message.ProtoReflect()
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
	<-workflow.GetSignalChannel[bool](ctx, "finish")
	return identity, nil
}
