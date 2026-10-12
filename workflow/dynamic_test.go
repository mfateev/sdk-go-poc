package workflow

import (
	"context"
	"testing"

	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/temporal"
)

func TestDynamicHandlerSignatures(t *testing.T) {
	ctx := context.Background()
	var nilQuery func(string, converter.EncodedValues) (string, error)
	for _, fn := range []any{nil, nilQuery, func(string) (string, error) { return "", nil }, func(int, converter.EncodedValues) (string, error) { return "", nil }, func(string, converter.EncodedValues) error { return nil }} {
		if err := SetDynamicQueryHandler(ctx, fn, QueryHandlerOptions{}); err == nil {
			t.Fatalf("accepted dynamic query %T", fn)
		}
	}
	var nilUpdate func(context.Context, string, converter.EncodedValues) error
	for _, fn := range []any{nil, nilUpdate, func(context.Context, string) error { return nil }, func(context.Context, int, converter.EncodedValues) error { return nil }, func(context.Context, string, converter.EncodedValues) string { return "" }} {
		if err := SetDynamicUpdateHandler(ctx, fn, UpdateHandlerOptions{}); err == nil {
			t.Fatalf("accepted dynamic update %T", fn)
		}
	}
	valid := func(context.Context, string, converter.EncodedValues) error { return nil }
	for _, validator := range []any{func(string) error { return nil }, func(int, converter.EncodedValues) error { return nil }, func(string, converter.EncodedValues) string { return "" }} {
		if err := SetDynamicUpdateHandler(ctx, valid, UpdateHandlerOptions{Validator: validator}); err == nil {
			t.Fatalf("accepted dynamic validator %T", validator)
		}
	}
	if err := SetDynamicQueryHandler(nil, func(string, converter.EncodedValues) (string, error) { return "", nil }, QueryHandlerOptions{}); err == nil {
		t.Fatal("accepted nil context")
	}
	if err := SetDynamicUpdateHandler(nil, valid, UpdateHandlerOptions{}); err == nil {
		t.Fatal("accepted nil context")
	}
}

func TestDynamicEncodedArgumentsNoData(t *testing.T) {
	for _, payloads := range []*commonpb.Payloads{nil, {}} {
		values := encodedArguments{payloads: payloads, converter: converter.GetDefaultDataConverter()}
		if values.HasValues() {
			t.Fatal("empty arguments report values")
		}
		var result string
		if err := values.Get(&result); err != temporal.ErrNoData {
			t.Fatalf("empty argument error = %v, want ErrNoData", err)
		}
	}
}
