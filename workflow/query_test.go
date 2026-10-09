package workflow

import (
	"context"
	"testing"
)

func TestQueryHandlerSignatures(t *testing.T) {
	var nilHandler func() (string, error)
	for _, handler := range []any{nil, nilHandler, 123, func() {}, func() error { return nil }, func() (chan int, error) { return nil, nil }, func() (string, string) { return "", "" }, func(...string) (string, error) { return "", nil }} {
		if err := validateQueryHandler(handler); err == nil {
			t.Fatalf("accepted invalid query %T", handler)
		}
	}
	for _, handler := range []any{func() (string, error) { return "", nil }, func(int, string) (map[string]int, error) { return nil, nil }} {
		if err := validateQueryHandler(handler); err != nil {
			t.Fatalf("rejected query %T: %v", handler, err)
		}
	}
	if err := SetQueryHandler(context.Background(), "__reserved", func() (string, error) { return "", nil }); err == nil {
		t.Fatal("accepted reserved query name")
	}
}
