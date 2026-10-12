package workflow

import (
	"context"
	"fmt"
	"testing"
)

func TestUpstreamQueryFnValidation(t *testing.T) {
	for i, tc := range []struct {
		fn    any
		valid bool
	}{
		{"not a function", false}, {func() {}, false}, {func() error { return nil }, false},
		{func(int) error { return nil }, false}, {func(int, int, string) error { return nil }, false},
		{func(int) (chan int, error) { return nil, nil }, false},
		{func(int) (int, error) { return 0, nil }, true},
		{func(int, int, string) (int, error) { return 0, nil }, true},
	} {
		t.Run(fmt.Sprint(i), func(t *testing.T) {
			if err := validateQueryHandler(tc.fn); (err == nil) != tc.valid {
				t.Fatalf("signature %T: %v", tc.fn, err)
			}
		})
	}
}

func TestUpstreamUpdateHandlerFnValidation(t *testing.T) {
	for i, tc := range []struct {
		fn    any
		valid bool
	}{
		{"not a function", false}, {func() {}, false}, {func() int { return 0 }, false},
		{func(context.Context, int) (int, int, error) { return 0, 0, nil }, false},
		{func(int) (chan int, error) { return nil, nil }, false}, {func() error { return nil }, false},
		{func(int, int, string) error { return nil }, false}, {func(int) error { return nil }, false},
		{func(context.Context, int) error { return nil }, true},
		{func(context.Context, int, int, string) error { return nil }, true},
		{func(context.Context) error { return nil }, true},
	} {
		t.Run(fmt.Sprint(i), func(t *testing.T) {
			if err := validateUpdateFunctions(tc.fn, nil); (err == nil) != tc.valid {
				t.Fatalf("signature %T: %v", tc.fn, err)
			}
		})
	}
}

func TestUpstreamUpdateValidatorFnValidation(t *testing.T) {
	handler := func(context.Context, int) error { return nil }
	for i, tc := range []struct {
		fn    any
		valid bool
	}{
		{"not a function", false}, {func() {}, false}, {func(int) (int, error) { return 0, nil }, false},
		{func(int) (chan int, error) { return nil, nil }, false}, {func(int) error { return nil }, true},
		{func(context.Context, int) error { return nil }, true}, {func(int) error { return nil }, true},
	} {
		t.Run(fmt.Sprint(i), func(t *testing.T) {
			if err := validateUpdateFunctions(handler, tc.fn); (err == nil) != tc.valid {
				t.Fatalf("signature %T: %v", tc.fn, err)
			}
		})
	}
	for _, tc := range []struct{ handler, validator any }{
		{func(context.Context) error { return nil }, func() error { return nil }},
		{func(context.Context, int, int, string) error { return nil }, func(int, int, string) error { return nil }},
	} {
		if err := validateUpdateFunctions(tc.handler, tc.validator); err != nil {
			t.Fatal(err)
		}
	}
}

func TestUpstreamEquivalentParams(t *testing.T) {
	for i, tc := range []struct {
		handler, validator any
		valid              bool
	}{
		{func(context.Context) error { return nil }, func() error { return nil }, true},
		{func(context.Context, int) error { return nil }, func(int) error { return nil }, true},
		{func(context.Context, int) error { return nil }, func(context.Context, int) error { return nil }, true},
		{func(context.Context, error) error { return nil }, func(error) error { return nil }, true},
		{func(context.Context, int) error { return nil }, func(int, ...struct{}) error { return nil }, false},
		{func(context.Context, int) error { return nil }, func(string) error { return nil }, false},
		{func(context.Context, int) error { return nil }, func(int, string) error { return nil }, false},
		{func(context.Context, int, chan struct{}, int) error { return nil }, func(int, string, int) error { return nil }, false},
		{func(context.Context, int) error { return nil }, func(any) error { return nil }, false},
		{func(context.Context, int) error { return nil }, "", false},
		{"", func(int) error { return nil }, false},
	} {
		t.Run(fmt.Sprint(i), func(t *testing.T) {
			if err := validateUpdateFunctions(tc.handler, tc.validator); (err == nil) != tc.valid {
				t.Fatalf("handler=%T validator=%T: %v", tc.handler, tc.validator, err)
			}
		})
	}
}
