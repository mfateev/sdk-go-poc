package workflow

import (
	"context"
	"testing"
)

func TestUpdateHandlerSignatures(t *testing.T) {
	handler := func(context.Context, int) (string, error) { return "", nil }
	for _, test := range []struct {
		handler, validator any
		valid              bool
	}{
		{handler, nil, true},
		{handler, func(int) error { return nil }, true},
		{handler, func(context.Context, int) error { return nil }, true},
		{func(context.Context) error { return nil }, func() error { return nil }, true},
		{nil, nil, false}, {42, nil, false}, {func(int) error { return nil }, nil, false},
		{func(context.Context) {}, nil, false},
		{func(context.Context) (chan int, error) { return nil, nil }, nil, false},
		{handler, 42, false},
		{handler, func(string) error { return nil }, false},
		{handler, func(int) {}, false},
		{handler, (func(int) error)(nil), false},
		{func(context.Context, ...int) error { return nil }, nil, false},
	} {
		if err := validateUpdateFunctions(test.handler, test.validator); (err == nil) != test.valid {
			t.Errorf("handler=%T validator=%T: %v", test.handler, test.validator, err)
		}
	}
}
