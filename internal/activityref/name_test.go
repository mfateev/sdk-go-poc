package activityref

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestValidateOptionalContextArgs(t *testing.T) {
	for _, fn := range []any{func(int) error { return nil }, func(context.Context, int) error { return nil }} {
		require.NoError(t, ValidateArgs(fn, []any{4}))
		require.ErrorContains(t, ValidateArgs(fn, nil), "expected 1 args")
		require.ErrorContains(t, ValidateArgs(fn, []any{4, 5}), "expected 1 args")
		require.ErrorContains(t, ValidateArgs(fn, []any{"bad"}), "cannot assign function argument")
	}
	for _, fn := range []any{func() error { return nil }, func(context.Context) error { return nil }} {
		require.NoError(t, ValidateArgs(fn, nil))
		require.ErrorContains(t, ValidateArgs(fn, []any{4}), "expected 0 args")
	}
	require.NoError(t, ValidateArgs(func(*int) error { return nil }, []any{nil}))
}
