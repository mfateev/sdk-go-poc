package temporalbridge

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/converter"
)

func TestLocalArgumentsOptionalContext(t *testing.T) {
	dc := converter.GetDefaultDataConverter()
	for _, fn := range []any{func(int64, *int) error { return nil }, func(context.Context, int64, *int) error { return nil }} {
		payloads, err := dc.ToPayloads(int64(1<<60+3), nil)
		require.NoError(t, err)
		args, err := localArguments(fn, payloads, dc)
		require.NoError(t, err)
		require.Equal(t, int64(1<<60+3), args[0])
		require.Nil(t, args[1])
		_, err = localArguments(fn, nil, dc)
		require.ErrorContains(t, err, "want 2")
	}
	for _, fn := range []any{func() error { return nil }, func(context.Context) error { return nil }} {
		args, err := localArguments(fn, nil, dc)
		require.NoError(t, err)
		require.Empty(t, args)
	}
}
