package testsuite_test

import (
	"testing"

	"github.com/mfateev/sdk-go-poc/testsuite"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/converter"
)

func TestDecodeUpdateResult(t *testing.T) {
	type result struct{ Message string }
	want := result{Message: "copied result"}
	dc := converter.GetDefaultDataConverter()
	payload, err := dc.ToPayload(want)
	require.NoError(t, err)
	raw := converter.NewRawValue(payload)
	require.Equal(t, want, testsuite.DecodeResult[result](want))
	require.Equal(t, want, testsuite.DecodeResult[result](raw))
	require.Equal(t, want, testsuite.DecodeResult[result](raw, dc))
	require.Panics(t, func() { testsuite.DecodeResult[string](raw) })
	require.Panics(t, func() { testsuite.DecodeResult[result](42) })
	require.Panics(t, func() { testsuite.DecodeResult[result](raw, dc, dc) })

	// Respect the supplied converter instead of silently using the default.
	bytesOnly := converter.NewCompositeDataConverter(converter.NewByteSlicePayloadConverter())
	require.Panics(t, func() { testsuite.DecodeResult[result](raw, bytesOnly) })
}
