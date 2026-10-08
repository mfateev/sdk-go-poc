// Package failurecodec preserves SDK error types across the copied-byte
// boundary using the pinned default failure and data converters.
package failurecodec

import (
	"context"
	"errors"

	"github.com/mfateev/sdk-go-poc/internal/failurewire"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/temporal"
)

func Encode(cause error, dc converter.DataConverter) ([]byte, error) {
	if canceled, ok := cause.(*cancellationError); ok {
		cause = canceled.cause
	}
	if errors.Is(cause, context.Canceled) && !temporal.IsCanceledError(cause) {
		cause = temporal.NewCanceledError()
	}
	fc := temporal.NewDefaultFailureConverter(temporal.DefaultFailureConverterOptions{DataConverter: dc})
	return failurewire.Encode(fc.ErrorToFailure(cause))
}

// Decode separates malformed transport data from the decoded operation error.
func Decode(data []byte, dc converter.DataConverter) (error, error) {
	failure, err := failurewire.Decode(data)
	if err != nil {
		return nil, err
	}
	fc := temporal.NewDefaultFailureConverter(temporal.DefaultFailureConverterOptions{DataConverter: dc})
	return fc.FailureToError(failure), nil
}

// WithCancellation preserves errors.As to the original SDK error while also
// supporting errors.Is with the native execution context's cancellation cause.
func WithCancellation(cause, reason error) error {
	if reason == nil {
		return cause
	}
	return &cancellationError{cause: cause, reason: reason}
}

type cancellationError struct{ cause, reason error }

func (e *cancellationError) Error() string        { return e.cause.Error() }
func (e *cancellationError) Unwrap() error        { return e.cause }
func (e *cancellationError) Is(target error) bool { return errors.Is(e.reason, target) }
