// Package failurecodec preserves SDK error types across the copied-byte
// boundary using owner-local failure and data converters.
package failurecodec

import (
	"context"
	"errors"

	"github.com/mfateev/sdk-go-poc/internal/failurewire"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/temporal"
)

func Encode(cause error, dc converter.DataConverter) ([]byte, error) {
	return EncodeWithConverter(cause, temporal.NewDefaultFailureConverter(temporal.DefaultFailureConverterOptions{DataConverter: dc}))
}

func EncodeWithConverter(cause error, fc converter.FailureConverter) ([]byte, error) {
	if canceled, ok := cause.(*cancellationError); ok {
		cause = canceled.cause
	}
	if errors.Is(cause, context.Canceled) && !temporal.IsCanceledError(cause) {
		cause = temporal.NewCanceledError()
	}
	failure := fc.ErrorToFailure(cause)
	if cause != nil && failure == nil {
		return nil, errors.New("failure converter returned nil failure for a non-nil error")
	}
	return failurewire.Encode(failure)
}

// Decode separates malformed transport data from the decoded operation error.
func Decode(data []byte, dc converter.DataConverter) (error, error) {
	return DecodeWithConverter(data, temporal.NewDefaultFailureConverter(temporal.DefaultFailureConverterOptions{DataConverter: dc}))
}

func DecodeWithConverter(data []byte, fc converter.FailureConverter) (error, error) {
	failure, err := failurewire.Decode(data)
	if err != nil {
		return nil, err
	}
	cause := fc.FailureToError(failure)
	if failure != nil && cause == nil {
		return nil, errors.New("failure converter returned nil error for a non-nil failure")
	}
	return cause, nil
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
