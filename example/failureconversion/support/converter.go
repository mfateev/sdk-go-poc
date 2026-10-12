package support

import (
	"errors"
	"sync/atomic"

	failurepb "go.temporal.io/api/failure/v1"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/temporal"
)

const ErrorType = "CUSTOM ERROR"
const Precise int64 = 9007199254740993

type CustomError struct{ Detail int64 }

func (CustomError) Error() string { return "this is a custom error type" }

type failureConverter struct {
	dc       converter.DataConverter
	fallback converter.FailureConverter
	calls    atomic.Uint64
}

func New(dc converter.DataConverter) converter.FailureConverter {
	return &failureConverter{dc: dc, fallback: temporal.NewDefaultFailureConverter(temporal.DefaultFailureConverterOptions{DataConverter: dc})}
}

//go:isolate
func Factory(config []byte, dc converter.DataConverter) (converter.FailureConverter, error) {
	if string(config) == "factory-error" {
		return nil, errors.New("custom failure converter factory failed")
	}
	if string(config) == "nil-converter" {
		return (*failureConverter)(nil), nil
	}
	if string(config) != ErrorType {
		return nil, errors.New("failure converter configuration changed")
	}
	return New(dc), nil
}

func (fc *failureConverter) ErrorToFailure(err error) *failurepb.Failure {
	fc.calls.Add(1) // Must be scratch-owned during read-only validation.
	var custom CustomError
	if errors.As(err, &custom) {
		details, err := fc.dc.ToPayloads(custom.Detail)
		if err != nil {
			panic(err)
		}
		return &failurepb.Failure{Message: custom.Error(), FailureInfo: &failurepb.Failure_ApplicationFailureInfo{
			ApplicationFailureInfo: &failurepb.ApplicationFailureInfo{Type: ErrorType, NonRetryable: true, Details: details},
		}}
	}
	return fc.fallback.ErrorToFailure(err)
}

func (fc *failureConverter) FailureToError(f *failurepb.Failure) error {
	fc.calls.Add(1)
	if f.GetApplicationFailureInfo().GetType() == ErrorType {
		var detail int64
		if err := fc.dc.FromPayloads(f.GetApplicationFailureInfo().GetDetails(), &detail); err != nil {
			panic(err)
		}
		return CustomError{Detail: detail}
	}
	return fc.fallback.FailureToError(f)
}
