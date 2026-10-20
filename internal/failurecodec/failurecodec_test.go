package failurecodec_test

import (
	"context"
	"errors"
	"testing"

	"github.com/mfateev/sdk-go-poc/example/failureconversion/support"
	"github.com/mfateev/sdk-go-poc/internal/failurecodec"

	failurepb "go.temporal.io/api/failure/v1"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/temporal"
)

func TestCustomFailureRoundTripAndCancellation(t *testing.T) {
	fc := support.New(converter.GetDefaultDataConverter())
	raw, err := failurecodec.EncodeWithConverter(support.CustomError{Detail: support.Precise}, fc)
	if err != nil {
		t.Fatal(err)
	}
	f := raw
	if f.GetApplicationFailureInfo().GetType() != support.ErrorType || !f.GetApplicationFailureInfo().GetNonRetryable() {
		t.Fatal("custom retry policy lost")
	}
	cause, err := failurecodec.DecodeWithConverter(raw, fc)
	var custom support.CustomError
	if err != nil || !errors.As(cause, &custom) || custom.Detail != support.Precise {
		t.Fatalf("custom error=%v transport=%v", cause, err)
	}
	raw, err = failurecodec.EncodeWithConverter(failurecodec.WithCancellation(temporal.NewCanceledError(), context.Canceled), fc)
	if err != nil {
		t.Fatal(err)
	}
	cause, err = failurecodec.DecodeWithConverter(raw, fc)
	if err != nil || !temporal.IsCanceledError(cause) {
		t.Fatalf("cancellation=%v %v", cause, err)
	}

}

type droppingConverter struct{}

func (droppingConverter) ErrorToFailure(error) *failurepb.Failure { return nil }
func (droppingConverter) FailureToError(*failurepb.Failure) error { return nil }

func TestCustomConverterCannotSilentlyDropFailures(t *testing.T) {
	if _, err := failurecodec.EncodeWithConverter(errors.New("failure"), droppingConverter{}); err == nil {
		t.Fatal("dropped error accepted")
	}
	raw, err := failurecodec.Encode(errors.New("failure"), converter.GetDefaultDataConverter())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := failurecodec.DecodeWithConverter(raw, droppingConverter{}); err == nil {
		t.Fatal("dropped failure accepted")
	}
}
