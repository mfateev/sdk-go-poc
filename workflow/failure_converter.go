package workflow

import (
	"bytes"
	"errors"
	failurepb "go.temporal.io/api/failure/v1"
	"isolate"
	"reflect"

	"github.com/mfateev/sdk-go-poc/internal/failurecodec"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/temporal"
)

var instanceFailureConverterFactory isolate.Handle
var instanceFailureConverterConfig []byte

func configureFailureConverter(factory isolate.Handle, config []byte, dc converter.DataConverter) error {
	if factory.Name() == "" {
		return nil
	}
	if _, err := makeFailureConverter(factory, config, dc); err != nil {
		return err
	}
	instanceFailureConverterFactory = factory
	instanceFailureConverterConfig = bytes.Clone(config)
	return nil
}

// Fresh instances receive the operation's bound serializer, without retaining
// mutable workflow caches in a query or validator's scratch owner.
func makeFailureConverter(factory isolate.Handle, config []byte, dc converter.DataConverter) (converter.FailureConverter, error) {
	if factory.Name() == "" {
		return temporal.NewDefaultFailureConverter(temporal.DefaultFailureConverterOptions{DataConverter: dc}), nil
	}
	var fc converter.FailureConverter
	err := factory.Invoke(func(args ...isolate.Value) error {
		if len(args) != 2 {
			return errors.New("workflow: failure converter factory must accept []byte and converter.DataConverter")
		}
		configSlot, ok := args[0].Pointer.(*[]byte)
		if !ok {
			return errors.New("workflow: invalid failure converter configuration parameter")
		}
		dcSlot, ok := args[1].Pointer.(*converter.DataConverter)
		if !ok {
			return errors.New("workflow: invalid failure converter data converter parameter")
		}
		*configSlot, *dcSlot = bytes.Clone(config), dc
		return nil
	}, func(values ...isolate.Value) error {
		if len(values) != 1 {
			return errors.New("workflow: failure converter factory must return converter.FailureConverter")
		}
		var ok bool
		fc, ok = values[0].Value.(converter.FailureConverter)
		if !ok || fc == nil || reflect.ValueOf(fc).Kind() == reflect.Pointer && reflect.ValueOf(fc).IsNil() {
			return errors.New("workflow: failure converter factory returned nil")
		}
		return nil
	})
	return fc, err
}

func encodeFailure(cause error, dc converter.DataConverter) (*failurepb.Failure, error) {
	fc, err := makeFailureConverter(instanceFailureConverterFactory, instanceFailureConverterConfig, dc)
	if err != nil {
		return nil, err
	}
	return failurecodec.EncodeWithConverter(cause, fc)
}

func decodeFailure(data *failurepb.Failure, dc converter.DataConverter) (error, error) {
	fc, err := makeFailureConverter(instanceFailureConverterFactory, instanceFailureConverterConfig, dc)
	if err != nil {
		return nil, err
	}
	return failurecodec.DecodeWithConverter(data, fc)
}
