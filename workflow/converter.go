package workflow

import (
	"bytes"
	"errors"
	"isolate"
	"reflect"

	"go.temporal.io/sdk/converter"
)

// This package's globals are initialized separately for every isolate. Build
// the pinned SDK's default converter here so its converter map, ordered list,
// and mutable converter values belong to that instance. Type metadata still uses the
// audited process services. The host worker keeps its ordinary default object.
// Keep this order aligned with sdk v1.49.0/converter/default_data_converter.go.
var instanceDataConverter = newDefaultDataConverter()

func newDefaultDataConverter() converter.DataConverter {
	return converter.NewCompositeDataConverter(
		converter.NewNilPayloadConverter(),
		converter.NewByteSlicePayloadConverter(),
		converter.NewProtoJSONPayloadConverter(),
		converter.NewProtoPayloadConverter(),
		converter.NewJSONPayloadConverter(),
	)
}

var instanceConverterFactory isolate.Handle
var instanceConverterConfig []byte

func configureDataConverter(factory isolate.Handle, config []byte) error {
	if factory.Name() == "" {
		return nil
	}
	dc, err := makeDataConverter(factory, config)
	if err != nil {
		return err
	}
	instanceConverterFactory = factory
	instanceConverterConfig = bytes.Clone(config)
	instanceDataConverter = dc
	return nil
}

func makeDataConverter(factory isolate.Handle, config []byte) (converter.DataConverter, error) {
	var dc converter.DataConverter
	err := factory.Invoke(func(args ...isolate.Value) error {
		if len(args) != 1 {
			return errors.New("workflow: invalid data converter factory arguments")
		}
		p, ok := args[0].Pointer.(*[]byte)
		if !ok {
			return errors.New("workflow: data converter factory must accept []byte")
		}
		*p = bytes.Clone(config)
		return nil
	}, func(results ...isolate.Value) error {
		if len(results) != 1 {
			return errors.New("workflow: data converter factory must return a converter")
		}
		var ok bool
		dc, ok = results[0].Value.(converter.DataConverter)
		if !ok || dc == nil || reflect.ValueOf(dc).Kind() == reflect.Pointer && reflect.ValueOf(dc).IsNil() {
			return errors.New("workflow: data converter factory returned nil")
		}
		return nil
	})
	return dc, err
}

// Read-only execution cannot mutate the workflow's converter caches. Build a
// scratch-owned converter for each conversion, using the same copied config.
// Factory callbacks remain subject to read-only ownership and effect checks.
func currentDataConverter() converter.DataConverter {
	if isolate.IsReadOnly() && instanceConverterFactory.Name() != "" {
		dc, err := makeDataConverter(instanceConverterFactory, instanceConverterConfig)
		if err != nil {
			panic(err)
		}
		return dc
	}
	return instanceDataConverter
}
