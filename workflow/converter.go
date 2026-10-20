package workflow

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	commonpb "go.temporal.io/api/common/v1"
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
	if isolate.IsReadOnly() {
		return protectProtobuf(newDefaultDataConverter())
	}
	return protectProtobuf(instanceDataConverter)
}

// ContextAware tailors serialization to native workflow and host/activity
// contexts. Implementations must return an independently usable converter;
// WithWorkflowContext also runs in read-only queries and update validators.
// Neither method may mutate its parent converter or the supplied context.
type ContextAware interface {
	WithWorkflowContext(context.Context) converter.DataConverter
	WithContext(context.Context) converter.DataConverter
}

type dataConverterKey struct{}

// WithDataConverter overrides serialization for operations using this derived
// context. The converter must be constructed inside the isolate, be deterministic,
// and support concurrent use by native workflow goroutines. Queries/validators
// cannot mutate a captured converter: ContextAware implementations can construct
// scratch-owned state in WithWorkflowContext. Host codecs remain on the host.
func WithDataConverter(ctx context.Context, dc converter.DataConverter) context.Context {
	if ctx == nil {
		panic("workflow: nil context")
	}
	if isNilConverter(dc) {
		panic("workflow: nil data converter")
	}
	return context.WithValue(ctx, dataConverterKey{}, dc)
}

// WithWorkflowContext binds the optional native context-aware interface. The
// standard WithContext method is also supported for converters using one native
// context interface in both workflows and activities.
func WithWorkflowContext(ctx context.Context, dc converter.DataConverter) converter.DataConverter {
	if aware, ok := dc.(interface {
		WithWorkflowContext(context.Context) converter.DataConverter
	}); ok {
		dc = aware.WithWorkflowContext(ctx)
	} else if aware, ok := dc.(interface {
		WithContext(context.Context) converter.DataConverter
	}); ok {
		dc = aware.WithContext(ctx)
	}
	if isNilConverter(dc) {
		panic("workflow: context-aware data converter returned nil")
	}
	return dc
}

func WithContext(ctx context.Context, dc converter.DataConverter) converter.DataConverter {
	if aware, ok := dc.(interface {
		WithContext(context.Context) converter.DataConverter
	}); ok {
		dc = aware.WithContext(ctx)
	}
	if isNilConverter(dc) {
		panic("workflow: context-aware data converter returned nil")
	}
	return dc
}

func isNilConverter(dc converter.DataConverter) bool {
	return dc == nil || reflect.ValueOf(dc).Kind() == reflect.Pointer && reflect.ValueOf(dc).IsNil()
}

// GetDataConverter returns the converter selected and bound to ctx. Futures
// capture it when scheduled, rather than using the context passed to Get.
func GetDataConverter(ctx context.Context) converter.DataConverter {
	if ctx == nil {
		panic("workflow: nil context")
	}
	dc, _ := ctx.Value(dataConverterKey{}).(converter.DataConverter)
	if dc == nil {
		dc = currentDataConverter()
	}
	return protectProtobuf(WithWorkflowContext(ctx, dc))
}

func rootDataConverter() converter.DataConverter {
	if rootContext != nil {
		return GetDataConverter(rootContext)
	}
	return currentDataConverter()
}

func encodeContextArgs(ctx context.Context, args []any) (*commonpb.Payloads, error) {
	if ctx == nil {
		return nil, fmt.Errorf("workflow: nil context")
	}
	return encodeArgsWithConverter(args, GetDataConverter(ctx))
}
