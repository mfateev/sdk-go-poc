package workflow

import (
	"context"
	"fmt"
	"github.com/mfateev/sdk-go-poc/hostintegration"
	"isolate"
	"reflect"

	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/sdk/converter"
	"google.golang.org/protobuf/proto"
)

// Read-only marshal may update a generated message's size cache. Clone just
// message values into scratch memory first; descriptors remain opaque handles
// and coder tables are host-owned behind SDK-generated host operations.
type protobufConverter struct{ converter.DataConverter }

func protectProtobuf(dc converter.DataConverter) converter.DataConverter {
	if _, ok := dc.(*protobufConverter); ok {
		return dc
	}
	return &protobufConverter{DataConverter: dc}
}

func protobufInput(value any) (any, error) {
	if isUnsupportedProtoValue(value) {
		return nil, fmt.Errorf("workflow: unsupported protobuf type %T; use a generated Google message without extensions", value)
	}
	if !isolate.IsReadOnly() || !hostintegration.IsGenerated(value) {
		return value, nil
	}
	if message, ok := value.(proto.Message); ok {
		return proto.Clone(message), nil
	}
	v := reflect.ValueOf(value)
	if v.Kind() == reflect.Struct {
		pointer := reflect.New(v.Type())
		pointer.Elem().Set(v)
		return proto.Clone(pointer.Interface().(proto.Message)), nil
	}
	return value, nil
}
func (dc *protobufConverter) ToPayload(value any) (*commonpb.Payload, error) {
	value, err := protobufInput(value)
	if err != nil {
		return nil, err
	}
	return dc.DataConverter.ToPayload(value)
}
func (dc *protobufConverter) ToPayloads(values ...any) (*commonpb.Payloads, error) {
	inputs := make([]any, len(values))
	for i, value := range values {
		var err error
		inputs[i], err = protobufInput(value)
		if err != nil {
			return nil, err
		}
	}
	return dc.DataConverter.ToPayloads(inputs...)
}
func (dc *protobufConverter) FromPayload(payload *commonpb.Payload, value any) error {
	if isUnsupportedProtoValue(value) {
		return fmt.Errorf("workflow: unsupported protobuf destination %T", value)
	}
	return dc.DataConverter.FromPayload(payload, value)
}
func (dc *protobufConverter) FromPayloads(payloads *commonpb.Payloads, values ...any) error {
	for _, value := range values {
		if isUnsupportedProtoValue(value) {
			return fmt.Errorf("workflow: unsupported protobuf destination %T", value)
		}
	}
	return dc.DataConverter.FromPayloads(payloads, values...)
}

func (dc *protobufConverter) WithWorkflowContext(ctx context.Context) converter.DataConverter {
	return protectProtobuf(WithWorkflowContext(ctx, dc.DataConverter))
}
func (dc *protobufConverter) WithContext(ctx context.Context) converter.DataConverter {
	return protectProtobuf(WithContext(ctx, dc.DataConverter))
}
