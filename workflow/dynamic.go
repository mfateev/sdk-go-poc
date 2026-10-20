package workflow

import (
	"context"
	"errors"
	"reflect"
	"strings"

	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/temporal"
	goWorkflow "go.temporal.io/sdk/workflow"
)

type DynamicRegisterOptions = goWorkflow.DynamicRegisterOptions
type DynamicRuntimeOptions = goWorkflow.DynamicRuntimeOptions
type LoadDynamicRuntimeOptionsDetails = goWorkflow.LoadDynamicRuntimeOptionsDetails

// Encoded arguments and their converter belong to the current isolate owner.
// Read-only handlers receive fresh payloads and scratch converters.
type encodedArguments struct {
	payloads  *commonpb.Payloads
	converter converter.DataConverter
}

func (v encodedArguments) HasValues() bool { return len(v.payloads.GetPayloads()) != 0 }
func (v encodedArguments) Get(pointers ...any) error {
	if !v.HasValues() {
		return temporal.ErrNoData
	}
	for _, pointer := range pointers {
		if isUnsupportedProtoValue(pointer) {
			return errors.New("workflow: legacy gogo protobuf messages are unsupported; use Google protobuf")
		}
	}
	return v.converter.FromPayloads(v.payloads, pointers...)
}

var dynamicQueryHandler any
var dynamicQueryContext context.Context
var dynamicUpdateHandler updateHandler

// SetDynamicQueryHandler installs a fallback for unregistered query names.
// The signature is func(string, converter.EncodedValues) (result, error).
// Options and replacement behavior match SetQueryHandlerWithOptions.
func SetDynamicQueryHandler(ctx context.Context, handler any, options QueryHandlerOptions) error {
	assertWritable()
	if ctx == nil {
		return errors.New("workflow: nil context")
	}
	if err := validateQueryHandler(handler); err != nil {
		return err
	}
	t := reflect.TypeOf(handler)
	if t.NumIn() != 2 || t.In(0) != reflect.TypeFor[string]() || t.In(1) != reflect.TypeFor[converter.EncodedValues]() {
		return errors.New("dynamic query handler must take (string, converter.EncodedValues)")
	}
	dynamicQueryHandler, dynamicQueryContext = handler, ctx
	return registerQuery(QueryRegistration{Dynamic: true, Options: options})
}

// SetDynamicUpdateHandler installs a fallback for unregistered update names.
// The handler takes (context.Context, string, converter.EncodedValues) and
// returns error or (result, error). Its optional validator takes the same name
// and encoded values, with an optional context, and executes read-only.
func SetDynamicUpdateHandler(ctx context.Context, handler any, options UpdateHandlerOptions) error {
	assertWritable()
	if ctx == nil {
		return errors.New("workflow: nil context")
	}
	if err := validateUpdateFunctions(handler, options.Validator); err != nil {
		return err
	}
	t := reflect.TypeOf(handler)
	if t.NumIn() != 3 || t.In(1) != reflect.TypeFor[string]() || t.In(2) != reflect.TypeFor[converter.EncodedValues]() {
		return errors.New("dynamic update handler must take (context.Context, string, converter.EncodedValues)")
	}
	dynamicUpdateHandler = updateHandler{handler: handler, validator: options.Validator, ctx: ctx, dynamic: true}
	return registerUpdate(UpdateRegistration{Dynamic: true, UnfinishedPolicy: options.UnfinishedPolicy, Description: options.Description})
}

func resolveQueryHandler(name string) (any, context.Context, bool) {
	if handler := queryHandlers[name]; handler != nil {
		return handler, queryContexts[name], false
	}
	if !strings.HasPrefix(name, "__") {
		return dynamicQueryHandler, dynamicQueryContext, true
	}
	return nil, nil, false
}
func resolveUpdateHandler(name string) updateHandler {
	if h, ok := updateHandlers[name]; ok {
		return h
	}
	if !strings.HasPrefix(name, "__") {
		return dynamicUpdateHandler
	}
	return updateHandler{}
}
func updateHandlerArguments(h updateHandler, fn any, ctx context.Context, requestName string, raw *commonpb.Payloads) ([]reflect.Value, error) {
	if !h.dynamic {
		return updateArguments(fn, ctx, raw)
	}
	dc := GetDataConverter(ctx)
	payloads, err := decodePayloads(raw, dc)
	if err != nil {
		return nil, err
	}
	values := []reflect.Value{reflect.ValueOf(requestName), reflect.ValueOf(encodedArguments{payloads, dc})}
	if reflect.TypeOf(fn).In(0) == reflect.TypeFor[context.Context]() {
		values = append([]reflect.Value{reflect.ValueOf(ctx)}, values...)
	}
	return values, nil
}
