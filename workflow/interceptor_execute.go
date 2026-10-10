package workflow

import (
	"context"
	"errors"
	"fmt"
	"isolate"
	"reflect"

	commonpb "go.temporal.io/api/common/v1"
)

func runInterceptedFunction(handle isolate.Handle, ctx context.Context, payloads *commonpb.Payloads) error {
	signature := handle.Signature()
	if signature.NumIn() == 0 || signature.In(0) != reflect.TypeFor[context.Context]() {
		return completePayloads(nil, errors.New("workflow: isolate workflow must take context.Context first"))
	}
	if err := checkArgumentCount(payloads, signature.NumIn()-1); err != nil {
		return completePayloads(nil, err)
	}
	pointers := make([]any, signature.NumIn()-1)
	for i := range pointers {
		pointers[i] = reflect.New(signature.In(i + 1)).Interface()
		if isProtoValue(pointers[i]) {
			return completePayloads(nil, errors.New("workflow: protobuf values are outside the isolate POC subset"))
		}
	}
	if err := currentDataConverter().FromPayloads(payloads, pointers...); err != nil {
		return completePayloads(nil, fmt.Errorf("workflow: decode arguments: %w", err))
	}
	input := &ExecuteWorkflowInput{Args: make([]any, len(pointers))}
	for i, pointer := range pointers {
		input.Args[i] = reflect.ValueOf(pointer).Elem().Interface()
	}
	chain, err := buildInterceptors(ctx, func(ctx context.Context, input *ExecuteWorkflowInput) (any, error) {
		rootContext = ctx
		// Admit buffered history signals after the inbound workflow hooks have
		// installed their context, before invoking application workflow code.
		if _, err := isolate.Call(OpFlushSignals, nil); err != nil {
			return nil, err
		}
		var result any
		err := handle.Invoke(func(slots ...isolate.Value) error {
			if len(slots) != len(input.Args)+1 {
				return errors.New("workflow: interceptor argument count mismatch")
			}
			*slots[0].Pointer.(*context.Context) = ctx
			for i, arg := range input.Args {
				target := reflect.ValueOf(slots[i+1].Pointer).Elem()
				value, err := interceptorArgument(arg, target.Type())
				if err != nil {
					return fmt.Errorf("workflow: interceptor argument %d: %w", i, err)
				}
				target.Set(value)
			}
			return nil
		}, func(values ...isolate.Value) error {
			if len(values) != 0 {
				result = values[0].Value
			}
			return nil
		})
		return result, err
	})
	if err != nil {
		return completePayloads(nil, err)
	}
	activeInterceptors = chain
	ctx = context.WithValue(ctx, interceptorChainKey{}, chain)
	rootContext = ctx
	go serveInterceptedSignals()
	result, cause := chain.inbound.ExecuteWorkflow(ctx, input)
	// An immediate workflow return must not discard admitted signal hooks.
	if _, err := isolate.Call(OpFlushSignals, nil); err != nil {
		return completePayloads(nil, err)
	}
	if cause != nil {
		return completePayloads(nil, cause)
	}
	if signature.NumOut() < 2 && result == nil {
		return completePayloads(nil, nil)
	}
	if isProtoValue(result) {
		return completePayloads(nil, errors.New("workflow: protobuf values are outside the isolate POC subset"))
	}
	encoded, err := currentDataConverter().ToPayloads(result)
	if err != nil {
		return completePayloads(nil, fmt.Errorf("workflow: encode result: %w", err))
	}
	return completePayloads(encoded, nil)
}
