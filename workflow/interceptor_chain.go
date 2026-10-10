package workflow

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"isolate"
	"reflect"
	"time"

	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/log"
)

var instanceInterceptorFactory isolate.Handle
var instanceInterceptorConfig []byte
var activeInterceptors *interceptorChain

type interceptorChain struct {
	inbound  WorkflowInboundInterceptor
	outbound WorkflowOutboundInterceptor
}
type interceptorChainKey struct{}

func configureInterceptors(factory isolate.Handle, config []byte) {
	instanceInterceptorFactory = factory
	instanceInterceptorConfig = bytes.Clone(config)
}

func makeInterceptors() ([]WorkerInterceptor, error) {
	if instanceInterceptorFactory.Name() == "" {
		return nil, nil
	}
	var result []WorkerInterceptor
	err := instanceInterceptorFactory.Invoke(func(args ...isolate.Value) error {
		if len(args) != 1 {
			return errors.New("workflow: invalid interceptor factory arguments")
		}
		slot, ok := args[0].Pointer.(*[]byte)
		if !ok {
			return errors.New("workflow: interceptor factory must accept []byte")
		}
		*slot = bytes.Clone(instanceInterceptorConfig)
		return nil
	}, func(values ...isolate.Value) error {
		if len(values) != 1 {
			return errors.New("workflow: interceptor factory must return []interceptor.WorkerInterceptor")
		}
		var ok bool
		result, ok = values[0].Value.([]WorkerInterceptor)
		if !ok {
			return errors.New("workflow: invalid interceptor factory result")
		}
		for i, interceptor := range result {
			if interceptor == nil || reflect.ValueOf(interceptor).Kind() == reflect.Pointer && reflect.ValueOf(interceptor).IsNil() {
				return fmt.Errorf("workflow: nil interceptor at index %d", i)
			}
		}
		return nil
	})
	return result, err
}

func buildInterceptors(ctx context.Context, execute func(context.Context, *ExecuteWorkflowInput) (any, error)) (*interceptorChain, error) {
	// Constructors must not inherit the cached outbound chain through root
	// context values, especially when constructing a read-only scratch chain.
	ctx = context.WithValue(ctx, interceptorChainKey{}, (*interceptorChain)(nil))
	factories, err := makeInterceptors()
	if err != nil {
		return nil, err
	}
	leaf := &interceptorTerminal{execute: execute}
	var incoming WorkflowInboundInterceptor = leaf
	for i := len(factories) - 1; i >= 0; i-- {
		incoming = factories[i].InterceptWorkflow(ctx, incoming)
		if incoming == nil {
			return nil, errors.New("workflow: interceptor returned a nil inbound chain")
		}
	}
	if err := incoming.Init(&interceptorOutboundTerminal{}); err != nil {
		return nil, err
	}
	if leaf.outbound == nil {
		return nil, errors.New("workflow: interceptor Init must call Next.Init")
	}
	return &interceptorChain{inbound: incoming, outbound: leaf.outbound}, nil
}

func currentOutbound(ctx context.Context) WorkflowOutboundInterceptor {
	if ctx != nil {
		if chain, ok := ctx.Value(interceptorChainKey{}).(*interceptorChain); ok {
			if chain == nil {
				return nil
			}
			return chain.outbound
		}
	}
	// A scratch-owned chain must be explicitly attached to read-only contexts.
	// Never expose the mutable cached workflow chain to queries or validators.
	if isolate.IsReadOnly() || activeInterceptors == nil {
		return nil
	}
	return activeInterceptors.outbound
}

func scratchInterceptors(ctx context.Context) (context.Context, *interceptorChain, error) {
	chain, err := buildInterceptors(ctx, nil)
	if err != nil {
		return ctx, nil, err
	}
	return context.WithValue(ctx, interceptorChainKey{}, chain), chain, nil
}

type interceptorTerminal struct {
	WorkflowInboundInterceptorBase
	outbound WorkflowOutboundInterceptor
	execute  func(context.Context, *ExecuteWorkflowInput) (any, error)
}

func (t *interceptorTerminal) Init(out WorkflowOutboundInterceptor) error {
	t.outbound = out
	return nil
}
func (t *interceptorTerminal) ExecuteWorkflow(ctx context.Context, in *ExecuteWorkflowInput) (any, error) {
	if t.execute == nil {
		return nil, errors.New("workflow: workflow execution is unavailable in a read-only chain")
	}
	return t.execute(ctx, in)
}
func (t *interceptorTerminal) HandleSignal(ctx context.Context, in *HandleSignalInput) error {
	deliver, _ := ctx.Value(interceptedSignalKey{}).(func(*HandleSignalInput) error)
	if deliver == nil {
		return errors.New("workflow: signal delivery is unavailable")
	}
	return deliver(in)
}
func (t *interceptorTerminal) HandleQuery(_ context.Context, in *HandleQueryInput) (any, error) {
	return invokeHandler(queryHandlers[in.QueryType], nil, in.Args)
}
func (t *interceptorTerminal) ValidateUpdate(ctx context.Context, in *UpdateInput) error {
	if skip, _ := ctx.Value(updateSkipValidatorKey{}).(bool); skip {
		return nil
	}
	validator := updateHandlers[in.Name].validator
	if validator == nil {
		return nil
	}
	_, err := invokeHandler(validator, ctx, in.Args)
	return err
}
func (t *interceptorTerminal) ExecuteUpdate(ctx context.Context, in *UpdateInput) (any, error) {
	return invokeHandler(updateHandlers[in.Name].handler, ctx, in.Args)
}

type updateSkipValidatorKey struct{}

// invokeHandler applies interceptor-modified arguments with the same concrete
// type checks as a direct Go invocation. No value leaves the current owner.
func invokeHandler(fn any, ctx context.Context, arguments []any) (any, error) {
	if fn == nil {
		return nil, errors.New("workflow: handler not registered")
	}
	typ := reflect.TypeOf(fn)
	offset := 0
	if ctx != nil && typ.NumIn() != 0 && typ.In(0) == reflect.TypeFor[context.Context]() {
		offset = 1
	}
	if len(arguments) != typ.NumIn()-offset {
		return nil, errors.New("workflow: interceptor argument count mismatch")
	}
	args := make([]reflect.Value, typ.NumIn())
	if offset != 0 {
		args[0] = reflect.ValueOf(ctx)
	}
	for i, arg := range arguments {
		value, err := interceptorArgument(arg, typ.In(i+offset))
		if err != nil {
			return nil, fmt.Errorf("workflow: interceptor argument %d: %w", i, err)
		}
		args[i+offset] = value
	}
	results := reflect.ValueOf(fn).Call(args)
	var result any
	if len(results) == 2 {
		result = results[0].Interface()
	}
	if len(results) != 0 {
		if cause := results[len(results)-1].Interface(); cause != nil {
			return result, cause.(error)
		}
	}
	return result, nil
}
func interceptorArgument(arg any, target reflect.Type) (reflect.Value, error) {
	if arg == nil {
		switch target.Kind() {
		case reflect.Interface, reflect.Pointer, reflect.Slice, reflect.Map, reflect.Chan, reflect.Func:
			return reflect.Zero(target), nil
		default:
			return reflect.Value{}, fmt.Errorf("nil is not assignable to %v", target)
		}
	}
	value := reflect.ValueOf(arg)
	if !value.Type().AssignableTo(target) {
		return reflect.Value{}, fmt.Errorf("%v is not assignable to %v", value.Type(), target)
	}
	return value, nil
}

type interceptedReferenceKey struct{ kind string }
type interceptedReferenceValue struct {
	name string
	fn   any
}

func interceptedReference(ctx context.Context, kind, name string) any {
	if ctx != nil {
		if ref, ok := ctx.Value(interceptedReferenceKey{kind}).(interceptedReferenceValue); ok && ref.name == name {
			return ref.fn
		}
	}
	return name
}
func withInterceptedReference(ctx context.Context, kind, name string, fn any) context.Context {
	return context.WithValue(ctx, interceptedReferenceKey{kind}, interceptedReferenceValue{name: name, fn: fn})
}

type interceptorOutboundTerminal struct {
	WorkflowOutboundInterceptorBase
}

func (*interceptorOutboundTerminal) ExecuteActivity(ctx context.Context, activityType string, args ...any) Future {
	return executeActivity(ctx, interceptedReference(ctx, "activity", activityType), args...)
}
func (*interceptorOutboundTerminal) ExecuteLocalActivity(ctx context.Context, activityType string, args ...any) Future {
	return executeLocalActivity(ctx, interceptedReference(ctx, "activity", activityType), args...)
}
func (*interceptorOutboundTerminal) ExecuteChildWorkflow(ctx context.Context, workflowType string, args ...any) ChildWorkflowFuture {
	return executeChildWorkflow(ctx, interceptedReference(ctx, "workflow", workflowType), args...)
}
func (*interceptorOutboundTerminal) GetInfo(ctx context.Context) *Info { return getInfo(ctx) }
func (*interceptorOutboundTerminal) GetCurrentUpdateInfo(ctx context.Context) *UpdateInfo {
	return getCurrentUpdateInfo(ctx)
}
func (*interceptorOutboundTerminal) GetLogger(ctx context.Context) log.Logger { return getLogger(ctx) }
func (*interceptorOutboundTerminal) GetMetricsHandler(ctx context.Context) client.MetricsHandler {
	return getMetricsHandler(ctx)
}
func (*interceptorOutboundTerminal) Now(ctx context.Context) time.Time { return time.Now() }
func (*interceptorOutboundTerminal) Sleep(ctx context.Context, duration time.Duration) error {
	return sleep(ctx, duration)
}
func (*interceptorOutboundTerminal) RequestCancelExternalWorkflow(ctx context.Context, workflowID, runID string) Future {
	return requestCancelExternalWorkflow(ctx, workflowID, runID)
}
func (*interceptorOutboundTerminal) SignalExternalWorkflow(ctx context.Context, workflowID, runID, signalName string, arg any) Future {
	return signalExternalWorkflow(ctx, workflowID, runID, signalName, arg)
}
func (*interceptorOutboundTerminal) SignalChildWorkflow(ctx context.Context, workflowID, signalName string, arg any) Future {
	return signalChildWorkflow(ctx, workflowID, signalName, arg)
}
func (*interceptorOutboundTerminal) GetSignalChannel(ctx context.Context, signalName string) <-chan SignalResult {
	return getSignalChannel(ctx, signalName)
}
func (*interceptorOutboundTerminal) GetVersion(ctx context.Context, changeID string, minSupported, maxSupported Version) Version {
	return getVersion(ctx, changeID, minSupported, maxSupported)
}
func (*interceptorOutboundTerminal) SetQueryHandler(ctx context.Context, queryType string, handler any) error {
	return setQueryHandlerWithOptions(ctx, queryType, handler, QueryHandlerOptions{})
}
func (*interceptorOutboundTerminal) SetQueryHandlerWithOptions(ctx context.Context, queryType string, handler any, options QueryHandlerOptions) error {
	return setQueryHandlerWithOptions(ctx, queryType, handler, options)
}
func (*interceptorOutboundTerminal) SetUpdateHandler(ctx context.Context, updateName string, handler any, options UpdateHandlerOptions) error {
	return setUpdateHandlerWithOptions(ctx, updateName, handler, options)
}
func (*interceptorOutboundTerminal) IsReplaying(ctx context.Context) bool { return isReplaying(ctx) }
func (*interceptorOutboundTerminal) NewContinueAsNewError(ctx context.Context, fn any, args ...any) error {
	return newContinueAsNewError(ctx, fn, args...)
}
func (*interceptorOutboundTerminal) ExecuteNexusOperation(ctx context.Context, input ExecuteNexusOperationInput) NexusOperationFuture {
	return executeNexusOperation(ctx, input)
}
func (*interceptorOutboundTerminal) RequestCancelNexusOperation(ctx context.Context, input RequestCancelNexusOperationInput) {
	requestCancelNexusOperation(ctx, input)
}
