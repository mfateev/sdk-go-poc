package workflow

import (
	"context"
	"errors"
	"fmt"
	commonpb "go.temporal.io/api/common/v1"
	"isolate"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/mfateev/sdk-go-poc/internal/tracecontext"
	failurepb "go.temporal.io/api/failure/v1"
	goWorkflow "go.temporal.io/sdk/workflow"
)

type UpdateHandlerOptions = goWorkflow.UpdateHandlerOptions
type HandlerUnfinishedPolicy = goWorkflow.HandlerUnfinishedPolicy
type UpdateInfo = goWorkflow.UpdateInfo

const (
	HandlerUnfinishedPolicyWarnAndAbandon = goWorkflow.HandlerUnfinishedPolicyWarnAndAbandon
	HandlerUnfinishedPolicyAbandon        = goWorkflow.HandlerUnfinishedPolicyAbandon
)

type UpdateRegistration struct {
	Dynamic          bool                    `json:"dynamic,omitempty"`
	Name             string                  `json:"name"`
	UnfinishedPolicy HandlerUnfinishedPolicy `json:"unfinished_policy"`
	Description      string                  `json:"description,omitempty"`
}

type UpdateRequest struct {
	Header   *commonpb.Header   `json:"header,omitempty"`
	ID       string             `json:"id"`
	Name     string             `json:"name"`
	Payloads *commonpb.Payloads `json:"payloads"`
}

type UpdateCompletion struct {
	ID       string             `json:"id"`
	Payloads *commonpb.Payloads `json:"payloads,omitempty"`
	Failure  *failurepb.Failure `json:"failure,omitempty"`
}

type updateHandler struct {
	dynamic            bool
	handler, validator any
	ctx                context.Context
}
type updateInfoKey struct{}
type updateValidationKey struct{}

var updateHandlers = make(map[string]updateHandler)
var runningUpdateCount atomic.Int64
var updateServiceStarted bool
var updateWaitMu sync.Mutex
var updateFinished = func() chan struct{} { c := make(chan struct{}); close(c); return c }()

// WaitHandlersFinished waits until accepted update handlers have posted their
// completion messages. Call it before completing or continuing a workflow.
func WaitHandlersFinished(ctx context.Context) error {
	assertWritable()
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		updateWaitMu.Lock()
		if runningUpdateCount.Load() == 0 {
			updateWaitMu.Unlock()
			return nil
		}
		done := updateFinished
		updateWaitMu.Unlock()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-done:
			// Another update may have started before this goroutine resumed.
		}
	}
}

func SetUpdateHandler(ctx context.Context, name string, handler any) error {
	return SetUpdateHandlerWithOptions(ctx, name, handler, UpdateHandlerOptions{})
}

// Validators execute under a separate scratch owner. A validator may allocate
// temporary values, but cannot change captured workflow state or emit commands.
// Accepted handlers run as high-priority native workflow goroutines. Ordinary
// children they start keep normal priority.
func setUpdateHandlerWithOptions(ctx context.Context, name string, handler any, options UpdateHandlerOptions) error {
	assertWritable()
	if ctx == nil {
		return errors.New("workflow: nil context")
	}
	if strings.HasPrefix(name, "__") && !isWorkflowStreamName(name) {
		return errors.New("update names starting with '__' are reserved for internal use")
	}
	if err := validateUpdateFunctions(handler, options.Validator); err != nil {
		return err
	}
	updateHandlers[name] = updateHandler{handler: handler, validator: options.Validator, ctx: ctx}
	return registerUpdate(UpdateRegistration{Name: name, UnfinishedPolicy: options.UnfinishedPolicy, Description: options.Description})
}

func registerUpdate(registration UpdateRegistration) error {
	startReadOnlyService()
	if !updateServiceStarted {
		updateServiceStarted = true
		isolate.GoWithPriority(isolate.PriorityHigh, serveUpdates)
	}
	raw, err := MarshalTransport(registration)
	if err != nil {
		return err
	}
	_, err = isolate.Call(OpRegisterUpdate, raw)
	return err
}

func getCurrentUpdateInfo(ctx context.Context) *UpdateInfo {
	if ctx == nil {
		return nil
	}
	info, _ := ctx.Value(updateInfoKey{}).(*UpdateInfo)
	return info
}

func AllHandlersFinished(ctx context.Context) bool {
	if ctx != nil {
		if validating, _ := ctx.Value(updateValidationKey{}).(bool); validating {
			return false
		}
	}
	return runningUpdateCount.Load() == 0
}

func validateUpdateFunctions(handler, validator any) error {
	t := reflect.TypeOf(handler)
	if t == nil || t.Kind() != reflect.Func || reflect.ValueOf(handler).IsNil() {
		return errors.New("update handler must be a non-nil function")
	}
	if t.IsVariadic() {
		return errors.New("workflow: variadic update handlers are outside the isolate POC subset")
	}
	if t.NumIn() == 0 || t.In(0) != reflect.TypeFor[context.Context]() {
		return errors.New("first parameter of update handler must be context.Context")
	}
	if t.NumOut() < 1 || t.NumOut() > 2 || !t.Out(t.NumOut()-1).Implements(reflect.TypeFor[error]()) {
		return errors.New("update handler must return error or (result, error)")
	}
	if t.NumOut() == 2 {
		switch t.Out(0).Kind() {
		case reflect.Func, reflect.Chan, reflect.UnsafePointer:
			return errors.New("update handler result must be serializable")
		}
	}
	if validator == nil {
		return nil
	}
	v := reflect.TypeOf(validator)
	if v.Kind() != reflect.Func || reflect.ValueOf(validator).IsNil() || v.IsVariadic() || v.NumOut() != 1 || !v.Out(0).Implements(reflect.TypeFor[error]()) {
		return errors.New("update validator must be a non-nil function returning error")
	}
	offset := 0
	if v.NumIn() > 0 && v.In(0) == reflect.TypeFor[context.Context]() {
		offset = 1
	}
	if v.NumIn()-offset != t.NumIn()-1 {
		return errors.New("update handler and validator have different numbers of parameters")
	}
	for i := 1; i < t.NumIn(); i++ {
		if t.In(i) != v.In(i-1+offset) {
			return fmt.Errorf("update handler and validator differ at parameter %d", i-1)
		}
	}
	return nil
}

func updateArguments(fn any, ctx context.Context, raw *commonpb.Payloads) ([]reflect.Value, error) {
	t := reflect.TypeOf(fn)
	offset := 0
	if t.NumIn() > 0 && t.In(0) == reflect.TypeFor[context.Context]() {
		offset = 1
	}
	payloads, err := decodePayloads(raw, GetDataConverter(ctx))
	if err != nil {
		return nil, err
	}
	if err := checkArgumentCount(payloads, t.NumIn()-offset); err != nil {
		return nil, err
	}
	args := make([]reflect.Value, t.NumIn())
	if offset != 0 {
		args[0] = reflect.ValueOf(ctx)
	}
	pointers := make([]any, t.NumIn()-offset)
	for i := range pointers {
		p := reflect.New(t.In(i + offset))
		if isUnsupportedProtoValue(p.Interface()) {
			return nil, errors.New("workflow: legacy gogo protobuf messages are unsupported; use Google protobuf")
		}
		pointers[i], args[i+offset] = p.Interface(), p.Elem()
	}
	if err := GetDataConverter(ctx).FromPayloads(payloads, pointers...); err != nil {
		return nil, err
	}
	return args, nil
}

func validateUpdate(request QueryRequest) (failure *failurepb.Failure, err error) {
	// This encompasses argument callbacks, the validator, and failure conversion.
	// None may escape the read-only service and terminate the workflow.
	defer func() {
		if p := recover(); p != nil {
			failure = &failurepb.Failure{
				Message: fmt.Sprintf("update validator panic: %v", p), Source: "GoSDK",
				FailureInfo: &failurepb.Failure_ApplicationFailureInfo{ApplicationFailureInfo: &failurepb.ApplicationFailureInfo{Type: "PanicError"}},
			}
		}
	}()
	h := resolveUpdateHandler(request.Name)
	if h.handler == nil {
		return nil, errors.New("workflow: update handler not registered")
	}
	// Native context cancellation internals are mutable. Give validation its own
	// cancellation snapshot, retaining only read-only access to root values.
	base := h.ctx
	if base == nil {
		base = rootContext
	}
	ctx, cancel := context.WithCancel(context.WithoutCancel(base))
	defer cancel()
	if request.Canceled {
		cancel()
	}
	ctx = context.WithValue(ctx, tracecontext.ScopeKey{}, request.ObservationID)
	ctxWithInfo := context.WithValue(ctx, updateInfoKey{}, &UpdateInfo{ID: request.UpdateID, Name: request.Name})
	ctxWithInfo = context.WithValue(ctxWithInfo, updateValidationKey{}, true)
	ctxWithInfo, err = withIncomingHeader(ctxWithInfo, request.Header)
	if err != nil {
		return nil, err
	}
	args, cause := updateHandlerArguments(h, h.handler, ctxWithInfo, request.Name, request.Payloads)
	if cause == nil && instanceInterceptorFactory.Name() != "" {
		ctxWithInfo = context.WithValue(ctxWithInfo, updateSkipValidatorKey{}, request.SkipValidator)
		ctxWithInfo, chain, chainErr := scratchInterceptors(ctxWithInfo)
		if chainErr != nil {
			return nil, chainErr
		}
		input := &UpdateInput{Name: request.Name, Args: make([]any, len(args)-1)}
		for i, arg := range args[1:] {
			input.Args[i] = arg.Interface()
		}
		cause = chain.inbound.ValidateUpdate(ctxWithInfo, input)
	} else if cause == nil && !request.SkipValidator && h.validator != nil {
		var args []reflect.Value
		args, cause = updateHandlerArguments(h, h.validator, ctxWithInfo, request.Name, request.Payloads)
		if cause == nil {
			result := reflect.ValueOf(h.validator).Call(args)[0].Interface()
			if result != nil {
				cause = result.(error)
			}
		}
	}
	if cause == nil {
		return nil, nil
	}
	return encodeFailure(cause, GetDataConverter(ctxWithInfo))
}

func serveUpdates() {
	isolate.MarkServiceGoroutine()
	for {
		raw, err := isolate.Call(OpNextUpdate, nil)
		if err != nil {
			panic(err)
		}
		var request UpdateRequest
		if err := UnmarshalTransport(raw, &request); err != nil {
			panic(err)
		}
		updateWaitMu.Lock()
		if runningUpdateCount.Add(1) == 1 {
			updateFinished = make(chan struct{})
		}
		updateWaitMu.Unlock()
		isolate.GoWithPriority(isolate.PriorityHigh, func() { executeUpdate(request) })
	}
}

func executeUpdate(request UpdateRequest) {
	info := &UpdateInfo{ID: request.ID, Name: request.Name}
	h := resolveUpdateHandler(request.Name)
	base := h.ctx
	if base == nil {
		base = rootContext
	}
	ctx := context.WithValue(base, updateInfoKey{}, info)
	ctx, headerErr := withIncomingHeader(ctx, request.Header)
	if headerErr != nil {
		panic(headerErr)
	}
	args, cause := updateHandlerArguments(h, h.handler, ctx, request.Name, request.Payloads)
	var value any
	if cause == nil {
		// Handler panics follow the normal SDK Workflow Task failure policy.
		if activeInterceptors != nil {
			input := &UpdateInput{Name: request.Name, Args: make([]any, len(args)-1)}
			for i, arg := range args[1:] {
				input.Args[i] = arg.Interface()
			}
			value, cause = activeInterceptors.inbound.ExecuteUpdate(ctx, input)
		} else {
			results := reflect.ValueOf(h.handler).Call(args)
			if result := results[len(results)-1].Interface(); result != nil {
				cause = result.(error)
			}
			if len(results) == 2 {
				value = results[0].Interface()
			}
		}
	}
	completion := UpdateCompletion{ID: request.ID}
	if cause == nil {
		if isUnsupportedProtoValue(value) {
			cause = errors.New("workflow: legacy gogo protobuf messages are unsupported; use Google protobuf")
		} else {
			payloads, err := GetDataConverter(ctx).ToPayloads(value)
			cause = err
			if cause == nil {
				completion.Payloads = payloads
			}
		}
	}
	if cause != nil {
		var err error
		completion.Failure, err = encodeFailure(cause, GetDataConverter(ctx))
		if err != nil {
			panic(err)
		}
	}
	raw, err := MarshalTransport(completion)
	if err != nil {
		panic(err)
	}
	if _, err := isolate.Call(OpCompleteUpdate, raw); err != nil {
		panic(err)
	}
	// A root waiting on AllHandlersFinished must not finish before the SDK has
	// queued this update's completion message.
	updateWaitMu.Lock()
	if runningUpdateCount.Add(-1) == 0 {
		close(updateFinished)
	}
	updateWaitMu.Unlock()
}
