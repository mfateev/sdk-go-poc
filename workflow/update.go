package workflow

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"isolate"
	"reflect"
	"strings"
	"sync/atomic"

	"github.com/mfateev/sdk-go-poc/internal/failurecodec"
	"github.com/mfateev/sdk-go-poc/internal/failurewire"
	"github.com/mfateev/sdk-go-poc/internal/payloadwire"
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
	Name             string                  `json:"name"`
	UnfinishedPolicy HandlerUnfinishedPolicy `json:"unfinished_policy"`
	Description      string                  `json:"description,omitempty"`
}

type UpdateRequest struct {
	ID       string `json:"id"`
	Name     string `json:"name"`
	Payloads []byte `json:"payloads"`
}

type UpdateCompletion struct {
	ID       string `json:"id"`
	Payloads []byte `json:"payloads,omitempty"`
	Failure  []byte `json:"failure,omitempty"`
}

type updateHandler struct{ handler, validator any }
type updateInfoKey struct{}
type updateValidationKey struct{}

var updateHandlers = make(map[string]updateHandler)
var runningUpdateCount atomic.Int64
var updateServiceStarted bool

func SetUpdateHandler(ctx context.Context, name string, handler any) error {
	return SetUpdateHandlerWithOptions(ctx, name, handler, UpdateHandlerOptions{})
}

// Validators execute under a separate scratch owner. A validator may allocate
// temporary values, but cannot change captured workflow state or emit commands.
// Accepted handlers run as ordinary native workflow goroutines.
func SetUpdateHandlerWithOptions(ctx context.Context, name string, handler any, options UpdateHandlerOptions) error {
	assertWritable()
	if ctx == nil {
		return errors.New("workflow: nil context")
	}
	if strings.HasPrefix(name, "__") {
		return errors.New("update names starting with '__' are reserved for internal use")
	}
	if err := validateUpdateFunctions(handler, options.Validator); err != nil {
		return err
	}
	updateHandlers[name] = updateHandler{handler, options.Validator}
	startReadOnlyService()
	if !updateServiceStarted {
		updateServiceStarted = true
		go serveUpdates()
	}
	raw, err := json.Marshal(UpdateRegistration{Name: name, UnfinishedPolicy: options.UnfinishedPolicy, Description: options.Description})
	if err != nil {
		return err
	}
	_, err = isolate.Call(OpRegisterUpdate, raw)
	return err
}

func GetCurrentUpdateInfo(ctx context.Context) *UpdateInfo {
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

func updateArguments(fn any, ctx context.Context, raw []byte) ([]reflect.Value, error) {
	t := reflect.TypeOf(fn)
	offset := 0
	if t.NumIn() > 0 && t.In(0) == reflect.TypeFor[context.Context]() {
		offset = 1
	}
	payloads, err := decodePayloads(raw)
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
		if isProtoValue(p.Interface()) {
			return nil, errors.New("workflow: protobuf values are outside the isolate POC subset")
		}
		pointers[i], args[i+offset] = p.Interface(), p.Elem()
	}
	if err := currentDataConverter().FromPayloads(payloads, pointers...); err != nil {
		return nil, err
	}
	return args, nil
}

func validateUpdate(request QueryRequest) (failure []byte, err error) {
	// This encompasses argument callbacks, the validator, and failure conversion.
	// None may escape the read-only service and terminate the workflow.
	defer func() {
		if p := recover(); p != nil {
			failure, err = failurewire.Encode(&failurepb.Failure{
				Message: fmt.Sprintf("update validator panic: %v", p), Source: "GoSDK",
				FailureInfo: &failurepb.Failure_ApplicationFailureInfo{ApplicationFailureInfo: &failurepb.ApplicationFailureInfo{Type: "PanicError"}},
			})
		}
	}()
	h := updateHandlers[request.Name]
	if h.handler == nil {
		return nil, errors.New("workflow: update handler not registered")
	}
	// Native context cancellation internals are mutable. Give validation its own
	// cancellation snapshot, retaining only read-only access to root values.
	ctx, cancel := context.WithCancel(context.WithoutCancel(rootContext))
	defer cancel()
	if request.Canceled {
		cancel()
	}
	ctxWithInfo := context.WithValue(ctx, updateInfoKey{}, &UpdateInfo{ID: request.UpdateID, Name: request.Name})
	ctxWithInfo = context.WithValue(ctxWithInfo, updateValidationKey{}, true)
	_, cause := updateArguments(h.handler, ctxWithInfo, request.Payloads)
	if cause == nil && !request.SkipValidator && h.validator != nil {
		var args []reflect.Value
		args, cause = updateArguments(h.validator, ctxWithInfo, request.Payloads)
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
	return failurecodec.Encode(cause, currentDataConverter())
}

func serveUpdates() {
	for {
		raw, err := isolate.Call(OpNextUpdate, nil)
		if err != nil {
			panic(err)
		}
		var request UpdateRequest
		if err := json.Unmarshal(raw, &request); err != nil {
			panic(err)
		}
		runningUpdateCount.Add(1)
		go executeUpdate(request)
	}
}

func executeUpdate(request UpdateRequest) {
	info := &UpdateInfo{ID: request.ID, Name: request.Name}
	ctx := context.WithValue(rootContext, updateInfoKey{}, info)
	h := updateHandlers[request.Name]
	args, cause := updateArguments(h.handler, ctx, request.Payloads)
	var value any
	if cause == nil {
		// Handler panics follow the normal SDK Workflow Task failure policy.
		results := reflect.ValueOf(h.handler).Call(args)
		if result := results[len(results)-1].Interface(); result != nil {
			cause = result.(error)
		}
		if len(results) == 2 {
			value = results[0].Interface()
		}
	}
	completion := UpdateCompletion{ID: request.ID}
	if cause == nil {
		if isProtoValue(value) {
			cause = errors.New("workflow: protobuf values are outside the isolate POC subset")
		} else {
			payloads, err := currentDataConverter().ToPayloads(value)
			cause = err
			if cause == nil {
				completion.Payloads, cause = payloadwire.Encode(payloads)
			}
		}
	}
	if cause != nil {
		var err error
		completion.Failure, err = failurecodec.Encode(cause, currentDataConverter())
		if err != nil {
			panic(err)
		}
	}
	raw, err := json.Marshal(completion)
	if err != nil {
		panic(err)
	}
	if _, err := isolate.Call(OpCompleteUpdate, raw); err != nil {
		panic(err)
	}
	// A root waiting on AllHandlersFinished must not finish before the SDK has
	// queued this update's completion message.
	runningUpdateCount.Add(-1)
}
