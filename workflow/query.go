package workflow

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"isolate"
	"reflect"
	"strings"

	"github.com/mfateev/sdk-go-poc/internal/payloadwire"
	goWorkflow "go.temporal.io/sdk/workflow"
)

type QueryHandlerOptions = goWorkflow.QueryHandlerOptions

type QueryRegistration struct {
	Name    string              `json:"name"`
	Options QueryHandlerOptions `json:"options"`
}
type QueryRequest struct {
	Kind          string `json:"kind,omitempty"`
	UpdateID      string `json:"update_id,omitempty"`
	Canceled      bool   `json:"canceled,omitempty"`
	SkipValidator bool   `json:"skip_validator,omitempty"`
	ID            uint64 `json:"id"`
	Name          string `json:"name"`
	Payloads      []byte `json:"payloads"`
}
type QueryResponse struct {
	Failure  []byte `json:"failure,omitempty"`
	ID       uint64 `json:"id"`
	Payloads []byte `json:"payloads,omitempty"`
	Error    string `json:"error,omitempty"`
	Failed   bool   `json:"failed,omitempty"`
}

var queryHandlers = make(map[string]any)
var queryServiceStarted bool

// SetQueryHandler retains the SDK signature, with a standard Go context for
// registration. Handlers take serialized arguments and return (result, error).
// They may read captured workflow state, including after workflow completion.
func SetQueryHandler(ctx context.Context, queryType string, handler any) error {
	return SetQueryHandlerWithOptions(ctx, queryType, handler, QueryHandlerOptions{})
}

func SetQueryHandlerWithOptions(ctx context.Context, queryType string, handler any, options QueryHandlerOptions) error {
	assertWritable()
	if ctx == nil {
		return errors.New("workflow: nil context")
	}
	if strings.HasPrefix(queryType, "__") {
		return errors.New("queryType starts with '__' is reserved for internal use")
	}
	if err := validateQueryHandler(handler); err != nil {
		return err
	}
	queryHandlers[queryType] = handler // The SDK permits replacing a handler.
	startReadOnlyService()
	raw, err := json.Marshal(QueryRegistration{Name: queryType, Options: options})
	if err != nil {
		return err
	}
	_, err = isolate.Call(OpRegisterQuery, raw)
	return err
}

func validateQueryHandler(handler any) error {
	t := reflect.TypeOf(handler)
	if t == nil || t.Kind() != reflect.Func || reflect.ValueOf(handler).IsNil() {
		return errors.New("handler must be a non-nil function")
	}
	if t.IsVariadic() {
		return errors.New("workflow: variadic query handlers are outside the isolate POC subset")
	}
	if t.NumOut() != 2 {
		return fmt.Errorf("handler must return 2 values (serializable result and error), but found %d return values", t.NumOut())
	}
	switch t.Out(0).Kind() {
	case reflect.Func, reflect.Chan, reflect.UnsafePointer:
		return fmt.Errorf("first return value of handler must be serializable but found: %v", t.Out(0).Kind())
	}
	if !t.Out(1).Implements(reflect.TypeFor[error]()) {
		return fmt.Errorf("second return value of handler must be error but found %v", t.Out(1).Kind())
	}
	return nil
}

func assertWritable() {
	if isolate.IsReadOnly() {
		panic("workflow: query handlers and update validators cannot mutate workflow state or schedule operations")
	}
}

// IsReadOnly has the SDK meaning for queries and update validators.
func IsReadOnly(ctx context.Context) bool { return isolate.IsReadOnly() }

func startReadOnlyService() {
	if !queryServiceStarted {
		queryServiceStarted = true
		go serveQueries()
	}
}

// This dedicated goroutine remains parked when the workflow completes. The
// host admits it separately; workflow goroutines never dispatch during queries.
func serveQueries() {
	var response []byte
	for {
		raw, err := isolate.ReadOnlyCall(OpQuery, response)
		if err != nil {
			panic(err)
		}
		var request QueryRequest
		if err := json.Unmarshal(raw, &request); err != nil {
			panic(err)
		}
		outcome := QueryResponse{ID: request.ID}
		if request.Kind == "validate" {
			outcome.Failure, err = validateUpdate(request)
		} else {
			outcome.Payloads, err = invokeQuery(queryHandlers[request.Name], request.Payloads)
		}
		if err != nil {
			outcome.Failed, outcome.Error = true, readOnlyErrorMessage(err)
		}
		response, err = json.Marshal(outcome)
		if err != nil {
			panic(err)
		}
	}
}

// Error formatting invokes application code too. Recover it separately so an
// Error method that writes captured state cannot escape the handler boundary.
func readOnlyErrorMessage(err error) (message string) {
	defer func() {
		if recover() != nil {
			message = "read-only handler error formatting panicked"
		}
	}()
	return err.Error()
}

func invokeQuery(handler any, raw []byte) (result []byte, err error) {
	defer func() {
		if p := recover(); p != nil {
			result = nil
			err = fmt.Errorf("query handler panic: %v", p)
		}
	}()
	if handler == nil {
		return nil, errors.New("workflow: query handler not registered")
	}
	t := reflect.TypeOf(handler)
	payloads, err := decodePayloads(raw)
	if err != nil {
		return nil, err
	}
	if err := checkArgumentCount(payloads, t.NumIn()); err != nil {
		return nil, err
	}
	args := make([]reflect.Value, t.NumIn())
	pointers := make([]any, len(args))
	for i := range args {
		p := reflect.New(t.In(i))
		if isProtoValue(p.Interface()) {
			return nil, errors.New("workflow: protobuf values are outside the isolate POC subset")
		}
		pointers[i], args[i] = p.Interface(), p.Elem()
	}
	if err := instanceDataConverter.FromPayloads(payloads, pointers...); err != nil {
		return nil, err
	}
	values := reflect.ValueOf(handler).Call(args)
	if cause := values[1].Interface(); cause != nil {
		return nil, cause.(error)
	}
	value := values[0].Interface()
	if isProtoValue(value) {
		return nil, errors.New("workflow: protobuf values are outside the isolate POC subset")
	}
	payloads, err = instanceDataConverter.ToPayloads(value)
	if err != nil {
		return nil, err
	}
	return payloadwire.Encode(payloads)
}
