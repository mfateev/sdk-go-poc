// Package workflow exposes host-mediated Temporal operations to a statically
// linked isolate program. Host operations use a copied-byte boundary; typed
// workflow arguments and results use Temporal's default data converter here.
// The POC currently accepts nil, byte, and ordinary JSON payload encodings.
package workflow

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"isolate"
	"reflect"
	"time"

	gogoproto "github.com/gogo/protobuf/proto"
	"github.com/mfateev/sdk-go-poc/internal/activityref"
	"github.com/mfateev/sdk-go-poc/internal/payloadwire"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/sdk/converter"
	"google.golang.org/protobuf/proto"
)

// Operation numbers are a wire contract. Never renumber an existing operation.
const (
	OpInput            uint32 = 1
	OpActivity         uint32 = 2
	OpSleep            uint32 = 3
	OpSignal           uint32 = 4
	OpComplete         uint32 = 5
	OpStart            uint32 = 6
	OpStartPayloads    uint32 = 7
	OpCompletePayloads uint32 = 8
	OpActivityPayloads uint32 = 9
)

// Handler is a named workflow function. Each execution receives its own
// isolate instance, including a fresh copy of the registration table.
type Handler func([]byte) ([]byte, error)

var handlers = make(map[string]Handler)

type typedHandler func(*commonpb.Payloads) (*commonpb.Payloads, error)

var typedHandlers = make(map[string]typedHandler)

// Register associates a workflow entry name with a function. Call it from the
// isolate program's init function. Registration must not perform host work.
func Register(name string, handler Handler) {
	if name == "" || handler == nil {
		panic("workflow: registration requires a name and handler")
	}
	if _, exists := handlers[name]; exists {
		panic("workflow: duplicate registration: " + name)
	}
	if _, exists := typedHandlers[name]; exists {
		panic("workflow: duplicate registration: " + name)
	}
	handlers[name] = handler
}

func registerTyped(name string, handler typedHandler) {
	if name == "" || handler == nil {
		panic("workflow: typed registration requires a name and function")
	}
	if _, exists := handlers[name]; exists {
		panic("workflow: duplicate registration: " + name)
	}
	if _, exists := typedHandlers[name]; exists {
		panic("workflow: duplicate registration: " + name)
	}
	typedHandlers[name] = handler
}

// RegisterTyped0 registers a typed workflow without arguments. The result is
// converted inside the isolate using Temporal's default converter.
func RegisterTyped0[R any](name string, handler func() (R, error)) {
	if handler == nil {
		panic("workflow: nil typed handler")
	}
	registerTyped(name, func(payloads *commonpb.Payloads) (*commonpb.Payloads, error) {
		if err := checkArgumentCount(payloads, 0); err != nil {
			return nil, err
		}
		result, err := handler()
		return encodeTypedResult(result, err)
	})
}

// RegisterTyped registers a typed workflow with one argument.
func RegisterTyped[A, R any](name string, handler func(A) (R, error)) {
	if handler == nil {
		panic("workflow: nil typed handler")
	}
	registerTyped(name, func(payloads *commonpb.Payloads) (*commonpb.Payloads, error) {
		if err := checkArgumentCount(payloads, 1); err != nil {
			return nil, err
		}
		var arg A
		if isProtoValue(any(arg)) || isProtoValue(any(&arg)) {
			return nil, errors.New("workflow: protobuf values are outside the isolate POC subset")
		}
		if err := converter.GetDefaultDataConverter().FromPayloads(payloads, &arg); err != nil {
			return nil, fmt.Errorf("workflow: decode arguments: %w", err)
		}
		result, err := handler(arg)
		return encodeTypedResult(result, err)
	})
}

// RegisterTyped2 registers a typed workflow with two arguments.
func RegisterTyped2[A, B, R any](name string, handler func(A, B) (R, error)) {
	if handler == nil {
		panic("workflow: nil typed handler")
	}
	registerTyped(name, func(payloads *commonpb.Payloads) (*commonpb.Payloads, error) {
		if err := checkArgumentCount(payloads, 2); err != nil {
			return nil, err
		}
		var first A
		var second B
		if isProtoValue(any(first)) || isProtoValue(any(&first)) ||
			isProtoValue(any(second)) || isProtoValue(any(&second)) {
			return nil, errors.New("workflow: protobuf values are outside the isolate POC subset")
		}
		if err := converter.GetDefaultDataConverter().FromPayloads(payloads, &first, &second); err != nil {
			return nil, fmt.Errorf("workflow: decode arguments: %w", err)
		}
		result, err := handler(first, second)
		return encodeTypedResult(result, err)
	})
}

func checkArgumentCount(payloads *commonpb.Payloads, want int) error {
	if got := len(payloads.GetPayloads()); got != want {
		return fmt.Errorf("workflow: got %d arguments, want %d", got, want)
	}
	return nil
}

func encodeTypedResult[R any](result R, cause error) (*commonpb.Payloads, error) {
	if cause != nil {
		return nil, cause
	}
	// Protobuf's lazy descriptor caches still cross isolate owners in this POC.
	// Check both R and *R because generated messages commonly implement their
	// message interface only on a pointer receiver.
	if isProtoValue(any(result)) || isProtoValue(any(&result)) {
		return nil, errors.New("workflow: protobuf values are outside the isolate POC subset")
	}
	payloads, err := converter.GetDefaultDataConverter().ToPayloads(result)
	if err != nil {
		return nil, fmt.Errorf("workflow: encode result: %w", err)
	}
	return payloads, nil
}

func isProtoValue(value any) bool {
	if _, ok := value.(proto.Message); ok {
		return true
	}
	_, ok := value.(gogoproto.Message)
	if ok {
		return true
	}
	if value == nil {
		return false
	}
	typ := reflect.TypeOf(value)
	// Pointer values already had their full method set checked above.
	// Constructing pointers to them would create unnecessary **T metadata.
	if typ.Kind() == reflect.Pointer {
		return false
	}
	pointer := reflect.PointerTo(typ)
	return pointer.Implements(reflect.TypeFor[proto.Message]()) || pointer.Implements(reflect.TypeFor[gogoproto.Message]())
}

func checkPOCEncodings(payloads *commonpb.Payloads) error {
	for i, payload := range payloads.GetPayloads() {
		if payload == nil {
			return fmt.Errorf("workflow: nil payload at argument %d", i)
		}
		encoding := string(payload.GetMetadata()[converter.MetadataEncoding])
		switch encoding {
		case converter.MetadataEncodingNil, converter.MetadataEncodingBinary, converter.MetadataEncodingJSON:
		default:
			return fmt.Errorf("workflow: payload encoding %q is outside the isolate POC subset", encoding)
		}
	}
	return nil
}

// Start is the host-provided entry name and input for one execution.
type Start struct {
	Name  string `json:"name"`
	Input []byte `json:"input"`
}

// PayloadStart carries the entry name and serialized Temporal Payloads. The
// bytes cross the isolate boundary; Go argument values do not.
type PayloadStart struct {
	Name     string `json:"name"`
	Payloads []byte `json:"payloads"`
}

// Run selects the registered function, calls it, and reports its result to the
// host. The isolate program's main function can simply call Run.
func Run() error {
	payload, err := isolate.Call(OpStartPayloads, nil)
	if err != nil {
		return err
	}
	var start PayloadStart
	if err := json.Unmarshal(payload, &start); err != nil {
		return Complete(nil, fmt.Errorf("workflow: decode start: %w", err))
	}
	if handler, ok := typedHandlers[start.Name]; ok {
		payloads, err := decodePayloads(start.Payloads)
		if err != nil {
			return completePayloads(nil, fmt.Errorf("workflow: decode input payloads: %w", err))
		}
		result, err := handler(payloads)
		return completePayloads(result, err)
	}
	handler, ok := handlers[start.Name]
	if !ok {
		return Complete(nil, fmt.Errorf("workflow: unknown entry %q", start.Name))
	}
	input, err := decodeBytes(start.Payloads)
	if err != nil {
		return Complete(nil, fmt.Errorf("workflow: decode input: %w", err))
	}
	result, err := handler(input)
	return Complete(result, err)
}

func decodeBytes(data []byte) ([]byte, error) {
	if len(data) == 0 {
		return nil, nil
	}
	var payloads commonpb.Payloads
	decoded, err := payloadwire.Decode(data)
	if err != nil {
		return nil, err
	}
	payloads = *decoded
	if len(payloads.Payloads) != 1 {
		return nil, fmt.Errorf("got %d arguments, want 1", len(payloads.Payloads))
	}
	if err := checkPOCEncodings(&payloads); err != nil {
		return nil, err
	}
	var value []byte
	if err := converter.GetDefaultDataConverter().FromPayloads(&payloads, &value); err != nil {
		return nil, err
	}
	return value, nil
}

func decodePayloads(data []byte) (*commonpb.Payloads, error) {
	if len(data) == 0 {
		return new(commonpb.Payloads), nil
	}
	payloads, err := payloadwire.Decode(data)
	if err != nil {
		return nil, err
	}
	if err := checkPOCEncodings(payloads); err != nil {
		return nil, err
	}
	return payloads, nil
}

// RunFunction is the dispatcher used by the worker for compiler-discovered
// //go:isolate functions. Typed invocation and default-converter operations
// execute here, behind the isolate's copied-byte boundary.
func RunFunction(handle isolate.Handle) error {
	startBytes, err := isolate.Call(OpStartPayloads, nil)
	if err != nil {
		return err
	}
	var start PayloadStart
	if err := json.Unmarshal(startBytes, &start); err != nil {
		return completePayloads(nil, fmt.Errorf("workflow: decode start: %w", err))
	}
	payloads, err := decodePayloads(start.Payloads)
	if err != nil {
		return completePayloads(nil, fmt.Errorf("workflow: decode input payloads: %w", err))
	}
	var result *commonpb.Payloads
	err = handle.Invoke(func(args ...isolate.Value) error {
		if err := checkArgumentCount(payloads, len(args)); err != nil {
			return err
		}
		pointers := make([]any, len(args))
		for i, arg := range args {
			if isProtoValue(arg.Value) || isProtoValue(arg.Pointer) {
				return errors.New("workflow: protobuf values are outside the isolate POC subset")
			}
			pointers[i] = arg.Pointer
		}
		if err := converter.GetDefaultDataConverter().FromPayloads(payloads, pointers...); err != nil {
			return fmt.Errorf("workflow: decode arguments: %w", err)
		}
		return nil
	}, func(values ...isolate.Value) error {
		if len(values) == 0 {
			return nil
		}
		value := values[0]
		if isProtoValue(value.Value) || isProtoValue(value.Pointer) {
			return errors.New("workflow: protobuf values are outside the isolate POC subset")
		}
		var err error
		result, err = converter.GetDefaultDataConverter().ToPayloads(value.Value)
		if err != nil {
			return fmt.Errorf("workflow: encode result: %w", err)
		}
		return nil
	})
	return completePayloads(result, err)
}

// ActivityRequest is the wire representation of a host activity call.
type ActivityRequest struct {
	Name                string        `json:"name"`
	Input               []byte        `json:"input"`
	StartToCloseTimeout time.Duration `json:"start_to_close_timeout"`
}

// ActivityPayloadRequest carries arguments encoded inside the isolate using
// Temporal's default converter. The host forwards them to the activity.
type ActivityPayloadRequest struct {
	Function            bool          `json:"function,omitempty"`
	Name                string        `json:"name"`
	Payloads            []byte        `json:"payloads"`
	StartToCloseTimeout time.Duration `json:"start_to_close_timeout"`
}

// Signal is one incoming workflow signal.
type Signal struct {
	Name  string `json:"name"`
	Input []byte `json:"input"`
}

// SignalResult carries one signal or an error from its host call.
type SignalResult struct {
	Signal Signal
	Err    error
}

// ActivityResult carries the completed activity result or its error.
type ActivityResult[R any] struct {
	Result R
	Err    error
}

// Completion is the wire representation of a workflow result.
type Completion struct {
	Result []byte `json:"result"`
	Error  string `json:"error,omitempty"`
}

// PayloadCompletion carries serialized Temporal Payloads produced by a typed
// handler, or its error. The host forwards these payloads without conversion.
type PayloadCompletion struct {
	Payloads []byte `json:"payloads"`
	Error    string `json:"error,omitempty"`
}

// Input returns the workflow's first byte-slice argument.
func Input() ([]byte, error) { return isolate.Call(OpInput, nil) }

// ExecuteActivity infers the input and result types from a one-argument activity.
// The function identifies host code; it is never invoked inside the isolate.
func ExecuteActivity[I, R any](activity func(I) (R, error), timeout time.Duration, input I) (R, error) {
	return executeActivityFunction[R](activity, timeout, input)
}

// ExecuteActivityWithContext identifies an activity whose first parameter is a
// host context. The worker supplies that context; only input crosses Call.
func ExecuteActivityWithContext[I, R any](activity func(context.Context, I) (R, error), timeout time.Duration, input I) (R, error) {
	return executeActivityFunction[R](activity, timeout, input)
}

func executeActivityFunction[R any](activity any, timeout time.Duration, input any) (R, error) {
	name, err := activityref.Name(activity)
	if err != nil {
		var zero R
		return zero, err
	}
	return executeActivity[R](name, true, timeout, input)
}

// ExecuteActivityAsync infers types and delivers one result, then closes.
func ExecuteActivityAsync[I, R any](activity func(I) (R, error), timeout time.Duration, input I) <-chan ActivityResult[R] {
	return activityAsync(func() (R, error) { return ExecuteActivity(activity, timeout, input) })
}

// ExecuteActivityAsyncWithContext is the async form for host-context activities.
func ExecuteActivityAsyncWithContext[I, R any](activity func(context.Context, I) (R, error), timeout time.Duration, input I) <-chan ActivityResult[R] {
	return activityAsync(func() (R, error) { return ExecuteActivityWithContext(activity, timeout, input) })
}

func activityAsync[R any](call func() (R, error)) <-chan ActivityResult[R] {
	results := make(chan ActivityResult[R], 1)
	go func() {
		defer close(results)
		result, err := call()
		results <- ActivityResult[R]{Result: result, Err: err}
	}()
	return results
}

// ExecuteActivityByName schedules a host activity with zero or more typed arguments
// and waits for its typed result. R must be explicit because Go cannot infer
// type parameters from return values. Use struct{} for error-only activities.
// Arguments/results use Temporal's default converter inside the isolate.
func ExecuteActivityByName[R any](name string, timeout time.Duration, args ...any) (R, error) {
	return executeActivity[R](name, false, timeout, args...)
}

func executeActivity[R any](name string, function bool, timeout time.Duration, args ...any) (R, error) {
	var zero R
	if name == "" || timeout <= 0 {
		return zero, errors.New("workflow: activity name and timeout are required")
	}
	if isProtoValue(zero) || isProtoValue(&zero) {
		return zero, errors.New("workflow: protobuf values are outside the isolate POC subset")
	}
	payloads, err := encodeActivityArgs(args)
	if err != nil {
		return zero, err
	}
	request, err := json.Marshal(ActivityPayloadRequest{Name: name, Function: function, Payloads: payloads, StartToCloseTimeout: timeout})
	if err != nil {
		return zero, err
	}
	response, err := isolate.Call(OpActivityPayloads, request)
	if err != nil {
		return zero, err
	}
	return decodeActivityResult[R](response)
}

func encodeActivityArgs(args []any) ([]byte, error) {
	for _, arg := range args {
		if isProtoValue(arg) {
			return nil, errors.New("workflow: protobuf values are outside the isolate POC subset")
		}
	}
	payloads, err := converter.GetDefaultDataConverter().ToPayloads(args...)
	if err != nil {
		return nil, fmt.Errorf("workflow: encode activity arguments: %w", err)
	}
	if err := checkPOCEncodings(payloads); err != nil {
		return nil, err
	}
	return payloadwire.Encode(payloads)
}

func decodeActivityResult[R any](response []byte) (R, error) {
	var result R
	if isProtoValue(result) || isProtoValue(&result) {
		return result, errors.New("workflow: protobuf values are outside the isolate POC subset")
	}
	payloads, err := decodePayloads(response)
	if err != nil {
		return result, fmt.Errorf("workflow: decode activity result: %w", err)
	}
	if len(payloads.Payloads) == 0 {
		return result, nil
	} // Error-only activity.
	if len(payloads.Payloads) != 1 {
		return result, fmt.Errorf("workflow: activity returned %d payloads, want 1", len(payloads.Payloads))
	}
	if err := converter.GetDefaultDataConverter().FromPayloads(payloads, &result); err != nil {
		var zero R
		return zero, fmt.Errorf("workflow: decode activity result: %w", err)
	}
	return result, nil
}

// ExecuteActivityAsyncByName starts a typed activity call in an isolate-owned
// goroutine. The buffered channel receives one result and then closes.
func ExecuteActivityAsyncByName[R any](name string, timeout time.Duration, args ...any) <-chan ActivityResult[R] {
	return activityAsync(func() (R, error) { return ExecuteActivityByName[R](name, timeout, args...) })
}

// Sleep waits on a durable Temporal timer. Do not use time.Sleep for this POC.
func Sleep(duration time.Duration) error {
	if duration < 0 {
		return errors.New("workflow: negative sleep duration")
	}
	payload, err := json.Marshal(duration)
	if err != nil {
		return err
	}
	_, err = isolate.Call(OpSleep, payload)
	return err
}

// NextSignal waits for the next incoming signal, regardless of its name.
func NextSignal() (Signal, error) { return nextSignal("") }

func nextSignal(name string) (Signal, error) {
	var signal Signal
	payload, err := isolate.Call(OpSignal, []byte(name))
	if err != nil {
		return signal, err
	}
	err = json.Unmarshal(payload, &signal)
	return signal, err
}

// GetSignalChannel receives signals with the given name. An empty name receives
// all signals. Each call starts an isolate-owned goroutine that waits through
// Call; its channel is buffered so a completed call can finish if the workflow
// has selected another case. A host error is sent once, then the channel closes.
func GetSignalChannel(name string) <-chan SignalResult {
	results := make(chan SignalResult, 1)
	go func() {
		defer close(results)
		for {
			signal, err := nextSignal(name)
			results <- SignalResult{Signal: signal, Err: err}
			if err != nil {
				return
			}
		}
	}()
	return results
}

// Complete finishes the workflow. The program should return from main next.
func Complete(result []byte, cause error) error {
	completion := Completion{Result: result}
	if cause != nil {
		completion.Error = cause.Error()
	}
	payload, err := json.Marshal(completion)
	if err != nil {
		return err
	}
	_, err = isolate.Call(OpComplete, payload)
	return err
}

func completePayloads(result *commonpb.Payloads, cause error) error {
	completion := PayloadCompletion{}
	if cause != nil {
		completion.Error = cause.Error()
	} else if result != nil {
		var err error
		completion.Payloads, err = payloadwire.Encode(result)
		if err != nil {
			return err
		}
	}
	payload, err := json.Marshal(completion)
	if err != nil {
		return err
	}
	_, err = isolate.Call(OpCompletePayloads, payload)
	return err
}
