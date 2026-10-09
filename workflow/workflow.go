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
	"github.com/mfateev/sdk-go-poc/internal/failurecodec"
	"github.com/mfateev/sdk-go-poc/internal/payloadwire"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/sdk/converter"
	"google.golang.org/protobuf/proto"
)

// Operation numbers are a wire contract. Never renumber an existing operation.
const (
	OpInput               uint32 = 1
	OpActivity            uint32 = 2
	OpSleep               uint32 = 3
	OpSignal              uint32 = 4
	OpComplete            uint32 = 5
	OpStart               uint32 = 6
	OpStartPayloads       uint32 = 7
	OpCompletePayloads    uint32 = 8
	OpActivityPayloads    uint32 = 9
	OpWorkflowCancel      uint32 = 10
	OpCancellableCall     uint32 = 11
	OpCancelCall          uint32 = 12
	OpScheduleActivity    uint32 = 13
	OpAwaitActivity       uint32 = 14
	OpCancelActivity      uint32 = 15
	OpGetVersion          uint32 = 16
	OpIsReplaying         uint32 = 17
	OpResolveWorkflowName uint32 = 18
	OpRegisterQuery       uint32 = 19
	OpQuery               uint32 = 20
	OpRegisterUpdate      uint32 = 21
	OpNextUpdate          uint32 = 22
	OpCompleteUpdate      uint32 = 23
	OpScheduleChild       uint32 = 24
	OpAwaitChild          uint32 = 25
	OpAwaitChildExecution uint32 = 26
	OpCancelChild         uint32 = 27
	OpSignalChild         uint32 = 28
	OpAwaitChildSignal    uint32 = 29
)

// Handler is a named workflow function. Each execution receives its own
// isolate instance, including a fresh copy of the registration table.
type Handler func(context.Context, []byte) ([]byte, error)

var handlers = make(map[string]Handler)

type typedHandler func(context.Context, *commonpb.Payloads) (*commonpb.Payloads, error)

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
func RegisterTyped0[R any](name string, handler func(context.Context) (R, error)) {
	if handler == nil {
		panic("workflow: nil typed handler")
	}
	registerTyped(name, func(ctx context.Context, payloads *commonpb.Payloads) (*commonpb.Payloads, error) {
		if err := checkArgumentCount(payloads, 0); err != nil {
			return nil, err
		}
		result, err := handler(ctx)
		return encodeTypedResult(result, err)
	})
}

// RegisterTyped registers a typed workflow with one argument.
func RegisterTyped[A, R any](name string, handler func(context.Context, A) (R, error)) {
	if handler == nil {
		panic("workflow: nil typed handler")
	}
	registerTyped(name, func(ctx context.Context, payloads *commonpb.Payloads) (*commonpb.Payloads, error) {
		if err := checkArgumentCount(payloads, 1); err != nil {
			return nil, err
		}
		var arg A
		if isProtoValue(any(arg)) || isProtoValue(any(&arg)) {
			return nil, errors.New("workflow: protobuf values are outside the isolate POC subset")
		}
		if err := currentDataConverter().FromPayloads(payloads, &arg); err != nil {
			return nil, fmt.Errorf("workflow: decode arguments: %w", err)
		}
		result, err := handler(ctx, arg)
		return encodeTypedResult(result, err)
	})
}

// RegisterTyped2 registers a typed workflow with two arguments.
func RegisterTyped2[A, B, R any](name string, handler func(context.Context, A, B) (R, error)) {
	if handler == nil {
		panic("workflow: nil typed handler")
	}
	registerTyped(name, func(ctx context.Context, payloads *commonpb.Payloads) (*commonpb.Payloads, error) {
		if err := checkArgumentCount(payloads, 2); err != nil {
			return nil, err
		}
		var first A
		var second B
		if isProtoValue(any(first)) || isProtoValue(any(&first)) ||
			isProtoValue(any(second)) || isProtoValue(any(&second)) {
			return nil, errors.New("workflow: protobuf values are outside the isolate POC subset")
		}
		if err := currentDataConverter().FromPayloads(payloads, &first, &second); err != nil {
			return nil, fmt.Errorf("workflow: decode arguments: %w", err)
		}
		result, err := handler(ctx, first, second)
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
	payloads, err := currentDataConverter().ToPayloads(result)
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
		if instanceConverterFactory.Name() != "" {
			continue
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
	ConverterConfig []byte     `json:"converter_config,omitempty"`
	TaskQueue       string     `json:"task_queue,omitempty"`
	Options         RunOptions `json:"options"`
	Canceled        bool       `json:"canceled,omitempty"`
	Name            string     `json:"name"`
	Payloads        []byte     `json:"payloads"`
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
	ctx, cancel := executionContext(start.Canceled, start.TaskQueue, start.Options)
	defer cancel()
	if handler, ok := typedHandlers[start.Name]; ok {
		payloads, err := decodePayloads(start.Payloads)
		if err != nil {
			return completePayloads(nil, fmt.Errorf("workflow: decode input payloads: %w", err))
		}
		result, err := handler(ctx, payloads)
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
	result, err := handler(ctx, input)
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
	if err := currentDataConverter().FromPayloads(&payloads, &value); err != nil {
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
	return RunFunctionWithDataConverter(handle, isolate.Handle{})
}

// RunFunctionWithDataConverter constructs the worker-configured serializer
// inside the workflow owner before decoding arguments or invoking user code.
func RunFunctionWithDataConverter(handle, factory isolate.Handle) error {
	startBytes, err := isolate.Call(OpStartPayloads, nil)
	if err != nil {
		return err
	}
	var start PayloadStart
	if err := json.Unmarshal(startBytes, &start); err != nil {
		return completePayloads(nil, fmt.Errorf("workflow: decode start: %w", err))
	}
	if err := configureDataConverter(factory, start.ConverterConfig); err != nil {
		return completePayloads(nil, fmt.Errorf("workflow: create data converter: %w", err))
	}
	payloads, err := decodePayloads(start.Payloads)
	if err != nil {
		return completePayloads(nil, fmt.Errorf("workflow: decode input payloads: %w", err))
	}
	ctx, cancel := executionContext(start.Canceled, start.TaskQueue, start.Options)
	defer cancel()
	var result *commonpb.Payloads
	err = handle.Invoke(func(args ...isolate.Value) error {
		if len(args) == 0 {
			return errors.New("workflow: isolate workflow must take context.Context first")
		}
		slot, ok := args[0].Pointer.(*context.Context)
		if !ok {
			return errors.New("workflow: isolate workflow must take context.Context first")
		}
		*slot = ctx
		args = args[1:]
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
		if err := currentDataConverter().FromPayloads(payloads, pointers...); err != nil {
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
		result, err = currentDataConverter().ToPayloads(value.Value)
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
	ID                  uint64           `json:"id,omitempty"`
	Options             *ActivityOptions `json:"options,omitempty"`
	Function            bool             `json:"function,omitempty"`
	Name                string           `json:"name"`
	Payloads            []byte           `json:"payloads"`
	StartToCloseTimeout time.Duration    `json:"start_to_close_timeout"`
}

// Signal is one incoming workflow signal.
type Signal struct {
	// Payloads is the plain serialized envelope; nextSignal decodes Input under
	// the receiving isolate owner, after host codecs have been removed.
	Payloads []byte `json:"payloads,omitempty"`
	Name     string `json:"name"`
	Input    []byte `json:"input"`
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
	Failure       []byte                `json:"failure,omitempty"`
	Failed        bool                  `json:"failed,omitempty"`
	ContinueAsNew *ContinueAsNewRequest `json:"continue_as_new,omitempty"`
	Canceled      bool                  `json:"canceled,omitempty"`
	Result        []byte                `json:"result"`
	Error         string                `json:"error,omitempty"`
}

// PayloadCompletion carries serialized Temporal Payloads produced by a typed
// handler, or its error. The host forwards these payloads without conversion.
type PayloadCompletion struct {
	Failure       []byte                `json:"failure,omitempty"`
	Failed        bool                  `json:"failed,omitempty"`
	ContinueAsNew *ContinueAsNewRequest `json:"continue_as_new,omitempty"`
	Canceled      bool                  `json:"canceled,omitempty"`
	Payloads      []byte                `json:"payloads"`
	Error         string                `json:"error,omitempty"`
}

// Input returns the workflow's first byte-slice argument.
func Input() ([]byte, error) { return isolate.Call(OpInput, nil) }

func encodeActivityArgs(args []any) ([]byte, error) {
	for _, arg := range args {
		if isProtoValue(arg) {
			return nil, errors.New("workflow: protobuf values are outside the isolate POC subset")
		}
	}
	payloads, err := currentDataConverter().ToPayloads(args...)
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
	if err := currentDataConverter().FromPayloads(payloads, &result); err != nil {
		var zero R
		return zero, fmt.Errorf("workflow: decode activity result: %w", err)
	}
	return result, nil
}

// Sleep waits on a durable Temporal timer. Do not use time.Sleep for this POC.
func Sleep(ctx context.Context, duration time.Duration) error {
	if duration < 0 {
		return errors.New("workflow: negative sleep duration")
	}
	payload, err := json.Marshal(duration)
	if err != nil {
		return err
	}
	_, err = call(ctx, OpSleep, payload)
	return err
}

// NextSignal waits for the next incoming signal, regardless of its name.
func NextSignal(ctx context.Context) (Signal, error) { return nextSignal(ctx, "") }

func nextSignal(ctx context.Context, name string) (Signal, error) {
	var signal Signal
	payload, err := call(ctx, OpSignal, []byte(name))
	if err != nil {
		return signal, err
	}
	err = json.Unmarshal(payload, &signal)
	if err == nil && len(signal.Payloads) != 0 {
		var values *commonpb.Payloads
		values, err = decodePayloads(signal.Payloads)
		if err == nil {
			err = checkArgumentCount(values, 1)
		}
		if err == nil {
			err = currentDataConverter().FromPayloads(values, &signal.Input)
		}
		signal.Payloads = nil
	}
	return signal, err
}

// GetSignalChannel receives signals with the given name. An empty name receives
// all signals. Each call starts an isolate-owned goroutine that waits through
// Call; its channel is buffered so a completed call can finish if the workflow
// has selected another case. A host error is sent once, then the channel closes.
func GetSignalChannel(ctx context.Context, name string) <-chan SignalResult {
	results := make(chan SignalResult, 1)
	if ctx == nil {
		results <- SignalResult{Err: errors.New("workflow: nil context")}
		close(results)
		return results
	}
	go func() {
		defer close(results)
		for {
			signal, err := nextSignal(ctx, name)
			select {
			case results <- SignalResult{Signal: signal, Err: err}:
			case <-ctx.Done():
				return
			}
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
		var err error
		completion.ContinueAsNew, err = continuationRequest(cause)
		if err != nil {
			return err
		}
		completion.Failed = true
		completion.Error = cause.Error()
		completion.Canceled = errors.Is(cause, context.Canceled)
		if completion.ContinueAsNew == nil {
			completion.Failure, err = failurecodec.Encode(cause, currentDataConverter())
			if err != nil {
				return err
			}
		}
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
		var err error
		completion.ContinueAsNew, err = continuationRequest(cause)
		if err != nil {
			return err
		}
		completion.Failed = true
		completion.Error = cause.Error()
		completion.Canceled = errors.Is(cause, context.Canceled)
		if completion.ContinueAsNew == nil {
			completion.Failure, err = failurecodec.Encode(cause, currentDataConverter())
			if err != nil {
				return err
			}
		}
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
