// Package workflow exposes Temporal operations to statically compiled isolates
// using native contexts, goroutines and channels. Host operations use copied
// bytes; an isolate-owned data converter handles typed arguments and results.
// The default serializer supports nil, bytes, JSON and generated Google protobuf; workers may configure a
// marked serializer factory. Transport codecs remain on the host.
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
	"github.com/mfateev/sdk-go-poc/internal/integrationcontract"
	"github.com/mfateev/sdk-go-poc/internal/payloadwire"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/sdk/converter"
	"google.golang.org/protobuf/proto"
)

// Operation numbers are a wire contract. Never renumber an existing operation.
const (
	OpInput                    uint32 = 1
	OpActivity                 uint32 = 2
	OpSleep                    uint32 = 3
	OpSignal                   uint32 = 4
	OpComplete                 uint32 = 5
	OpStart                    uint32 = 6
	OpStartPayloads            uint32 = 7
	OpCompletePayloads         uint32 = 8
	OpActivityPayloads         uint32 = 9
	OpWorkflowCancel           uint32 = 10
	OpCancellableCall          uint32 = 11
	OpCancelCall               uint32 = 12
	OpScheduleActivity         uint32 = 13
	OpAwaitActivity            uint32 = 14
	OpCancelActivity           uint32 = 15
	OpGetVersion               uint32 = 16
	OpIsReplaying              uint32 = 17
	OpResolveWorkflowName      uint32 = 18
	OpRegisterQuery            uint32 = 19
	OpQuery                    uint32 = 20
	OpRegisterUpdate           uint32 = 21
	OpNextUpdate               uint32 = 22
	OpCompleteUpdate           uint32 = 23
	OpScheduleChild            uint32 = 24
	OpAwaitChild               uint32 = 25
	OpAwaitChildExecution      uint32 = 26
	OpCancelChild              uint32 = 27
	OpSignalChild              uint32 = 28
	OpAwaitChildSignal         uint32 = 29
	OpScheduleExternal         uint32 = 30
	OpAwaitExternal            uint32 = 31
	OpScheduleLocal            uint32 = 32
	OpAwaitLocal               uint32 = 33
	OpCancelLocal              uint32 = 34
	OpScheduleNexus            uint32 = 35
	OpAwaitNexus               uint32 = 36
	OpAwaitNexusExecution      uint32 = 37
	OpCancelNexus              uint32 = 38
	OpSessionID                uint32 = 39
	OpAddSession               uint32 = 40
	OpRemoveSession            uint32 = 41
	OpInfo                     uint32 = 42
	OpResolveActivityName      uint32 = 43
	OpInterceptSignal          uint32 = 44
	OpFlushSignals             uint32 = 45
	OpGetTypedSearchAttributes uint32 = 46
	OpUpsertSearchAttributes   uint32 = 47
	OpUpsertMemo               uint32 = 48
	OpMemoEncodingPolicy       uint32 = 49
	OpLastCompletionResult     uint32 = 50
	OpLastError                uint32 = 51
	OpRegisterSignal           uint32 = 52
	OpHasLastCompletionResult  uint32 = 53
	OpFlushSignalChannels      uint32 = 54
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
		if isUnsupportedProtoValue(any(arg)) || isUnsupportedProtoValue(any(&arg)) {
			return nil, errors.New("workflow: only generated Google protobuf messages without extensions are supported")
		}
		if err := GetDataConverter(ctx).FromPayloads(payloads, &arg); err != nil {
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
		if isUnsupportedProtoValue(any(first)) || isUnsupportedProtoValue(any(&first)) ||
			isUnsupportedProtoValue(any(second)) || isUnsupportedProtoValue(any(&second)) {
			return nil, errors.New("workflow: only generated Google protobuf messages without extensions are supported")
		}
		if err := GetDataConverter(ctx).FromPayloads(payloads, &first, &second); err != nil {
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
	// Check the concrete message provenance, including value results.
	if isUnsupportedProtoValue(any(result)) || isUnsupportedProtoValue(any(&result)) {
		return nil, errors.New("workflow: only generated Google protobuf messages without extensions are supported")
	}
	payloads, err := rootDataConverter().ToPayloads(result)
	if err != nil {
		return nil, fmt.Errorf("workflow: encode result: %w", err)
	}
	return payloads, nil
}

// Reject alternate/dynamic implementations before entering generated codecs.
// The runtime records type provenance only after the audited process builder.
func isUnsupportedProtoValue(value any) bool {
	if value == nil {
		return false
	}
	typ := reflect.TypeOf(value)
	for typ.Kind() == reflect.Pointer {
		typ = typ.Elem()
	}
	if typ.Kind() != reflect.Struct {
		return false
	}
	pointer := reflect.PointerTo(typ)
	if pointer.Implements(reflect.TypeFor[proto.Message]()) {
		return !isolate.IsGeneratedProtobuf(value)
	}
	return pointer.Implements(reflect.TypeFor[gogoproto.Message]())
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
		case converter.MetadataEncodingNil, converter.MetadataEncodingBinary, converter.MetadataEncodingJSON,
			converter.MetadataEncodingProtoJSON, converter.MetadataEncodingProto:
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
	RandomRunID             string            `json:"random_run_id,omitempty"`
	Dynamic                 bool              `json:"dynamic,omitempty"`
	FailureConverterFactory string            `json:"failure_converter_factory,omitempty"`
	FailureConverterConfig  []byte            `json:"failure_converter_config,omitempty"`
	PropagatorFactory       string            `json:"propagator_factory,omitempty"`
	PropagatorConfig        []byte            `json:"propagator_config,omitempty"`
	ConverterFactory        string            `json:"converter_factory,omitempty"`
	InterceptorFactory      string            `json:"interceptor_factory,omitempty"`
	InterceptorConfig       []byte            `json:"interceptor_config,omitempty"`
	Header                  map[string][]byte `json:"header,omitempty"`
	ConverterConfig         []byte            `json:"converter_config,omitempty"`
	TaskQueue               string            `json:"task_queue,omitempty"`
	Options                 RunOptions        `json:"options"`
	Canceled                bool              `json:"canceled,omitempty"`
	Name                    string            `json:"name"`
	Payloads                []byte            `json:"payloads"`
}

// Run selects the registered function, calls it, and reports its result to the
// host. The isolate program's main function can simply call Run.
func Run() error {
	payload, err := isolate.Call(OpStartPayloads, integrationcontract.Pack(nil))
	if err != nil {
		return err
	}
	payload, err = integrationcontract.Unpack(payload)
	if err != nil {
		panic(err) // A contract mismatch fails the Workflow Task, not the execution.
	}
	var start PayloadStart
	if err := json.Unmarshal(payload, &start); err != nil {
		return Complete(nil, fmt.Errorf("workflow: decode start: %w", err))
	}
	randomStreamRunID = start.RandomRunID
	ctx, cancel := executionContext(start.Canceled, start.TaskQueue, start.Options)
	defer cancel()
	ctx, err = withIncomingHeader(ctx, start.Header)
	if err != nil {
		return completePayloads(nil, err)
	}
	rootContext = ctx
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
	if err := rootDataConverter().FromPayloads(&payloads, &value); err != nil {
		return nil, err
	}
	return value, nil
}

func decodePayloads(data []byte, converters ...converter.DataConverter) (*commonpb.Payloads, error) {
	if len(data) == 0 {
		return new(commonpb.Payloads), nil
	}
	payloads, err := payloadwire.Decode(data)
	if err != nil {
		return nil, err
	}
	if len(converters) == 0 {
		if err := checkPOCEncodings(payloads); err != nil {
			return nil, err
		}
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
	return runFunction(handle, factory, nil)
}

// RunFunctionWithSupports composes compiler-created factories, selected by the
// copied startup envelope. It composes converters, propagators and interceptors.
func RunFunctionWithSupports(handle isolate.Handle, supports []isolate.Handle) error {
	return runFunction(handle, isolate.Handle{}, supports)
}

func runFunction(handle, factory isolate.Handle, supports []isolate.Handle) error {
	startBytes, err := isolate.Call(OpStartPayloads, integrationcontract.Pack(nil))
	if err != nil {
		return err
	}
	startBytes, err = integrationcontract.Unpack(startBytes)
	if err != nil {
		panic(err)
	}
	var start PayloadStart
	if err := json.Unmarshal(startBytes, &start); err != nil {
		return completePayloads(nil, fmt.Errorf("workflow: decode start: %w", err))
	}
	randomStreamRunID = start.RandomRunID
	var interceptorFactory, propagatorFactory, failureFactory isolate.Handle
	for _, support := range supports {
		if support.Name() == start.FailureConverterFactory {
			failureFactory = support
		}
		if support.Name() == start.ConverterFactory {
			factory = support
		}
		if support.Name() == start.PropagatorFactory {
			propagatorFactory = support
		}
		if support.Name() == start.InterceptorFactory {
			interceptorFactory = support
		}
	}
	if start.FailureConverterFactory != "" && failureFactory.Name() == "" {
		return completePayloads(nil, errors.New("workflow: failure converter factory is not part of the program"))
	}
	if start.PropagatorFactory != "" && propagatorFactory.Name() == "" {
		return completePayloads(nil, errors.New("workflow: context propagator factory is not part of the program"))
	}
	if err := configureContextPropagators(propagatorFactory, start.PropagatorConfig); err != nil {
		return completePayloads(nil, fmt.Errorf("workflow: create context propagators: %w", err))
	}
	if start.InterceptorFactory != "" && interceptorFactory.Name() == "" {
		return completePayloads(nil, errors.New("workflow: interceptor factory is not part of the program"))
	}
	if start.ConverterFactory != "" && factory.Name() != start.ConverterFactory {
		return completePayloads(nil, errors.New("workflow: converter factory is not part of the program"))
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
	ctx, err = withIncomingHeader(ctx, start.Header)
	if err != nil {
		return completePayloads(nil, err)
	}
	rootContext = ctx
	if err := configureFailureConverter(failureFactory, start.FailureConverterConfig, GetDataConverter(ctx)); err != nil {
		return completePayloads(nil, fmt.Errorf("workflow: create failure converter: %w", err))
	}
	configureInterceptors(interceptorFactory, start.InterceptorConfig)
	if interceptorFactory.Name() != "" {
		return runInterceptedFunction(handle, ctx, payloads, start.Dynamic)
	}
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
		if start.Dynamic {
			if len(args) != 2 {
				return errors.New("workflow: invalid dynamic workflow signature")
			}
			slot, ok := args[1].Pointer.(*converter.EncodedValues)
			if !ok {
				return errors.New("workflow: dynamic workflow must take converter.EncodedValues second")
			}
			*slot = encodedArguments{payloads, GetDataConverter(ctx)}
			return nil
		}
		args = args[1:]
		if err := checkArgumentCount(payloads, len(args)); err != nil {
			return err
		}
		pointers := make([]any, len(args))
		for i, arg := range args {
			if isUnsupportedProtoValue(arg.Value) || isUnsupportedProtoValue(arg.Pointer) {
				return errors.New("workflow: only generated Google protobuf messages without extensions are supported")
			}
			pointers[i] = arg.Pointer
		}
		if err := GetDataConverter(ctx).FromPayloads(payloads, pointers...); err != nil {
			return fmt.Errorf("workflow: decode arguments: %w", err)
		}
		return nil
	}, func(values ...isolate.Value) error {
		if len(values) == 0 {
			return nil
		}
		value := values[0]
		if isUnsupportedProtoValue(value.Value) || isUnsupportedProtoValue(value.Pointer) {
			return errors.New("workflow: only generated Google protobuf messages without extensions are supported")
		}
		var err error
		result, err = rootDataConverter().ToPayloads(value.Value)
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
	Header              map[string][]byte `json:"header,omitempty"`
	ID                  uint64            `json:"id,omitempty"`
	Options             *ActivityOptions  `json:"options,omitempty"`
	Function            bool              `json:"function,omitempty"`
	Name                string            `json:"name"`
	Payloads            []byte            `json:"payloads"`
	StartToCloseTimeout time.Duration     `json:"start_to_close_timeout"`
}

// Signal is one incoming workflow signal.
type Signal struct {
	Header map[string][]byte `json:"header,omitempty"`
	// Payloads is the plain serialized envelope; nextSignal decodes Input under
	// the receiving isolate owner, after host codecs have been removed.
	Payloads []byte `json:"payloads,omitempty"`
	Name     string `json:"name"`
	Input    []byte `json:"input"`
}

// SignalResult carries a serialized signal or host error through interceptor hooks.
// Signal.Payloads retains the plain envelope for decoding by the typed channel.
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
	return encodeArgsWithConverter(args, rootDataConverter())
}

func encodeArgsWithConverter(args []any, dc converter.DataConverter) ([]byte, error) {
	for _, arg := range args {
		if isUnsupportedProtoValue(arg) {
			return nil, errors.New("workflow: only generated Google protobuf messages without extensions are supported")
		}
	}
	payloads, err := dc.ToPayloads(args...)
	if err != nil {
		return nil, fmt.Errorf("workflow: encode activity arguments: %w", err)
	}
	return payloadwire.Encode(payloads)
}

func decodeActivityResult[R any](response []byte) (R, error) {
	var result R
	if isUnsupportedProtoValue(result) || isUnsupportedProtoValue(&result) {
		return result, errors.New("workflow: only generated Google protobuf messages without extensions are supported")
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
	if err := rootDataConverter().FromPayloads(payloads, &result); err != nil {
		var zero R
		return zero, fmt.Errorf("workflow: decode activity result: %w", err)
	}
	return result, nil
}

// Sleep waits on a durable Temporal timer. Do not use time.Sleep for this POC.
func sleep(ctx context.Context, duration time.Duration) error {
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

// NextSignal waits for the next incoming byte-valued signal, regardless of its
// name. Undecodable signals are discarded with a log and counter increment.
func NextSignal(ctx context.Context) (Signal, error) { return nextSignal(ctx, "") }

func nextSignal(ctx context.Context, name string) (Signal, error) {
	for {
		signal, err := nextSignalEnvelope(ctx, name)
		if err != nil {
			return signal, err
		}
		value, err := decodeSignal[[]byte](signal, ctx)
		if err != nil {
			reportCorruptSignal(ctx, signal.Name, err)
			continue
		}
		signal.Input = value
		signal.Payloads = nil
		return signal, nil
	}
}

func nextSignalEnvelope(ctx context.Context, name string) (Signal, error) {
	var signal Signal
	payload, err := call(ctx, OpSignal, []byte(name))
	if err != nil {
		return signal, err
	}
	err = json.Unmarshal(payload, &signal)
	return signal, err
}

func newSignalChannel(ctx context.Context, name string) <-chan SignalResult {
	results := make(chan SignalResult, 1)
	if ctx == nil {
		results <- SignalResult{Err: errors.New("workflow: nil context")}
		close(results)
		return results
	}
	isolate.GoInbound(func() {
		defer close(results)
		for {
			signal, err := nextSignalEnvelope(ctx, name)
			select {
			case results <- SignalResult{Signal: signal, Err: err}:
			case <-ctx.Done():
				return
			}
			if err != nil {
				return
			}
		}
	})
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
			completion.Failure, err = encodeFailure(cause, rootDataConverter())
			if err != nil {
				return err
			}
		}
	}
	payload, err := json.Marshal(completion)
	if err != nil {
		return err
	}
	isolate.MarkServiceGoroutine()
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
			completion.Failure, err = encodeFailure(cause, rootDataConverter())
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
	isolate.MarkServiceGoroutine()
	_, err = isolate.Call(OpCompletePayloads, payload)
	return err
}
