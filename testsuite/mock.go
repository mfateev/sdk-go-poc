package testsuite

import (
	"context"
	"fmt"
	"isolate"
	"reflect"
	"runtime"
	"strings"
	"sync/atomic"
	"time"

	"github.com/stretchr/testify/mock"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/sdk/converter"
	goSuite "go.temporal.io/sdk/testsuite"
	goWorkflow "go.temporal.io/sdk/workflow"
)

// MockCallWrapper preserves SDK mock controls. Native workflow return callbacks
// execute on the host. Use ReturnIsolate for a replacement that calls workflow
// APIs; that replacement must be a marked function with the same signature.
type MockCallWrapper struct {
	call          *mock.Call
	sdk           *goSuite.MockCallWrapper
	signature     reflect.Type
	values        []any
	delay         func() time.Duration
	run           func(mock.Arguments)
	replacement   isolate.Handle
	panicMessage  *string
	calls         atomic.Int64
	minimum       int64
	optional      bool
	prerequisites []*MockCallWrapper
}

func functionName(fn any) string {
	name := runtime.FuncForPC(reflect.ValueOf(fn).Pointer()).Name()
	return strings.TrimSuffix(shortName(name), "-fm")
}

func (e *TestWorkflowEnvironment) OnWorkflow(fn any, args ...any) *MockCallWrapper {
	e.ensureOpen()
	name := e.workflowName(fn)
	h, native := e.native[name]
	if !native {
		return sdkMock(e.TestWorkflowEnvironment.OnWorkflow(fn, args...))
	}
	e.mocked[name] = true
	w := &MockCallWrapper{signature: h.Signature()}
	w.call = e.workflowMocks.On(name, args...).Return(w)
	return w
}

func (e *TestWorkflowEnvironment) OnActivity(fn any, args ...any) *MockCallWrapper {
	e.ensureOpen()
	if reflect.TypeOf(fn) != nil && reflect.TypeOf(fn).Kind() == reflect.Func {
		e.rememberActivity(fn, "")
	}
	return sdkMock(e.TestWorkflowEnvironment.OnActivity(fn, args...))
}

func sdkMock(sdk *goSuite.MockCallWrapper) *MockCallWrapper {
	w := &MockCallWrapper{sdk: sdk}
	sdk.Run(func(args mock.Arguments) {
		w.observe()
		if w.run != nil {
			w.run(args)
		}
	})
	return w
}

func (w *MockCallWrapper) observe() {
	for _, previous := range w.prerequisites {
		minimum := previous.minimum
		if minimum <= 0 && !previous.optional {
			minimum = 1
		}
		if previous.calls.Load() < minimum {
			panic("testsuite: mock called before its prerequisite expectations were satisfied")
		}
	}
	w.calls.Add(1)
}

func (w *MockCallWrapper) Once() *MockCallWrapper  { return w.Times(1) }
func (w *MockCallWrapper) Twice() *MockCallWrapper { return w.Times(2) }
func (w *MockCallWrapper) Times(n int) *MockCallWrapper {
	w.minimum = int64(n)
	if w.sdk != nil {
		w.sdk.Times(n)
	} else {
		w.call.Times(n)
	}
	return w
}
func (w *MockCallWrapper) Maybe() *MockCallWrapper {
	w.optional = true
	if w.sdk != nil {
		w.sdk.Maybe()
	} else {
		w.call.Maybe()
	}
	return w
}
func (w *MockCallWrapper) Never() *MockCallWrapper {
	if w.sdk != nil {
		w.sdk.Never()
	} else {
		w.call.Maybe()
		w.Panic("unexpected workflow mock call")
	}
	return w
}
func (w *MockCallWrapper) Run(fn func(mock.Arguments)) *MockCallWrapper {
	w.run = fn
	return w
}
func (w *MockCallWrapper) After(delay time.Duration) *MockCallWrapper {
	return w.AfterFn(func() time.Duration { return delay })
}
func (w *MockCallWrapper) AfterFn(fn func() time.Duration) *MockCallWrapper {
	if w.sdk != nil {
		w.sdk.AfterFn(fn)
	} else {
		w.delay = fn
	}
	return w
}
func (w *MockCallWrapper) Return(values ...any) *MockCallWrapper {
	if w.sdk != nil {
		w.sdk.Return(values...)
	} else {
		w.values = append([]any(nil), values...)
		w.replacement = isolate.Handle{}
	}
	return w
}
func (w *MockCallWrapper) Panic(message string) *MockCallWrapper {
	if w.sdk != nil {
		w.sdk.Panic(message)
	} else {
		w.panicMessage = &message
	}
	return w
}

func (w *MockCallWrapper) ReturnIsolate(fn any) *MockCallWrapper {
	if w.sdk != nil {
		panic("testsuite: ReturnIsolate requires a native workflow mock")
	}
	h, ok := isolate.LookupFunction(fn)
	if !ok || h.Signature() != w.signature {
		panic("testsuite: replacement must be a marked workflow with the same signature")
	}
	w.replacement = h
	w.values = nil
	return w
}

func (w *MockCallWrapper) NotBefore(calls ...*MockCallWrapper) *MockCallWrapper {
	var sdk []*goSuite.MockCallWrapper
	var native []*mock.Call
	for _, previous := range calls {
		if previous == nil {
			panic("testsuite: nil mock prerequisite")
		}
		if w.sdk != nil && previous.sdk != nil {
			sdk = append(sdk, previous.sdk)
		} else if w.call != nil && previous.call != nil {
			native = append(native, previous.call)
		} else {
			w.prerequisites = append(w.prerequisites, previous)
		}
	}
	if len(sdk) != 0 {
		w.sdk.NotBefore(sdk...)
	}
	if len(native) != 0 {
		w.call.NotBefore(native...)
	}
	return w
}

func (e *TestWorkflowEnvironment) InOrderMockCalls(calls ...*MockCallWrapper) {
	for i := 1; i < len(calls); i++ {
		calls[i].NotBefore(calls[i-1])
	}
}

func validateMockReturn(signature reflect.Type, values []any) {
	if len(values) == 1 && reflect.TypeOf(values[0]) != nil && reflect.TypeOf(values[0]).Kind() == reflect.Func {
		if reflect.TypeOf(values[0]) != signature {
			panic("testsuite: workflow mock callback must have the original workflow signature")
		}
		return
	}
	if len(values) != signature.NumOut() {
		panic(fmt.Sprintf("testsuite: mock returns %d values, workflow requires %d", len(values), signature.NumOut()))
	}
	for i, value := range values {
		if value == nil && i != signature.NumOut()-1 {
			switch signature.Out(i).Kind() {
			case reflect.Pointer, reflect.Interface, reflect.Map, reflect.Slice, reflect.Array:
			default:
				panic(fmt.Sprintf("testsuite: nil mock return %d requires a nullable result, workflow returns %v", i, signature.Out(i)))
			}
		}
		if value != nil && !reflect.TypeOf(value).AssignableTo(signature.Out(i)) {
			panic(fmt.Sprintf("testsuite: mock return %d has type %T, requires %v", i, value, signature.Out(i)))
		}
	}
}

func decodeMockArgs(signature reflect.Type, input *commonpb.Payloads, dc converter.DataConverter) mock.Arguments {
	args := mock.Arguments{context.Background()}
	pointers := make([]any, signature.NumIn()-1)
	for i := range pointers {
		pointers[i] = reflect.New(signature.In(i + 1)).Interface()
	}
	if err := dc.FromPayloads(input, pointers...); err != nil {
		panic(err)
	}
	for _, pointer := range pointers {
		args = append(args, reflect.ValueOf(pointer).Elem().Interface())
	}
	return args
}

func (w *MockCallWrapper) result(args mock.Arguments, signature reflect.Type, dc converter.DataConverter) (*commonpb.Payloads, error) {
	if w.panicMessage != nil {
		panic(*w.panicMessage)
	}
	values := w.values
	validateMockReturn(signature, values)
	if len(values) == 1 && reflect.TypeOf(values[0]) != nil && reflect.TypeOf(values[0]).Kind() == reflect.Func {
		in := make([]reflect.Value, len(args))
		for i, arg := range args {
			if arg == nil {
				in[i] = reflect.Zero(signature.In(i))
			} else {
				in[i] = reflect.ValueOf(arg)
			}
		}
		returned := reflect.ValueOf(values[0]).Call(in)
		values = make([]any, len(returned))
		for i, v := range returned {
			values[i] = v.Interface()
		}
	}
	if signature.NumOut() == 0 {
		return nil, nil
	}
	if cause := values[len(values)-1]; cause != nil {
		return nil, cause.(error)
	}
	if signature.NumOut() == 1 {
		return nil, nil
	}
	return dc.ToPayloads(values[0])
}

func (e *TestWorkflowEnvironment) AssertExpectations(t mock.TestingT) bool {
	host := e.TestWorkflowEnvironment.AssertExpectations(t)
	native := e.workflowMocks.AssertExpectations(t)
	return host && native
}

type silentTestingT struct{}

func (silentTestingT) Logf(string, ...any)   {}
func (silentTestingT) Errorf(string, ...any) {}
func (silentTestingT) FailNow()              {}

// Match the SDK's generic assertions across both workflow and activity mocks.
func (e *TestWorkflowEnvironment) AssertCalled(t mock.TestingT, name string, args ...any) bool {
	return e.AssertWorkflowCalled(silentTestingT{}, name, args...) ||
		e.AssertActivityCalled(silentTestingT{}, name, args...) ||
		e.AssertWorkflowCalled(t, name, args...) || e.AssertActivityCalled(t, name, args...)
}
func (e *TestWorkflowEnvironment) AssertNotCalled(t mock.TestingT, name string, args ...any) bool {
	workflow := e.AssertWorkflowNotCalled(t, name, args...)
	activity := e.AssertActivityNotCalled(t, name, args...)
	return workflow && activity
}
func (e *TestWorkflowEnvironment) AssertNumberOfCalls(t mock.TestingT, name string, n int) bool {
	return e.AssertWorkflowNumberOfCalls(silentTestingT{}, name, n) ||
		e.AssertActivityNumberOfCalls(silentTestingT{}, name, n) ||
		e.AssertWorkflowNumberOfCalls(t, name, n) || e.AssertActivityNumberOfCalls(t, name, n)
}

// Wrap the host mock controls too, so they can participate in InOrderMockCalls.
func (e *TestWorkflowEnvironment) OnSignalExternalWorkflow(namespace, workflowID, runID, signalName, arg any) *MockCallWrapper {
	return sdkMock(e.TestWorkflowEnvironment.OnSignalExternalWorkflow(namespace, workflowID, runID, signalName, arg))
}
func (e *TestWorkflowEnvironment) OnRequestCancelExternalWorkflow(namespace, workflowID, runID string) *MockCallWrapper {
	return sdkMock(e.TestWorkflowEnvironment.OnRequestCancelExternalWorkflow(namespace, workflowID, runID))
}
func (e *TestWorkflowEnvironment) OnGetVersion(changeID string, minSupported, maxSupported goWorkflow.Version) *MockCallWrapper {
	return sdkMock(e.TestWorkflowEnvironment.OnGetVersion(changeID, minSupported, maxSupported))
}
func (e *TestWorkflowEnvironment) OnSideEffect() *MockCallWrapper {
	return sdkMock(e.TestWorkflowEnvironment.OnSideEffect())
}
func (e *TestWorkflowEnvironment) OnMutableSideEffect(id string) *MockCallWrapper {
	return sdkMock(e.TestWorkflowEnvironment.OnMutableSideEffect(id))
}
func (e *TestWorkflowEnvironment) OnUpsertSearchAttributes(attributes any) *MockCallWrapper {
	return sdkMock(e.TestWorkflowEnvironment.OnUpsertSearchAttributes(attributes))
}
func (e *TestWorkflowEnvironment) OnUpsertTypedSearchAttributes(attributes any) *MockCallWrapper {
	return sdkMock(e.TestWorkflowEnvironment.OnUpsertTypedSearchAttributes(attributes))
}
func (e *TestWorkflowEnvironment) OnUpsertMemo(attributes any) *MockCallWrapper {
	return sdkMock(e.TestWorkflowEnvironment.OnUpsertMemo(attributes))
}
func (e *TestWorkflowEnvironment) OnNexusOperation(service, operation, input, options any) *MockCallWrapper {
	return sdkMock(e.TestWorkflowEnvironment.OnNexusOperation(service, operation, input, options))
}
func (e *TestWorkflowEnvironment) AssertWorkflowCalled(t mock.TestingT, name string, args ...any) bool {
	if e.mocked[name] {
		return e.workflowMocks.AssertCalled(t, name, args...)
	}
	return e.TestWorkflowEnvironment.AssertWorkflowCalled(t, name, args...)
}
func (e *TestWorkflowEnvironment) AssertWorkflowNotCalled(t mock.TestingT, name string, args ...any) bool {
	if _, native := e.native[name]; native {
		return e.workflowMocks.AssertNotCalled(t, name, args...)
	}
	return e.TestWorkflowEnvironment.AssertWorkflowNotCalled(t, name, args...)
}
func (e *TestWorkflowEnvironment) AssertWorkflowNumberOfCalls(t mock.TestingT, name string, n int) bool {
	if _, native := e.native[name]; native {
		return e.workflowMocks.AssertNumberOfCalls(t, name, n)
	}
	return e.TestWorkflowEnvironment.AssertWorkflowNumberOfCalls(t, name, n)
}
