// Package testsuite adapts Temporal's host test engine to marked native
// workflows. Host callbacks remain on the host; only copied inputs and outputs
// cross the isolate boundary. Environments must be closed after the test.
package testsuite

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"isolate"
	"reflect"
	"runtime/debug"
	"strings"
	"sync"
	"time"

	"github.com/mfateev/sdk-go-poc/temporalbridge"
	"github.com/mfateev/sdk-go-poc/testref"
	pocWorker "github.com/mfateev/sdk-go-poc/worker"
	"github.com/stretchr/testify/mock"
	commonpb "go.temporal.io/api/common/v1"
	failurepb "go.temporal.io/api/failure/v1"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/converter"
	bindings "go.temporal.io/sdk/internalbindings"
	"go.temporal.io/sdk/temporal"
	goSuite "go.temporal.io/sdk/testsuite"
	goWorker "go.temporal.io/sdk/worker"
	goWorkflow "go.temporal.io/sdk/workflow"
)

type WorkflowTestSuite struct{ goSuite.WorkflowTestSuite }
type TestActivityEnvironment = goSuite.TestActivityEnvironment
type TestUpdateCallback = goSuite.TestUpdateCallback
type TestRef[T any] = testref.Ref[T]
type DevServer = goSuite.DevServer
type DevServerOptions = goSuite.DevServerOptions
type CachedDownload = goSuite.CachedDownload

var StartDevServer = goSuite.StartDevServer

var ErrMockStartChildWorkflowFailed = goSuite.ErrMockStartChildWorkflowFailed

type CleanupT interface {
	Cleanup(func())
	Errorf(string, ...any)
}

// NewTestWorkflowEnvironment accepts an optional testing.T for automatic
// cleanup. Without it, call Close explicitly. Workflow completion retains query
// state until cleanup, just as the SDK's ordinary workflow test environment.
func (s *WorkflowTestSuite) NewTestWorkflowEnvironment(t ...CleanupT) *TestWorkflowEnvironment {
	if len(t) > 1 {
		panic("testsuite: at most one cleanup owner is supported")
	}
	e := &TestWorkflowEnvironment{
		TestWorkflowEnvironment: s.WorkflowTestSuite.NewTestWorkflowEnvironment(),
		refs:                    testref.NewRegistry(), names: make(map[uintptr]string), native: make(map[string]isolate.Handle), activities: make(map[string]any), mocked: make(map[string]bool),
	}
	e.starts = make(map[string]*childStart)
	e.worker = pocWorker.Wrap(&registrationWorker{env: e})
	if len(t) == 1 {
		t[0].Cleanup(func() {
			if err := e.Close(); err != nil {
				t[0].Errorf("close isolate test environment: %v", err)
			}
		})
	}
	return e
}

type TestWorkflowEnvironment struct {
	*goSuite.TestWorkflowEnvironment
	worker        goWorker.Worker
	refs          *testref.Registry
	names         map[uintptr]string
	native        map[string]isolate.Handle
	activities    map[string]any
	mocked        map[string]bool
	workflowMocks mock.Mock
	mu            sync.Mutex
	definitions   []*testDefinition
	closed        bool
	started       bool
	childStarted  func(*goWorkflow.Info, context.Context, converter.EncodedValues)
	starts        map[string]*childStart
}

func NewRef[T any](env *TestWorkflowEnvironment, initial T) (TestRef[T], error) {
	if env == nil {
		return TestRef[T]{}, fmt.Errorf("testsuite: nil environment")
	}
	return testref.New(env.refs, initial)
}

func (e *TestWorkflowEnvironment) ensureOpen() {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed {
		panic("testsuite: environment is closed")
	}
}

// Close drains all native instances and invalidates references. It is
// idempotent. Host callbacks must not call it while ExecuteWorkflow is running.
func (e *TestWorkflowEnvironment) Close() error {
	e.mu.Lock()
	if e.closed {
		e.mu.Unlock()
		return nil
	}
	e.closed = true
	definitions := e.definitions
	e.definitions = nil
	e.mu.Unlock()
	for _, d := range definitions {
		if d.WorkflowDefinition != nil {
			d.WorkflowDefinition.Close()
		}
	}
	e.refs.Close()
	var pending []error
	for _, d := range definitions {
		if closer, ok := d.WorkflowDefinition.(interface{ CloseError() error }); ok && closer.CloseError() != nil {
			pending = append(pending, fmt.Errorf("workflow %s cleanup: %w", d.name, closer.CloseError()))
		}
		if resources, ok := d.WorkflowDefinition.(interface{ Resources() isolate.ResourceStats }); ok && resources.Resources().LiveGoroutines != 0 {
			pending = append(pending, fmt.Errorf("workflow %s still has live goroutines after cleanup: %s", d.name, d.StackTrace()))
		}
	}
	return errors.Join(pending...)
}

func (e *TestWorkflowEnvironment) RegisterWorkflow(fn any) {
	e.RegisterWorkflowWithOptions(fn, goWorkflow.RegisterOptions{})
}
func (e *TestWorkflowEnvironment) RegisterWorkflowWithOptions(fn any, options goWorkflow.RegisterOptions) {
	e.ensureOpen()
	if h, ok := isolate.LookupFunction(fn); ok {
		name := options.Name
		if name == "" {
			name = shortName(h.Name())
		}
		e.names[reflect.ValueOf(fn).Pointer()] = name
		e.native[name] = h
	} else if typ := reflect.TypeOf(fn); typ != nil && typ.Kind() == reflect.Func && typ.NumIn() > 0 && typ.In(0) == reflect.TypeFor[context.Context]() {
		panic("testsuite: native workflows must be top-level //go:isolate functions built with the isolate-aware go test toolchain")
	}
	e.worker.RegisterWorkflowWithOptions(fn, options)
}

func shortName(name string) string { return name[strings.LastIndex(name, ".")+1:] }

func (e *TestWorkflowEnvironment) workflowName(fn any) string {
	if name, ok := fn.(string); ok {
		return name
	}
	v := reflect.ValueOf(fn)
	if v.Kind() != reflect.Func {
		panic("testsuite: workflow must be a function or registered name")
	}
	if name := e.names[v.Pointer()]; name != "" {
		return name
	}
	if h, ok := isolate.LookupFunction(fn); ok {
		e.RegisterWorkflow(fn)
		return shortName(h.Name())
	}
	return ""
}

func (e *TestWorkflowEnvironment) ExecuteWorkflow(fn any, args ...any) {
	e.ensureOpen()
	if e.started {
		panic("testsuite: create a new environment for each root execution")
	}
	e.started = true
	name := e.workflowName(fn)
	if _, native := e.native[name]; native {
		fn = name
	}
	e.TestWorkflowEnvironment.ExecuteWorkflow(fn, args...)
}

func (e *TestWorkflowEnvironment) RegisterActivity(fn any) {
	e.RegisterActivityWithOptions(fn, activity.RegisterOptions{})
}
func (e *TestWorkflowEnvironment) RegisterActivityWithOptions(fn any, options activity.RegisterOptions) {
	e.ensureOpen()
	e.rememberActivity(fn, options.Name)
	e.worker.RegisterActivityWithOptions(fn, options)
}

func (e *TestWorkflowEnvironment) rememberActivity(fn any, alias string) {
	v := reflect.ValueOf(fn)
	if v.IsValid() && v.Kind() == reflect.Pointer && v.Type().Elem().Kind() == reflect.Struct {
		for i := 0; i < v.NumMethod(); i++ {
			e.activities[alias+v.Type().Method(i).Name] = v.Method(i).Interface()
		}
		return
	}
	if v.Kind() != reflect.Func {
		return
	}
	name := functionName(fn)
	e.activities[name] = fn
	if alias != "" {
		e.activities[alias] = fn
	}
}

func (e *TestWorkflowEnvironment) SetOnChildWorkflowStartedListener(listener func(*goWorkflow.Info, context.Context, converter.EncodedValues)) *TestWorkflowEnvironment {
	e.childStarted = listener
	e.TestWorkflowEnvironment.SetOnChildWorkflowStartedListener(func(info *goWorkflow.Info, _ goWorkflow.Context, args converter.EncodedValues) {
		if listener != nil {
			listener(copyInfo(info), context.Background(), args)
		}
	})
	return e
}

func (e *TestWorkflowEnvironment) SetIsolateDataConverter(options pocWorker.DataConverterOptions) error {
	return pocWorker.SetIsolateDataConverter(e.worker, options)
}
func (e *TestWorkflowEnvironment) SetIsolateFailureConverter(options pocWorker.FailureConverterOptions) error {
	return pocWorker.SetIsolateFailureConverter(e.worker, options)
}
func (e *TestWorkflowEnvironment) SetIsolateContextPropagators(options pocWorker.ContextPropagatorOptions) error {
	return pocWorker.SetIsolateContextPropagators(e.worker, options)
}
func (e *TestWorkflowEnvironment) SetIsolateInterceptors(options pocWorker.InterceptorOptions) error {
	return pocWorker.SetIsolateInterceptors(e.worker, options)
}

type registrationWorker struct {
	goWorker.Worker
	env *TestWorkflowEnvironment
}

func (w *registrationWorker) RegisterWorkflowWithOptions(fn any, options goWorkflow.RegisterOptions) {
	if f, ok := fn.(temporalbridge.Factory); ok {
		f.TestRefHandler = w.env.refs.Handle
		f.ResolveLocalActivity = func(name string) any { return w.env.activities[name] }
		fn = testFactory{env: w.env, factory: f, name: options.Name}
	}
	w.env.TestWorkflowEnvironment.RegisterWorkflowWithOptions(fn, options)
}

func (w *registrationWorker) RegisterActivityWithOptions(fn any, options activity.RegisterOptions) {
	w.env.TestWorkflowEnvironment.RegisterActivityWithOptions(fn, options)
}

type testFactory struct {
	env     *TestWorkflowEnvironment
	factory temporalbridge.Factory
	name    string
}

func (f testFactory) NewWorkflowDefinition() bindings.WorkflowDefinition {
	d := &testDefinition{owner: f.env, factory: f.factory, name: f.name}
	f.env.mu.Lock()
	defer f.env.mu.Unlock()
	if f.env.closed {
		panic("testsuite: environment is closed")
	}
	f.env.definitions = append(f.env.definitions, d)
	return d
}

type testDefinition struct {
	bindings.WorkflowDefinition
	owner        *TestWorkflowEnvironment
	factory      temporalbridge.Factory
	name         string
	host         bindings.WorkflowEnvironment
	header       *commonpb.Header
	input        *commonpb.Payloads
	initialized  bool
	mockComplete bool
	plan         *MockCallWrapper
	mockArgs     mock.Arguments
	finished     bool
	mockErr      error
	timer        *bindings.TimerID
}

func (d *testDefinition) Execute(env bindings.WorkflowEnvironment, header *commonpb.Header, input *commonpb.Payloads) {
	d.host, d.header, d.input = testHost{WorkflowEnvironment: env, owner: d.owner}, header, input
}

func (d *testDefinition) OnWorkflowTaskStarted(timeout time.Duration) {
	// The host test engine passes zero when no worker deadline was configured.
	// Leave room for instrumented package initialization in native test builds.
	if timeout <= 0 {
		timeout = 15 * time.Second
	}
	defer func() {
		if p := recover(); p != nil {
			if d.WorkflowDefinition != nil {
				d.WorkflowDefinition.Close()
			}
			d.complete(nil, hostPanic(p))
		}
	}()
	if d.finished {
		return
	}
	if !d.initialized {
		d.initialized = true
		if listener := d.owner.childStarted; listener != nil && d.host.WorkflowInfo().ParentWorkflowExecution != nil {
			listener(copyInfo(d.host.WorkflowInfo()), context.Background(), encodedValues{d.input, d.host.GetDataConverter()})
		}
		if d.owner.mocked[d.name] {
			d.host.RegisterCancelHandler(func() {
				if d.finished || d.WorkflowDefinition != nil {
					return
				}
				d.mockErr = temporal.NewCanceledError()
				d.mockComplete = true
				if d.timer != nil {
					d.host.RequestCancelTimer(*d.timer)
					d.timer = nil
				}
			})
			d.mockArgs = decodeMockArgs(d.factory.Function.Signature(), d.input, d.host.GetDataConverter())
			returns := d.owner.workflowMocks.MethodCalled(d.name, d.mockArgs...)
			d.plan = returns[0].(*MockCallWrapper)
			d.plan.observe()
			delay := time.Duration(0)
			if d.plan.delay != nil {
				delay = d.plan.delay()
			}
			if delay > 0 {
				d.timer = d.host.NewTimer(delay, goWorkflow.TimerOptions{}, func(_ *commonpb.Payloads, err error) {
					if err != nil {
						d.mockErr = err
					}
					d.mockComplete = true
				})
				return
			}
			d.mockComplete = true
		}
	}
	if d.plan != nil && d.mockComplete {
		d.mockComplete = false
		if d.mockErr != nil {
			d.complete(nil, d.mockErr)
			return
		}
		if d.plan.run != nil {
			d.plan.run(d.mockArgs)
		}
		if d.plan.replacement.Name() != "" {
			d.factory.Function = d.plan.replacement
		} else {
			result, err := d.plan.result(d.mockArgs, d.factory.Function.Signature(), d.host.GetDataConverter())
			d.complete(result, err)
			return
		}
	} else if d.plan != nil && d.WorkflowDefinition == nil {
		return
	}
	if d.WorkflowDefinition == nil {
		d.owner.admitChild(d.host.WorkflowInfo().WorkflowExecution.ID, nil)
		d.WorkflowDefinition = d.factory.NewWorkflowDefinition()
		d.WorkflowDefinition.Execute(d.host, d.header, d.input)
	}
	d.WorkflowDefinition.OnWorkflowTaskStarted(timeout)
}

// The host engine calls Close at completion. Actual cleanup belongs to the
// test environment so post-completion queries can still inspect native state.
func (d *testDefinition) Close() {}
func (d *testDefinition) StackTrace() string {
	if d.WorkflowDefinition != nil {
		return d.WorkflowDefinition.StackTrace()
	}
	return ""
}

func hostPanic(value any) error {
	return temporal.NewDefaultFailureConverter(temporal.DefaultFailureConverterOptions{}).FailureToError(&failurepb.Failure{
		Message: fmt.Sprint(value), StackTrace: string(debug.Stack()),
		FailureInfo: &failurepb.Failure_ApplicationFailureInfo{ApplicationFailureInfo: &failurepb.ApplicationFailureInfo{Type: "PanicError", NonRetryable: true}},
	})
}

func copyInfo(info *goWorkflow.Info) *goWorkflow.Info {
	// Preserve the SDK's private metadata used by its getters while cloning
	// exported pointer, map and slice fields before handing them to listeners.
	copy := *info
	var exported goWorkflow.Info
	raw, err := json.Marshal(info)
	if err != nil {
		panic(err)
	}
	if err := json.Unmarshal(raw, &exported); err != nil {
		panic(err)
	}
	dst, src := reflect.ValueOf(&copy).Elem(), reflect.ValueOf(exported)
	for i := 0; i < dst.NumField(); i++ {
		if dst.Field(i).CanSet() {
			dst.Field(i).Set(src.Field(i))
		}
	}
	return &copy
}

type encodedValues struct {
	payloads  *commonpb.Payloads
	converter converter.DataConverter
}

func (v encodedValues) Get(pointers ...any) error {
	return v.converter.FromPayloads(v.payloads, pointers...)
}
func (v encodedValues) HasValues() bool { return v.payloads != nil && len(v.payloads.Payloads) != 0 }
