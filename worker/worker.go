// Package worker keeps Temporal's worker API while routing functions marked
// //temporal:workflow through the isolate workflow factory. Ordinary workflow and
// activity registrations are passed to the Temporal Go SDK.
package worker

import (
	"context"
	"fmt"
	"isolate"
	"reflect"
	"strings"
	"sync"

	"github.com/mfateev/sdk-go-poc/internal/activityref"
	"github.com/mfateev/sdk-go-poc/temporalbridge"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/internalbindings"
	goWorker "go.temporal.io/sdk/worker"
	goWorkflow "go.temporal.io/sdk/workflow"
)

type Worker = goWorker.Worker
type Options = goWorker.Options
type WorkerConfiguration = goWorker.WorkerConfiguration
type WorkerConfigurationProvider = goWorker.WorkerConfigurationProvider

// WorkflowReplayer accepts activity registrations as alias metadata for replay.
// Replay never executes these activity functions.
type WorkflowReplayer interface {
	goWorker.WorkflowReplayer
	RegisterActivity(any)
	RegisterActivityWithOptions(any, activity.RegisterOptions)
}
type WorkflowReplayerOptions = goWorker.WorkflowReplayerOptions

// New constructs a normal Temporal worker with isolate-aware registration.
func New(c client.Client, taskQueue string, options Options) Worker {
	return Wrap(goWorker.New(c, taskQueue, options))
}

// Wrap adds isolate registration to an existing worker. All lifecycle,
// activity and service methods retain their SDK behavior.
// Call before Start or Run. SDK worker plugins receive the wrapper as their
// registry; custom worker implementations retain their own plugin wiring.
// Workers implementing WorkerConfigurationProvider supply their effective options.
// For other workers, pass the options used to construct them if not defaults.
// Isolate workflows require BlockWorkflow panic policy. Ordinary registrations
// keep the underlying worker's policy and converter.
func Wrap(w Worker, options ...Options) Worker {
	if _, ok := w.(*isolateWorker); ok {
		return w
	}
	if len(options) > 1 {
		panic("worker.Wrap: at most one Options value is supported")
	}
	var opts Options
	if len(options) == 1 {
		opts = options[0]
	}
	if provider, ok := w.(WorkerConfigurationProvider); ok {
		opts = Options(provider.GetConfiguration())
	}
	wrapped := &isolateWorker{Worker: w, workflows: activityAliases{disabled: opts.DisableRegistrationAliasing}, activities: activityAliases{disabled: opts.DisableRegistrationAliasing}, failWorkflowOnPanic: opts.WorkflowPanicPolicy == goWorker.FailWorkflow}
	wrapped.configuration = WorkerConfiguration(opts)
	wrapped.configuration = wrapped.GetConfiguration()
	internalbindings.SetWorkerPluginRegistry(w, wrapped)
	return wrapped
}

type isolateWorker struct {
	Worker
	configuration       WorkerConfiguration
	workflows           activityAliases
	activities          activityAliases
	logs                logConfiguration
	sinks               sinkConfiguration
	interceptors        interceptorConfiguration
	resources           resourceConfiguration
	converters          converterConfiguration
	propagators         propagatorConfiguration
	failures            failureConverterConfiguration
	failWorkflowOnPanic bool
	dynamicRegistered   bool
	dynamicMu           sync.Mutex
}

// GetConfiguration forwards the SDK snapshot when available. For custom workers,
// it reports the options supplied to Wrap; their effective configuration is unknown.
func (w *isolateWorker) GetConfiguration() WorkerConfiguration {
	if provider, ok := w.Worker.(WorkerConfigurationProvider); ok {
		return provider.GetConfiguration()
	}
	configuration := w.configuration
	configuration.Interceptors = append(configuration.Interceptors[:0:0], configuration.Interceptors...)
	configuration.Plugins = append(configuration.Plugins[:0:0], configuration.Plugins...)
	if configuration.MaxEagerActivityReservationsPerWorkflowTask != nil {
		value := *configuration.MaxEagerActivityReservationsPerWorkflowTask
		configuration.MaxEagerActivityReservationsPerWorkflowTask = &value
	}
	return configuration
}

// Match Temporal's short-name alias rules. This table stays on the host, and
// the resolver observes registrations made after workflow registration too.
type activityAliases struct {
	mu       sync.RWMutex
	names    map[string]string
	disabled bool
}

func (a *activityAliases) register(fn any, options activity.RegisterOptions) {
	// Struct registration uses prefix+method names, not SDK function aliases.
	// Such prefixed methods must be called by name, as in the standard SDK.
	if typ := reflect.TypeOf(fn); typ != nil && typ.Kind() != reflect.Func {
		return
	}
	name, err := activityref.Name(fn)
	if err != nil {
		panic(err)
	}
	if options.Name == "" || a.disabled {
		return
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.names == nil {
		a.names = make(map[string]string)
	}
	a.names[name] = options.Name
}

func (a *activityAliases) resolve(name string) string {
	a.mu.RLock()
	defer a.mu.RUnlock()
	if alias := a.names[name]; alias != "" {
		return alias
	}
	return name
}

func (w *isolateWorker) RegisterActivity(fn any) {
	w.RegisterActivityWithOptions(fn, activity.RegisterOptions{})
}

func (w *isolateWorker) RegisterActivityWithOptions(fn any, options activity.RegisterOptions) {
	if err := activityref.ValidateContext(fn); err != nil {
		panic(err)
	}
	w.Worker.RegisterActivityWithOptions(fn, options)
	w.activities.register(fn, options)
}

func (w *isolateWorker) RegisterDynamicActivity(fn any, options activity.DynamicRegisterOptions) {
	if err := activityref.ValidateContext(fn); err != nil {
		panic(err)
	}
	w.Worker.RegisterDynamicActivity(fn, options)
}

func (w *isolateWorker) RegisterWorkflow(fn any) {
	w.RegisterWorkflowWithOptions(fn, goWorkflow.RegisterOptions{})
}

func (w *isolateWorker) RegisterWorkflowWithOptions(fn any, options goWorkflow.RegisterOptions) {
	if w.failWorkflowOnPanic {
		if _, marked := isolate.LookupFunction(fn); marked {
			panic("worker: isolate workflows require BlockWorkflow panic policy")
		}
	}
	original := fn
	fn, options = registration(fn, options, w.activities.resolve, w.logs.resolve)
	if factory, ok := fn.(temporalbridge.Factory); ok {
		factory.ResolveWorkflow = w.workflows.resolve
		factory.ResolveSink = w.sinks.resolve
		factory.ResolveInterceptors = w.interceptors.resolve
		factory.ResolveContextPropagators = w.propagators.resolve
		factory.ResolveFailureConverter = w.failures.resolve
		fn = factory
	}
	w.Worker.RegisterWorkflowWithOptions(converterFactory(resourceFactory(fn, &w.resources), &w.converters), options)
	w.workflows.register(original, activity.RegisterOptions{Name: options.Name})
}

func registration(fn any, options goWorkflow.RegisterOptions, resolve func(string) string, logs func() LogHandler) (any, goWorkflow.RegisterOptions) {
	handle, ok := isolate.LookupFunction(fn)
	if !ok {
		return fn, options
	}
	signature := handle.Signature()
	if signature.NumIn() == 0 || signature.In(0) != reflect.TypeFor[context.Context]() {
		panic("worker: isolate workflow must take context.Context first")
	}
	if options.Name == "" {
		name := handle.Name()
		options.Name = name[strings.LastIndex(name, ".")+1:]
	}
	return temporalbridge.Factory{Function: handle, ResolveActivity: resolve, ResolveLogHandler: logs}, options
}

type isolateReplayer struct {
	dynamicRegistered bool
	dynamicMu         sync.Mutex
	goWorker.WorkflowReplayer
	workflows    activityAliases
	activities   activityAliases
	logs         logConfiguration
	sinks        sinkConfiguration
	interceptors interceptorConfiguration
	resources    resourceConfiguration
	converters   converterConfiguration
	propagators  propagatorConfiguration
	failures     failureConverterConfiguration
}

func (r *isolateReplayer) RegisterActivity(fn any) {
	r.RegisterActivityWithOptions(fn, activity.RegisterOptions{})
}

func (r *isolateReplayer) RegisterActivityWithOptions(fn any, options activity.RegisterOptions) {
	if err := activityref.ValidateContext(fn); err != nil {
		panic(err)
	}
	r.activities.register(fn, options)
}

func (r *isolateReplayer) RegisterWorkflow(fn any) {
	r.RegisterWorkflowWithOptions(fn, goWorkflow.RegisterOptions{})
}

func (r *isolateReplayer) RegisterWorkflowWithOptions(fn any, options goWorkflow.RegisterOptions) {
	original := fn
	fn, options = registration(fn, options, r.activities.resolve, r.logs.resolve)
	if factory, ok := fn.(temporalbridge.Factory); ok {
		factory.ResolveWorkflow = r.workflows.resolve
		factory.ResolveSink = r.sinks.resolve
		factory.ResolveInterceptors = r.interceptors.resolve
		factory.ResolveContextPropagators = r.propagators.resolve
		factory.ResolveFailureConverter = r.failures.resolve
		factory.ReplayOnly = true
		fn = factory
	}
	r.WorkflowReplayer.RegisterWorkflowWithOptions(converterFactory(resourceFactory(fn, &r.resources), &r.converters), options)
	r.workflows.register(original, activity.RegisterOptions{Name: options.Name})
}

// GetWorkflowResult preserves the pinned SDK replayer's result-inspection
// extension used by the deterministic clock replay check.
func (r *isolateReplayer) GetWorkflowResult(id string, result any) error {
	getter, ok := r.WorkflowReplayer.(interface{ GetWorkflowResult(string, any) error })
	if !ok {
		return fmt.Errorf("Temporal replayer does not expose its completion result")
	}
	return getter.GetWorkflowResult(id, result)
}

func NewWorkflowReplayer() WorkflowReplayer {
	r, err := NewWorkflowReplayerWithOptions(WorkflowReplayerOptions{})
	if err != nil {
		panic(err)
	}
	return r
}

func NewWorkflowReplayerWithOptions(options WorkflowReplayerOptions) (WorkflowReplayer, error) {
	r, err := goWorker.NewWorkflowReplayerWithOptions(options)
	if err != nil {
		return nil, err
	}
	wrapped := &isolateReplayer{WorkflowReplayer: r, workflows: activityAliases{disabled: options.DisableRegistrationAliasing}, activities: activityAliases{disabled: options.DisableRegistrationAliasing}}
	internalbindings.SetReplayerPluginRegistry(r, wrapped)
	return wrapped, nil
}

func InterruptCh() <-chan any { return goWorker.InterruptCh() }
