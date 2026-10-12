package worker

import (
	"context"
	"isolate"
	"reflect"

	"github.com/mfateev/sdk-go-poc/temporalbridge"
	"go.temporal.io/sdk/converter"
	goWorkflow "go.temporal.io/sdk/workflow"
)

func dynamicRegistration(fn any) any {
	handle, marked := isolate.LookupFunction(fn)
	if !marked {
		return fn
	}
	t := handle.Signature()
	if t.NumIn() != 2 || t.In(0) != reflect.TypeFor[context.Context]() || t.In(1) != reflect.TypeFor[converter.EncodedValues]() || t.IsVariadic() {
		panic("worker: dynamic isolate workflow must take (context.Context, converter.EncodedValues)")
	}
	if t.NumOut() < 1 || t.NumOut() > 2 || !t.Out(t.NumOut()-1).Implements(reflect.TypeFor[error]()) {
		panic("worker: dynamic isolate workflow must return error or (result, error)")
	}
	if t.NumOut() == 2 {
		switch t.Out(0).Kind() {
		case reflect.Func, reflect.Chan, reflect.UnsafePointer:
			panic("worker: dynamic isolate workflow result must be serializable")
		}
	}
	return temporalbridge.Factory{Function: handle, Dynamic: true}
}

func (w *isolateWorker) RegisterDynamicWorkflow(fn any, options goWorkflow.DynamicRegisterOptions) {
	w.dynamicMu.Lock()
	defer w.dynamicMu.Unlock()
	if w.dynamicRegistered {
		panic("dynamic workflow already registered")
	}
	fn = dynamicRegistration(fn)
	if factory, ok := fn.(temporalbridge.Factory); ok {
		if w.failWorkflowOnPanic {
			panic("worker: isolate workflows require BlockWorkflow panic policy")
		}
		factory.ResolveActivity = w.activities.resolve
		factory.ResolveWorkflow = w.workflows.resolve
		factory.ResolveLogHandler = w.logs.resolve
		factory.ResolveSink = w.sinks.resolve
		factory.ResolveInterceptors = w.interceptors.resolve
		factory.ResolveContextPropagators = w.propagators.resolve
		factory.ResolveFailureConverter = w.failures.resolve
		fn = factory
	}
	w.Worker.RegisterDynamicWorkflow(converterFactory(resourceFactory(fn, &w.resources), &w.converters), options)
	w.dynamicRegistered = true
}

func (r *isolateReplayer) RegisterDynamicWorkflow(fn any, options goWorkflow.DynamicRegisterOptions) {
	r.dynamicMu.Lock()
	defer r.dynamicMu.Unlock()
	if r.dynamicRegistered {
		panic("dynamic workflow already registered")
	}
	fn = dynamicRegistration(fn)
	if factory, ok := fn.(temporalbridge.Factory); ok {
		factory.ResolveActivity = r.activities.resolve
		factory.ResolveWorkflow = r.workflows.resolve
		factory.ResolveLogHandler = r.logs.resolve
		factory.ResolveSink = r.sinks.resolve
		factory.ResolveInterceptors = r.interceptors.resolve
		factory.ResolveContextPropagators = r.propagators.resolve
		factory.ResolveFailureConverter = r.failures.resolve
		factory.ReplayOnly = true
		fn = factory
	}
	r.WorkflowReplayer.RegisterDynamicWorkflow(converterFactory(resourceFactory(fn, &r.resources), &r.converters), options)
	r.dynamicRegistered = true
}
