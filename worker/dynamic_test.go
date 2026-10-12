package worker

import (
	"context"
	"reflect"
	"testing"

	"github.com/mfateev/sdk-go-poc/example/dynamic"
	"github.com/mfateev/sdk-go-poc/temporalbridge"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/converter"
	sdkworker "go.temporal.io/sdk/worker"
	sdkwf "go.temporal.io/sdk/workflow"
)

type dynamicRegistrationWorker struct {
	sdkworker.Worker
	fn      any
	options sdkwf.DynamicRegisterOptions
}

func (w *dynamicRegistrationWorker) RegisterDynamicWorkflow(fn any, options sdkwf.DynamicRegisterOptions) {
	w.fn, w.options = fn, options
}
func hostDynamic(_ sdkwf.Context, _ converter.EncodedValues) (string, error) { return "host", nil }

//go:isolate
func invalidDynamicInput(context.Context, string) (string, error) { return "", nil }

func TestDynamicRegistrationRoutesNativeAndHostWorkflows(t *testing.T) {
	for _, fn := range []any{dynamic.Workflow, hostDynamic} {
		host := new(dynamicRegistrationWorker)
		w := Wrap(host)
		var requested string
		options := sdkwf.DynamicRegisterOptions{LoadDynamicRuntimeOptions: func(details sdkwf.LoadDynamicRuntimeOptionsDetails) (sdkwf.DynamicRuntimeOptions, error) {
			requested = details.WorkflowType.Name
			return sdkwf.DynamicRuntimeOptions{}, nil
		}}
		w.RegisterDynamicWorkflow(fn, options)
		if factory, ok := host.fn.(temporalbridge.Factory); ok {
			require.True(t, factory.Dynamic)
			require.NotEmpty(t, factory.Function.Name())
			require.NotNil(t, factory.ResolveDataConverter)
			require.NotNil(t, factory.ResolveContextPropagators)
			require.NotNil(t, factory.ResolveFailureConverter)
			require.NotNil(t, factory.ResolveInterceptors)
			require.NotNil(t, factory.ResolveResourceOptions)
		} else {
			require.Equal(t, reflect.ValueOf(hostDynamic).Pointer(), reflect.ValueOf(host.fn).Pointer())
		}
		_, err := host.options.LoadDynamicRuntimeOptions(sdkwf.LoadDynamicRuntimeOptionsDetails{WorkflowType: sdkwf.Type{Name: "runtime-name"}})
		require.NoError(t, err)
		require.Equal(t, "runtime-name", requested)
	}
}

func TestDynamicRegistrationRejectsInvalidNativeSignaturesAndPanicPolicy(t *testing.T) {
	host := new(dynamicRegistrationWorker)
	w := Wrap(host)
	for _, fn := range []any{invalidDynamicInput} {
		require.Panics(t, func() { w.RegisterDynamicWorkflow(fn, sdkwf.DynamicRegisterOptions{}) })
		require.Nil(t, host.fn)
	}
	w = Wrap(host, Options{WorkflowPanicPolicy: sdkworker.FailWorkflow})
	require.PanicsWithValue(t, "worker: isolate workflows require BlockWorkflow panic policy", func() { w.RegisterDynamicWorkflow(dynamic.Workflow, sdkwf.DynamicRegisterOptions{}) })
	require.Nil(t, host.fn)
	w.RegisterDynamicWorkflow(hostDynamic, sdkwf.DynamicRegisterOptions{})
	require.NotNil(t, host.fn)
}

func TestDynamicRegistrationRejectsConcurrentDuplicates(t *testing.T) {
	for _, target := range []interface {
		RegisterDynamicWorkflow(any, sdkwf.DynamicRegisterOptions)
	}{Wrap(new(dynamicRegistrationWorker)), NewWorkflowReplayer()} {
		ready := make(chan struct{})
		outcomes := make(chan bool, 2)
		for range 2 {
			go func() {
				succeeded := false
				defer func() { _ = recover(); outcomes <- succeeded }()
				<-ready
				target.RegisterDynamicWorkflow(dynamic.Workflow, sdkwf.DynamicRegisterOptions{})
				succeeded = true
			}()
		}
		close(ready)
		first, second := <-outcomes, <-outcomes
		require.NotEqual(t, first, second, "exactly one dynamic workflow registration must succeed")
	}
}
