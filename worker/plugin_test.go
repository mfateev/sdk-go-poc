package worker

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/mfateev/sdk-go-poc/example/dynamic"
	"github.com/mfateev/sdk-go-poc/example/order"
	"github.com/mfateev/sdk-go-poc/temporalbridge"
	"github.com/stretchr/testify/require"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	sdkworker "go.temporal.io/sdk/worker"
	sdkwf "go.temporal.io/sdk/workflow"
)

var errPluginRegistrationStop = errors.New("stop after plugin registrations")

type clientPluginBase = client.PluginBase
type workerPluginBase = sdkworker.PluginBase

type nativeRegistrationPlugin struct {
	clientPluginBase
	workerPluginBase
	t              *testing.T
	named, dynamic int
	replay         bool
	registry       any
}

func (*nativeRegistrationPlugin) Name() string { return "native-registration-test" }
func (p *nativeRegistrationPlugin) onNamed(fn any, options sdkwf.RegisterOptions) {
	p.named++
	require.Equal(p.t, "plugin-workflow", options.Name)
	p.checkFactory(fn, false)
}
func (p *nativeRegistrationPlugin) onDynamic(fn any, _ sdkwf.DynamicRegisterOptions) {
	p.dynamic++
	p.checkFactory(fn, true)
}
func (p *nativeRegistrationPlugin) checkFactory(fn any, dynamic bool) {
	f, ok := fn.(temporalbridge.Factory)
	require.True(p.t, ok, "native registration must be adapted before the host callback")
	require.Equal(p.t, dynamic, f.Dynamic)
	require.Equal(p.t, p.replay, f.ReplayOnly)
	require.NotNil(p.t, f.ResolveActivity)
	require.NotNil(p.t, f.ResolveWorkflow)
	require.NotNil(p.t, f.ResolveDataConverter)
	require.NotNil(p.t, f.ResolveInterceptors)
}
func (p *nativeRegistrationPlugin) ConfigureWorker(_ context.Context, o sdkworker.PluginConfigureWorkerOptions) error {
	o.WorkerRegistryOptions.OnRegisterWorkflow = p.onNamed
	o.WorkerRegistryOptions.OnRegisterDynamicWorkflow = p.onDynamic
	return nil
}
func (p *nativeRegistrationPlugin) ConfigureWorkflowReplayer(_ context.Context, o sdkworker.PluginConfigureWorkflowReplayerOptions) error {
	o.WorkflowReplayerRegistryOptions.OnRegisterWorkflow = p.onNamed
	o.WorkflowReplayerRegistryOptions.OnRegisterDynamicWorkflow = p.onDynamic
	return nil
}
func (p *nativeRegistrationPlugin) StartWorker(_ context.Context, o sdkworker.PluginStartWorkerOptions, _ func(context.Context, sdkworker.PluginStartWorkerOptions) error) error {
	require.Same(p.t, p.registry, o.WorkerRegistry)
	o.WorkerRegistry.RegisterActivityWithOptions(testActivity, activity.RegisterOptions{Name: "plugin-activity"})
	o.WorkerRegistry.RegisterWorkflowWithOptions(order.EchoWorkflow, sdkwf.RegisterOptions{Name: "plugin-workflow"})
	o.WorkerRegistry.RegisterDynamicWorkflow(dynamic.Workflow, sdkwf.DynamicRegisterOptions{})
	return errPluginRegistrationStop
}
func (p *nativeRegistrationPlugin) ReplayWorkflow(_ context.Context, o sdkworker.PluginReplayWorkflowOptions, _ func(context.Context, sdkworker.PluginReplayWorkflowOptions) error) error {
	require.Same(p.t, p.registry, o.WorkflowReplayRegistry)
	o.WorkflowReplayRegistry.RegisterWorkflowWithOptions(order.EchoWorkflow, sdkwf.RegisterOptions{Name: "plugin-workflow"})
	o.WorkflowReplayRegistry.RegisterDynamicWorkflow(dynamic.Workflow, sdkwf.DynamicRegisterOptions{})
	return errPluginRegistrationStop
}
func TestPluginNativeRegistrations(t *testing.T) {
	for _, inherited := range []bool{false, true} {
		for _, wrap := range []bool{false, true} {
			name := map[bool]string{false: "explicit", true: "inherited"}[inherited] + map[bool]string{false: "/new", true: "/wrap"}[wrap]
			t.Run(name, func(t *testing.T) {
				p := &nativeRegistrationPlugin{t: t}
				co := client.Options{}
				wo := Options{}
				if inherited {
					co.Plugins = []client.Plugin{p}
				} else {
					wo.Plugins = []sdkworker.Plugin{p}
				}
				c, err := client.NewLazyClient(co)
				require.NoError(t, err)
				defer c.Close()
				var w Worker
				if wrap {
					w = Wrap(sdkworker.New(c, "plugin-test", wo), wo)
				} else {
					w = New(c, "plugin-test", wo)
				}
				require.Same(t, w, Wrap(w))
				p.registry = w
				require.ErrorIs(t, w.Start(), errPluginRegistrationStop)
				require.Equal(t, 1, p.named)
				require.Equal(t, 1, p.dynamic)
				require.Equal(t, "plugin-activity", w.(*isolateWorker).activities.resolve("testActivity"))
				require.Equal(t, "plugin-workflow", w.(*isolateWorker).workflows.resolve("EchoWorkflow"))
			})
		}
	}
}
func TestPluginNativeReplayRegistrations(t *testing.T) {
	p := &nativeRegistrationPlugin{t: t, replay: true}
	r, err := NewWorkflowReplayerWithOptions(WorkflowReplayerOptions{Plugins: []sdkworker.Plugin{p}})
	require.NoError(t, err)
	p.registry = r
	require.ErrorIs(t, r.ReplayWorkflowHistory(nil, &historypb.History{}), errPluginRegistrationStop)
	require.Equal(t, 1, p.named)
	require.Equal(t, 1, p.dynamic)
}

// configurationMutationPlugin tests the options that the POC consumes during
// registration, including registrations made through the SDK's startup chain.
type configurationMutationPlugin struct {
	clientPluginBase
	workerPluginBase
	policy          sdkworker.WorkflowPanicPolicy
	disableAliasing bool
	register        func(sdkworker.PluginStartWorkerOptions)
}

func (*configurationMutationPlugin) Name() string { return "configuration-mutation" }
func (p *configurationMutationPlugin) ConfigureWorker(_ context.Context, o sdkworker.PluginConfigureWorkerOptions) error {
	o.WorkerOptions.WorkflowPanicPolicy = p.policy
	o.WorkerOptions.DisableRegistrationAliasing = p.disableAliasing
	return nil
}
func (p *configurationMutationPlugin) StartWorker(_ context.Context, o sdkworker.PluginStartWorkerOptions, _ func(context.Context, sdkworker.PluginStartWorkerOptions) error) error {
	p.register(o)
	return errPluginRegistrationStop
}

func TestPluginEffectiveWorkerConfiguration(t *testing.T) {
	for _, inherited := range []bool{false, true} {
		for _, wrap := range []bool{false, true} {
			for _, fail := range []bool{false, true} {
				for _, startup := range []bool{false, true} {
					for _, dynamicRegistration := range []bool{false, true} {
						name := fmt.Sprintf("inherited=%t/wrap=%t/fail=%t/startup=%t/dynamic=%t", inherited, wrap, fail, startup, dynamicRegistration)
						t.Run(name, func(t *testing.T) {
							p := &configurationMutationPlugin{disableAliasing: fail}
							wo := Options{DisableRegistrationAliasing: !fail}
							if fail {
								p.policy = sdkworker.FailWorkflow
							} else {
								wo.WorkflowPanicPolicy = sdkworker.FailWorkflow
							}
							co := client.Options{}
							if inherited {
								co.Plugins = []client.Plugin{p}
							} else {
								wo.Plugins = []sdkworker.Plugin{p}
							}
							c, err := client.NewLazyClient(co)
							require.NoError(t, err)
							defer c.Close()
							var w Worker
							if wrap {
								// Deliberately supply the stale original options.
								w = Wrap(sdkworker.New(c, "configuration-test", wo), wo)
							} else {
								w = New(c, "configuration-test", wo)
							}
							provider, ok := w.(WorkerConfigurationProvider)
							require.True(t, ok)
							cfg := provider.GetConfiguration()
							require.Equal(t, p.policy, cfg.WorkflowPanicPolicy)
							require.Equal(t, fail, cfg.DisableRegistrationAliasing)
							cfg.WorkflowPanicPolicy = sdkworker.FailWorkflow
							require.Equal(t, p.policy, provider.GetConfiguration().WorkflowPanicPolicy)

							register := func(registry sdkworker.Registry) {
								if dynamicRegistration {
									registry.RegisterDynamicWorkflow(dynamic.Workflow, sdkwf.DynamicRegisterOptions{})
								} else {
									registry.RegisterWorkflowWithOptions(order.EchoWorkflow, sdkwf.RegisterOptions{Name: "configured-workflow"})
								}
							}
							attempt := func() {
								if startup {
									p.register = func(o sdkworker.PluginStartWorkerOptions) {
										require.Same(t, w, o.WorkerRegistry)
										// The plugin registry intentionally exposes fewer methods.
										if dynamicRegistration {
											o.WorkerRegistry.RegisterDynamicWorkflow(dynamic.Workflow, sdkwf.DynamicRegisterOptions{})
										} else {
											o.WorkerRegistry.RegisterWorkflowWithOptions(order.EchoWorkflow, sdkwf.RegisterOptions{Name: "configured-workflow"})
										}
									}
									require.ErrorIs(t, w.Start(), errPluginRegistrationStop)
								} else {
									register(w)
								}
							}
							if fail {
								require.PanicsWithValue(t, "worker: isolate workflows require BlockWorkflow panic policy", attempt)
							} else {
								require.NotPanics(t, attempt)
							}

							w.RegisterActivityWithOptions(testActivity, activity.RegisterOptions{Name: "configured-activity"})
							expected := "configured-activity"
							if fail {
								expected = "testActivity"
							}
							require.Equal(t, expected, w.(*isolateWorker).activities.resolve("testActivity"))
							if !dynamicRegistration && !fail {
								require.Equal(t, "configured-workflow", w.(*isolateWorker).workflows.resolve("EchoWorkflow"))
							}
							// FailWorkflow remains valid for ordinary host workflows.
							require.NotPanics(t, func() { w.RegisterWorkflow(hostConfigurationWorkflow) })
						})
					}
				}
			}
		}
	}
}

func hostConfigurationWorkflow(sdkwf.Context) error { return nil }

func TestPluginEffectiveRegistrationAliasing(t *testing.T) {
	for _, disabled := range []bool{false, true} {
		t.Run(fmt.Sprint(disabled), func(t *testing.T) {
			p := &configurationMutationPlugin{disableAliasing: disabled}
			c, err := client.NewLazyClient(client.Options{Plugins: []client.Plugin{p}})
			require.NoError(t, err)
			defer c.Close()
			w := New(c, "alias-configuration", Options{DisableRegistrationAliasing: !disabled})
			w.RegisterWorkflowWithOptions(order.EchoWorkflow, sdkwf.RegisterOptions{Name: "configured-workflow"})
			w.RegisterActivityWithOptions(testActivity, activity.RegisterOptions{Name: "configured-activity"})
			expectedWorkflow, expectedActivity := "configured-workflow", "configured-activity"
			if disabled {
				expectedWorkflow, expectedActivity = "EchoWorkflow", "testActivity"
			}
			wrapped := w.(*isolateWorker)
			require.Equal(t, expectedWorkflow, wrapped.workflows.resolve("EchoWorkflow"))
			require.Equal(t, expectedActivity, wrapped.activities.resolve("testActivity"))
		})
	}
}
