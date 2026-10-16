package worker

import (
	"context"
	"errors"
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
