package probes

import (
 "context"
 "errors"
 "fmt"
 "testing"

 "github.com/mfateev/sdk-go-poc/example/order"
 pocworker "github.com/mfateev/sdk-go-poc/worker"
 "go.temporal.io/sdk/client"
 "go.temporal.io/sdk/worker"
 "go.temporal.io/sdk/workflow"
 historypb "go.temporal.io/api/history/v1"
)

var stopped = errors.New("audit: stop before polling")
type plugin struct {
 worker.PluginBase
 observed string
 registerOnStart bool
 registerOnReplay bool
 mutatePolicy bool
}
func (*plugin) Name() string { return "compatibility-audit" }
func (p *plugin) ConfigureWorker(_ context.Context, o worker.PluginConfigureWorkerOptions) error {
 if p.mutatePolicy { o.WorkerOptions.WorkflowPanicPolicy = worker.FailWorkflow }
 o.WorkerRegistryOptions.OnRegisterWorkflow = func(fn any, _ workflow.RegisterOptions) { p.observed = fmt.Sprintf("%T", fn) }
 return nil
}
func (p *plugin) StartWorker(_ context.Context, o worker.PluginStartWorkerOptions, _ func(context.Context, worker.PluginStartWorkerOptions) error) error {
 if p.registerOnStart { o.WorkerRegistry.RegisterWorkflowWithOptions(order.EchoWorkflow, workflow.RegisterOptions{Name:"audit"}) }
 return stopped
}
func (p *plugin) ReplayWorkflow(_ context.Context, o worker.PluginReplayWorkflowOptions, _ func(context.Context, worker.PluginReplayWorkflowOptions) error) error {
 if p.registerOnReplay { o.WorkflowReplayRegistry.RegisterWorkflowWithOptions(order.EchoWorkflow, workflow.RegisterOptions{Name:"audit"}) }
 return stopped
}
func TestPluginRegistrationControl(t *testing.T) {
 p:= &plugin{}
 c,err:=client.NewLazyClient(client.Options{}); if err!=nil {t.Fatal(err)}; defer c.Close()
 w:=pocworker.New(c,"audit",pocworker.Options{Plugins:[]worker.Plugin{p}})
 w.RegisterWorkflow(order.EchoWorkflow)
 t.Logf("direct wrapper registered %s",p.observed)
 if p.observed=="func(context.Context, []uint8) ([]uint8, error)" {t.Fatal("native wrapper did not adapt function")}
}
func TestPluginStartNativeRegistration(t *testing.T) {
 p:= &plugin{registerOnStart:true}
 c,err:=client.NewLazyClient(client.Options{}); if err!=nil {t.Fatal(err)}; defer c.Close()
 w:=pocworker.New(c,"audit",pocworker.Options{Plugins:[]worker.Plugin{p}})
 if err:=w.Start(); !errors.Is(err,stopped) {t.Fatalf("start returned %v",err)}
}
func TestPluginReplayNativeRegistration(t *testing.T) {
 p:= &plugin{registerOnReplay:true}
 r,err:=pocworker.NewWorkflowReplayerWithOptions(pocworker.WorkflowReplayerOptions{Plugins:[]worker.Plugin{p}}); if err!=nil {t.Fatal(err)}
 if err:=r.ReplayWorkflowHistory(nil,&historypb.History{}); !errors.Is(err,stopped) {t.Fatalf("replay returned %v",err)}
}
func TestPluginPanicPolicyMutation(t *testing.T) {
 p:= &plugin{mutatePolicy:true}
 c,err:=client.NewLazyClient(client.Options{}); if err!=nil {t.Fatal(err)}; defer c.Close()
 w:=pocworker.New(c,"audit",pocworker.Options{Plugins:[]worker.Plugin{p}})
 defer func(){ if recover()==nil {t.Error("native registration accepted plugin-mutated FailWorkflow although wrapper rejects explicit FailWorkflow")} }()
 w.RegisterWorkflow(order.EchoWorkflow)
}

func TestActivityContextOptionalBaselineComparison(t *testing.T) {
 c,err:=client.NewLazyClient(client.Options{});if err!=nil {t.Fatal(err)};defer c.Close()
 ordinary:=func() error{return nil}
 host:=worker.New(c,"audit-host",worker.Options{})
 host.RegisterActivity(ordinary)
 t.Log("pinned host accepts activity without context")
 native:=pocworker.New(c,"audit-native",pocworker.Options{})
 defer func(){ v:=recover();if v==nil {t.Fatal("expected documented POC rejection")};t.Logf("POC rejects same activity: %v",v) }()
 native.RegisterActivity(ordinary)
}
