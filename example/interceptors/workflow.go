// Package interceptors checks native workflow interception and owner isolation.
package interceptors

import (
	"context"
	"fmt"
	"time"

	"github.com/mfateev/sdk-go-poc/interceptor"
	"github.com/mfateev/sdk-go-poc/workflow"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/sdk/converter"
)

const SinkOp uint32 = 0x10100

type contextKey struct{}

var workflowState int
var initialized int

func init() { initialized++ }

//go:isolate
func NewInterceptors(config []byte) ([]interceptor.WorkerInterceptor, error) {
	if string(config) != "copied" {
		return nil, fmt.Errorf("interceptor config changed: %q", config)
	}
	return []interceptor.WorkerInterceptor{&workerInterceptor{name: "A"}, &workerInterceptor{name: "B"}}, nil
}

//go:isolate
func NewConverter(config []byte) (converter.DataConverter, error) {
	if string(config) != "json" {
		return nil, fmt.Errorf("converter config changed: %q", config)
	}
	return converter.NewCompositeDataConverter(converter.NewJSONPayloadConverter()), nil
}

type workerInterceptor struct {
	interceptor.WorkerInterceptorBase
	name string
}

func (w *workerInterceptor) InterceptWorkflow(ctx context.Context, next interceptor.WorkflowInboundInterceptor) interceptor.WorkflowInboundInterceptor {
	info := workflow.GetInfo(ctx)
	if info.WorkflowType.Name != "Workflow" {
		panic("workflow metadata lost")
	}
	return &inbound{WorkflowInboundInterceptorBase: interceptor.WorkflowInboundInterceptorBase{Next: next}, name: w.name}
}

type inbound struct {
	interceptor.WorkflowInboundInterceptorBase
	name  string
	calls int
}

func (i *inbound) emit(event string) { workflow.NewSink(SinkOp).Emit([]byte(i.name + "." + event)) }
func (i *inbound) Init(next interceptor.WorkflowOutboundInterceptor) error {
	return i.Next.Init(&outbound{WorkflowOutboundInterceptorBase: interceptor.WorkflowOutboundInterceptorBase{Next: next}, owner: i})
}
func (i *inbound) ExecuteWorkflow(ctx context.Context, in *interceptor.ExecuteWorkflowInput) (any, error) {
	i.calls++
	i.emit("enter")
	in.Args[0] = in.Args[0].(string) + i.name
	ctx = context.WithValue(ctx, contextKey{}, i.name)
	result, err := i.Next.ExecuteWorkflow(ctx, in)
	i.emit("exit")
	if err != nil {
		return nil, err
	}
	return result.(string) + i.name, nil
}
func (i *inbound) HandleSignal(ctx context.Context, in *interceptor.HandleSignalInput) error {
	if string(interceptor.WorkflowHeader(ctx)["signal"].Data) != "signal-header" {
		panic("signal header lost")
	}
	i.emit("signal")
	if i.name == "A" && in.SignalName == "filtered" {
		return nil
	}
	if in.SignalName == "renamed" {
		in.SignalName = "finish"
	}
	return i.Next.HandleSignal(ctx, in)
}
func (i *inbound) HandleQuery(ctx context.Context, in *interceptor.HandleQueryInput) (any, error) {
	i.calls++ // Must be a scratch-owned counter, not the cached chain's counter.
	if string(interceptor.WorkflowHeader(ctx)["query"].Data) != "query-header" {
		panic("query header lost")
	}
	if in.QueryType == "bad" {
		workflowState++
	} // Must panic without revocation.
	i.emit("query")
	return i.Next.HandleQuery(ctx, in)
}
func (i *inbound) ValidateUpdate(ctx context.Context, in *interceptor.UpdateInput) error {
	i.calls++ // Validation gets a fresh mutable scratch chain, too.
	if string(interceptor.WorkflowHeader(ctx)["update"].Data) != "update-header" {
		panic("validator header lost")
	}
	if in.Name == "bad" {
		workflowState++
	}
	i.emit("validate")
	return i.Next.ValidateUpdate(ctx, in)
}
func (i *inbound) ExecuteUpdate(ctx context.Context, in *interceptor.UpdateInput) (any, error) {
	if i.calls != 1 {
		panic("query/validator changed cached interceptor state")
	}
	if string(interceptor.WorkflowHeader(ctx)["update"].Data) != "update-header" {
		panic("update header lost")
	}
	i.emit("update")
	return i.Next.ExecuteUpdate(ctx, in)
}

type outbound struct {
	interceptor.WorkflowOutboundInterceptorBase
	owner *inbound
}

func (o *outbound) ExecuteActivity(ctx context.Context, name string, args ...any) workflow.Future {
	if name != "ActivityAlias" {
		panic("activity alias missing from interceptor")
	}
	o.owner.emit("activity")
	header := interceptor.WorkflowHeader(ctx)
	header["activity"] = &commonpb.Payload{Data: append(header["activity"].GetData(), o.owner.name...)}
	return o.Next.ExecuteActivity(ctx, name, args...)
}

func Activity(_ context.Context, input string) (string, error) { return input + "-activity", nil }

//go:isolate
func Workflow(ctx context.Context, input string) (string, error) {
	if initialized != 1 || workflowState != 0 {
		panic("package state initialized twice or shared between isolates")
	}
	if ctx.Value(contextKey{}) != "B" {
		panic("interceptor context did not reach workflow")
	}
	if string(interceptor.WorkflowHeader(ctx)["start"].Data) != "start-header" {
		panic("startup header lost")
	}
	workflowState = 7
	for _, name := range []string{"state", "bad"} {
		if err := workflow.SetQueryHandler(ctx, name, func() (int, error) { return workflowState, nil }); err != nil {
			return "", err
		}
	}
	for _, name := range []string{"bump", "bad"} {
		// The inbound validator hook must run even without an application validator.
		if err := workflow.SetUpdateHandler(ctx, name, func(context.Context) error { workflowState++; return nil }); err != nil {
			return "", err
		}
	}
	workflow.GetLogger(ctx).Info("workflow-log", "input", input)
	workflow.GetMetricsHandler(ctx).WithTags(map[string]string{"kind": "workflow"}).Counter("runs").Inc(1)
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{StartToCloseTimeout: time.Second})
	var result string
	if err := workflow.ExecuteActivity(ctx, Activity, input).Get(ctx, &result); err != nil {
		return "", err
	}
	<-workflow.GetSignalChannel(ctx, "finish")
	return result, nil
}

//go:isolate
func Immediate(ctx context.Context, input string) (string, error) { return input, nil }
