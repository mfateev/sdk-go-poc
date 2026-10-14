package signals

import (
	"context"
	"fmt"
	"github.com/mfateev/sdk-go-poc/interceptor"
	"github.com/mfateev/sdk-go-poc/workflow"
)

//temporal:workflow
func NewInterceptors([]byte) ([]interceptor.WorkerInterceptor, error) {
	return []interceptor.WorkerInterceptor{&signalInterceptor{}}, nil
}

type signalInterceptor struct {
	interceptor.WorkerInterceptorBase
}

func (*signalInterceptor) InterceptWorkflow(_ context.Context, next interceptor.WorkflowInboundInterceptor) interceptor.WorkflowInboundInterceptor {
	return &signalInbound{WorkflowInboundInterceptorBase: interceptor.WorkflowInboundInterceptorBase{Next: next}}
}

type signalInbound struct {
	interceptor.WorkflowInboundInterceptorBase
}

func (in *signalInbound) Init(next interceptor.WorkflowOutboundInterceptor) error {
	return in.Next.Init(&signalOutbound{WorkflowOutboundInterceptorBase: interceptor.WorkflowOutboundInterceptorBase{Next: next}})
}
func (in *signalInbound) HandleSignal(ctx context.Context, input *interceptor.HandleSignalInput) error {
	if input.SignalName == "renamed" {
		input.SignalName = "number"
	}
	if input.SignalName == "filtered" {
		return nil
	}
	return in.Next.HandleSignal(ctx, input)
}

type signalOutbound struct {
	interceptor.WorkflowOutboundInterceptorBase
}

func (out *signalOutbound) GetSignalChannel(ctx context.Context, name string) <-chan workflow.SignalResult {
	return forwardSignals(ctx, out.Next.GetSignalChannel(ctx, name))
}
func (out *signalOutbound) GetSignalChannelWithOptions(ctx context.Context, name string, options workflow.SignalChannelOptions) <-chan workflow.SignalResult {
	if name == "message" && ctx.Value(channelContextKey{}) != "caller" {
		panic(fmt.Sprintf("signal registration lost caller values: %v", ctx.Value(channelContextKey{})))
	}
	return forwardSignals(ctx, out.Next.GetSignalChannelWithOptions(ctx, name, options))
}
func forwardSignals(ctx context.Context, source <-chan workflow.SignalResult) <-chan workflow.SignalResult {
	target := make(chan workflow.SignalResult)
	// An ordinary goroutine installed by an outbound hook must participate in
	// proactive admission too; otherwise the application could drain too early.
	go func() {
		defer close(target)
		for {
			select {
			case <-ctx.Done():
				return
			case result, ok := <-source:
				if !ok {
					return
				}
				select {
				case target <- result:
				case <-ctx.Done():
					return
				}
			}
		}
	}()
	return target
}
