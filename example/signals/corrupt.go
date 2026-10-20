package signals

import (
	"context"
	"fmt"
	"strings"

	"github.com/mfateev/sdk-go-poc/interceptor"
	"github.com/mfateev/sdk-go-poc/workflow"
	commonpb "go.temporal.io/api/common/v1"
)

// CorruptWorkflow ports the upstream corrupted-signal receive cases to native
// blocking receives and selects. End-of-stream cases use a closed raw adapter,
// because application code cannot close a receive-only SDK signal channel.
//
//temporal:workflow
func CorruptWorkflow(ctx context.Context, mode string) ([]Message, error) {
	if mode == "next" {
		signal, err := workflow.NextSignal(ctx)
		if err != nil {
			return nil, err
		}
		if signal.Name != "bytes" || string(signal.Input) != "valid" {
			return nil, fmt.Errorf("invalid next signal: %+v", signal)
		}
		return []Message{{Text: "valid"}}, nil
	}
	if mode == "late" {
		<-workflow.GetSignalChannel[bool](ctx, "start")
	}
	ch := workflow.GetSignalChannel[Message](ctx, "message")
	switch mode {
	case "receive", "late":
		value, ok := <-ch
		if !ok {
			return nil, fmt.Errorf("signal closed early")
		}
		return []Message{value}, nil
	case "select":
		select {
		case value, ok := <-ch:
			if !ok {
				return nil, fmt.Errorf("signal closed early")
			}
			return []Message{value}, nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	case "async":
		var result []Message
		for {
			select {
			case value := <-ch:
				result = append(result, value)
			default:
				return result, nil
			}
		}
	case "closed-receive":
		value, ok := <-ch
		if ok || (value.ID != 0 || value.Text != "" || value.Data != nil) {
			return nil, fmt.Errorf("closed receive delivered %v, %t", value, ok)
		}
		return nil, nil
	case "closed-async", "closed-wire":
		select {
		case value, ok := <-ch:
			if ok || value.ID != 0 || value.Text != "" || value.Data != nil {
				return nil, fmt.Errorf("closed async delivered %v, %t", value, ok)
			}
		default:
			return nil, fmt.Errorf("closed channel was not ready")
		}
		return nil, nil
	default:
		return nil, fmt.Errorf("unknown corruption mode %q", mode)
	}
}

//temporal:workflow
func NewClosedSignalInterceptors(config []byte) ([]interceptor.WorkerInterceptor, error) {
	return []interceptor.WorkerInterceptor{&closedSignalInterceptor{wire: strings.HasSuffix(string(config), "wire")}}, nil
}

type closedSignalInterceptor struct {
	interceptor.WorkerInterceptorBase
	wire bool
}

func (in *closedSignalInterceptor) InterceptWorkflow(_ context.Context, next interceptor.WorkflowInboundInterceptor) interceptor.WorkflowInboundInterceptor {
	return &closedSignalInbound{WorkflowInboundInterceptorBase: interceptor.WorkflowInboundInterceptorBase{Next: next}, wire: in.wire}
}

type closedSignalInbound struct {
	interceptor.WorkflowInboundInterceptorBase
	wire bool
}

func (in *closedSignalInbound) Init(next interceptor.WorkflowOutboundInterceptor) error {
	return in.Next.Init(&closedSignalOutbound{WorkflowOutboundInterceptorBase: interceptor.WorkflowOutboundInterceptorBase{Next: next}, wire: in.wire})
}

type closedSignalOutbound struct {
	interceptor.WorkflowOutboundInterceptorBase
	wire bool
}

func (out *closedSignalOutbound) GetSignalChannel(ctx context.Context, name string) <-chan workflow.SignalResult {
	payloads := &commonpb.Payloads{Payloads: []*commonpb.Payload{nil}}
	if !out.wire {
		var err error
		payloads, err = workflow.GetDataConverter(ctx).ToPayloads("wrong")
		if err != nil {
			panic(err)
		}
	}
	ch := make(chan workflow.SignalResult, 1)
	ch <- workflow.SignalResult{Signal: workflow.Signal{Name: name, Payloads: payloads}}
	close(ch)
	return ch
}
