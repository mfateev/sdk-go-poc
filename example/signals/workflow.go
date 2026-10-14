package signals

import (
	"context"
	"fmt"
	"time"

	"github.com/mfateev/sdk-go-poc/workflow"
)

type channelContextKey struct{}

type Message struct {
	ID   int64
	Text string
	Data []byte
}

//temporal:workflow
func Workflow(ctx context.Context, mode string) ([]Message, error) {
	child, cancel := context.WithCancel(ctx)
	if mode == "detached" {
		child = context.WithoutCancel(ctx)
	}
	child = context.WithValue(child, channelContextKey{}, "caller")
	ch := workflow.GetSignalChannelWithOptions[Message](child, "message", workflow.SignalChannelOptions{Description: "Typed messages"})
	if ch != workflow.GetSignalChannel[Message](ctx, "message") || ch != workflow.GetSignalChannelWithOptions[Message](ctx, "message", workflow.SignalChannelOptions{Description: "ignored"}) {
		return nil, fmt.Errorf("typed channel identity changed")
	}
	cancel()
	if mode == "conflict" {
		workflow.GetSignalChannel[string](ctx, "message")
	}
	if mode == "cancel" {
		if _, ok := <-ch; ok {
			return nil, fmt.Errorf("cancellation delivered a value")
		}
		return nil, nil
	}
	var received []Message
	for range 3 {
		value, ok := <-ch
		if !ok {
			return nil, fmt.Errorf("signal channel closed early")
		}
		if len(value.Data) != 1 {
			return nil, fmt.Errorf("signal data lost")
		}
		value.Data[0]++
		received = append(received, value)
	}
	return received, nil
}

// DrainWorkflow checks signal readiness both at registration and after a host
// timer or signal wakes the application. Each run consumes the complete batch
// with ordinary nonblocking selects, as a Continue-As-New workflow would.
//
//temporal:workflow
func DrainWorkflow(ctx context.Context, mode string) ([]int, error) {
	if mode == "late" {
		<-workflow.GetSignalChannel[bool](ctx, "start")
	}
	numbers := workflow.GetSignalChannel[int](ctx, "number")
	other := workflow.GetSignalChannel[int](ctx, "other")
	var result []int
	if mode == "wake" {
		result = append(result, <-numbers)
	}
	if mode == "timer" {
		time.Sleep(time.Second)
	}
	for {
		select {
		case value := <-numbers:
			result = append(result, value)
		default:
			goto otherSignals
		}
	}
otherSignals:
	for {
		select {
		case value := <-other:
			result = append(result, value)
		default:
			return result, nil
		}
	}
}

// ContinueWorkflow carries a fully drained signal batch into the next run.
//
//temporal:workflow
func ContinueWorkflow(ctx context.Context, carried []int) ([]int, error) {
	if len(carried) != 0 {
		return carried, nil
	}
	numbers := workflow.GetSignalChannel[int](ctx, "number")
	var received []int
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case value := <-numbers:
		received = append(received, value)
	}
	for {
		select {
		case value := <-numbers:
			received = append(received, value)
		default:
			return nil, workflow.NewContinueAsNewError(ctx, ContinueWorkflow, received)
		}
	}
}
