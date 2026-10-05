package order

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/mfateev/sdk-go-poc/workflow"
)

//go:isolate
func OrderWorkflow(ctx context.Context, input []byte) ([]byte, error) {
	var err error
	input, err = workflow.ExecuteActivityByName[[]byte](ctx, "echo", time.Minute, input)
	if err == nil {
		started := time.Now()
		if err := workflow.Sleep(ctx, time.Second); err != nil {
			return nil, err
		}
		if time.Since(started) < time.Second {
			err = errors.New("host clock did not advance for durable timer")
		}
	}
	return input, err
}

//go:isolate
func EchoWorkflow(ctx context.Context, input []byte) ([]byte, error) {
	return append([]byte("registered:"), input...), nil
}

type TypedEchoRequest struct {
	Name string
}

type TypedEchoResult struct {
	Message string
}

//go:isolate
func TypedEchoWorkflow(ctx context.Context, request TypedEchoRequest, suffix string) (TypedEchoResult, error) {
	return TypedEchoResult{Message: "hello " + request.Name + suffix}, nil
}

type ActivityDetails struct {
	Message string
	Count   int
}

//go:isolate
func TypedActivityWorkflow(ctx context.Context, name string) (ActivityDetails, error) {
	details, err := workflow.ExecuteActivityByName[ActivityDetails](ctx, "details", time.Minute, name, 3)
	if err != nil {
		return ActivityDetails{}, err
	}
	result := <-workflow.ExecuteActivityAsync(ctx, ActivityLength, time.Minute, details.Message)
	if result.Err != nil {
		return ActivityDetails{}, result.Err
	}
	if result.Result != len(details.Message) {
		return ActivityDetails{}, errors.New("incorrect typed activity length")
	}
	return details, nil
}

// ActivityLength runs in the host. Its signature supplies the async result type.
func ActivityLength(_ context.Context, value string) (int, error) { return len(value), nil }

//go:isolate
func InferredActivityWorkflow(ctx context.Context, input int) (string, error) {
	return workflow.ExecuteActivity(ctx, FormatNumber, time.Minute, input)
}

// FormatNumber is host-only activity code; a reference never executes it here.
func FormatNumber(_ context.Context, input int) (string, error) {
	return fmt.Sprintf("number:%d", input), nil
}
