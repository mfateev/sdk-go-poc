package order

import (
	"errors"
	"time"

	"github.com/mfateev/sdk-go-poc/workflow"
)

//go:isolate
func OrderWorkflow(input []byte) ([]byte, error) {
	var err error
	input, err = workflow.ExecuteActivity[[]byte]("echo", time.Minute, input)
	if err == nil {
		started := time.Now()
		time.Sleep(time.Second)
		if time.Since(started) < time.Second {
			err = errors.New("host clock did not advance for durable timer")
		}
	}
	return input, err
}

//go:isolate
func EchoWorkflow(input []byte) ([]byte, error) {
	return append([]byte("registered:"), input...), nil
}

type TypedEchoRequest struct {
	Name string
}

type TypedEchoResult struct {
	Message string
}

//go:isolate
func TypedEchoWorkflow(request TypedEchoRequest, suffix string) (TypedEchoResult, error) {
	return TypedEchoResult{Message: "hello " + request.Name + suffix}, nil
}

type ActivityDetails struct {
	Message string
	Count   int
}

//go:isolate
func TypedActivityWorkflow(name string) (ActivityDetails, error) {
	details, err := workflow.ExecuteActivity[ActivityDetails]("details", time.Minute, name, 3)
	if err != nil {
		return ActivityDetails{}, err
	}
	result := <-workflow.ExecuteActivityAsync[int]("length", time.Minute, details.Message)
	if result.Err != nil {
		return ActivityDetails{}, result.Err
	}
	if result.Result != len(details.Message) {
		return ActivityDetails{}, errors.New("incorrect typed activity length")
	}
	return details, nil
}
