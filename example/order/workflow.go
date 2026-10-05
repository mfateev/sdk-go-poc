package order

import (
	"errors"
	"time"

	"github.com/mfateev/sdk-go-poc/workflow"
)

//go:isolate
func OrderWorkflow(input []byte) ([]byte, error) {
	var err error
	input, err = workflow.ExecuteActivity("echo", input, time.Minute)
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
