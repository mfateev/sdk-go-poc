package main

import (
	"errors"
	"time"

	"github.com/mfateev/sdk-go-poc/workflow"
)

func init() {
	workflow.Register("IsolateOrder", OrderWorkflow)
	workflow.Register("IsolateEcho", EchoWorkflow)
	workflow.RegisterTyped2("IsolateTypedEcho", TypedEchoWorkflow)
}

func main() {
	if err := workflow.Run(); err != nil {
		panic(err)
	}
}

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

func EchoWorkflow(input []byte) ([]byte, error) {
	return append([]byte("registered:"), input...), nil
}

type TypedEchoRequest struct {
	Name string
}

type TypedEchoResult struct {
	Message string
}

func TypedEchoWorkflow(request TypedEchoRequest, suffix string) (TypedEchoResult, error) {
	return TypedEchoResult{Message: "hello " + request.Name + suffix}, nil
}
