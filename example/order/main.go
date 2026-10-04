package main

import (
	"errors"
	"time"

	"github.com/mfateev/sdk-go-poc/workflow"
)

func init() {
	workflow.Register("IsolateOrder", OrderWorkflow)
	workflow.Register("IsolateEcho", EchoWorkflow)
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
