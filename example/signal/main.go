package main

import "github.com/mfateev/sdk-go-poc/workflow"

func init() { workflow.Register("IsolateSignal", SignalWorkflow) }

func main() {
	if err := workflow.Run(); err != nil {
		panic(err)
	}
}

func SignalWorkflow(_ []byte) ([]byte, error) {
	signal, err := workflow.NextSignal()
	return signal.Input, err
}
