package main

import "github.com/mfateev/sdk-go-poc/workflow"

func main() {
	_, err := workflow.Input()
	if err != nil {
		_ = workflow.Complete(nil, err)
		return
	}
	signal, err := workflow.NextSignal()
	_ = workflow.Complete(signal.Input, err)
}
