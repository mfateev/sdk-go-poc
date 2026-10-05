package signal

import "github.com/mfateev/sdk-go-poc/workflow"

//go:isolate
func SignalWorkflow() ([]byte, error) {
	signal, err := workflow.NextSignal()
	return signal.Input, err
}
