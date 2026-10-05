package signal

import (
	"context"
	"github.com/mfateev/sdk-go-poc/workflow"
)

//go:isolate
func SignalWorkflow(ctx context.Context) ([]byte, error) {
	signal, err := workflow.NextSignal(ctx)
	return signal.Input, err
}
