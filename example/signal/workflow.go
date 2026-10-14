package signal

import (
	"context"
	"github.com/mfateev/sdk-go-poc/workflow"
)

//temporal:workflow
func SignalWorkflow(ctx context.Context) ([]byte, error) {
	signal, err := workflow.NextSignal(ctx)
	return signal.Input, err
}
