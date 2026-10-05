package workflow

import (
	"context"
	"encoding/json"
	"errors"
	"isolate"
	"sync/atomic"
)

// CallRequest is a cancellable host operation. IDs are isolate-local and only
// serialized bytes cross the boundary, never a Go context or cancel function.
type CallRequest struct {
	ID      uint64 `json:"id"`
	Op      uint32 `json:"op"`
	Payload []byte `json:"payload"`
}

var nextCallID atomic.Uint64

func executionContext(canceled bool) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancel(context.Background())
	if canceled {
		cancel()
		return ctx, cancel
	}
	go func() {
		_, err := isolate.Call(OpWorkflowCancel, nil)
		if err == nil {
			cancel()
		}
	}()
	return ctx, cancel
}

func call(ctx context.Context, op uint32, payload []byte) ([]byte, error) {
	if ctx == nil {
		return nil, errors.New("workflow: nil context")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	id := nextCallID.Add(1)
	request, err := json.Marshal(CallRequest{ID: id, Op: op, Payload: payload})
	if err != nil {
		return nil, err
	}
	stop := context.AfterFunc(ctx, func() {
		cancelRequest, _ := json.Marshal(id)
		_, _ = isolate.Call(OpCancelCall, cancelRequest)
	})
	defer stop()
	result, err := isolate.Call(OpCancellableCall, request)
	// Cancellation and completion can be ready together. Favor cancellation
	// consistently; preserve standard error identity across the byte boundary.
	if cause := ctx.Err(); cause != nil {
		return nil, cause
	}
	return result, err
}
