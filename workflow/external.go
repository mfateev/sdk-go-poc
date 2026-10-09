package workflow

import (
	"context"
	"encoding/json"
	"errors"
	"isolate"
)

// ExternalRequest identifies a signal or cancellation of an external execution.
type ExternalRequest struct {
	ID                                       uint64
	Namespace, WorkflowID, RunID, SignalName string
	Cancel                                   bool
	Payloads                                 []byte
}

// SignalExternalWorkflow follows the SDK target/run and acknowledgment contract.
func SignalExternalWorkflow(ctx context.Context, workflowID, runID, signalName string, arg any) Future {
	return externalWorkflow(ctx, workflowID, runID, signalName, arg, false)
}

func RequestCancelExternalWorkflow(ctx context.Context, workflowID, runID string) Future {
	return externalWorkflow(ctx, workflowID, runID, "", nil, true)
}

func externalWorkflow(ctx context.Context, workflowID, runID, name string, arg any, cancel bool) Future {
	assertWritable()
	f := newOperationFuture()
	if ctx == nil {
		return failOperation(f, errors.New("workflow: nil context"))
	}
	// SDK external commands are not canceled by their context.
	if workflowID == "" || (!cancel && name == "") {
		return failOperation(f, errors.New("workflow: empty external workflow ID or signal name"))
	}
	r := ExternalRequest{ID: nextCallID.Add(1), Namespace: runOptions(ctx).Namespace, WorkflowID: workflowID, RunID: runID, SignalName: name, Cancel: cancel}
	if !cancel {
		var err error
		r.Payloads, err = encodeActivityArgs([]any{arg})
		if err != nil {
			return failOperation(f, err)
		}
	}
	p, err := json.Marshal(r)
	if err != nil {
		return failOperation(f, err)
	}
	if _, err = isolate.Call(OpScheduleExternal, p); err != nil {
		return failOperation(f, err)
	}
	go awaitOperation(f, OpAwaitExternal, r.ID, ctx)
	return f
}

func newOperationFuture() *activityFuture { return &activityFuture{done: make(chan struct{})} }
func failOperation(f *activityFuture, err error) *activityFuture {
	f.err = err
	f.ready.Store(true)
	close(f.done)
	return f
}
func awaitOperation(f *activityFuture, op uint32, id uint64, ctx context.Context) {
	p, _ := json.Marshal(id)
	r, err := isolate.Call(op, p)
	completeOperation(f, r, err, ctx)
}
