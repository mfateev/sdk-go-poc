package workflow

import (
	"context"
	"errors"
	commonpb "go.temporal.io/api/common/v1"
	"isolate"
)

// ExternalRequest identifies a signal or cancellation of an external execution.
type ExternalRequest struct {
	Header                                   *commonpb.Header
	ID                                       uint64
	Namespace, WorkflowID, RunID, SignalName string
	Cancel                                   bool
	Payloads                                 *commonpb.Payloads
}

// SignalExternalWorkflow follows the SDK target/run and acknowledgment contract.
func signalExternalWorkflow(ctx context.Context, workflowID, runID, signalName string, arg any) Future {
	return externalWorkflow(ctx, workflowID, runID, signalName, arg, false)
}

func requestCancelExternalWorkflow(ctx context.Context, workflowID, runID string) Future {
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
	f.converterContext = ctx
	f.dataConverter = GetDataConverter(ctx)
	r := ExternalRequest{ID: nextCallID.Add(1), Namespace: runOptions(ctx).Namespace, WorkflowID: workflowID, RunID: runID, SignalName: name, Cancel: cancel}
	var headerErr error
	r.Header, headerErr = outgoingHeader(ctx)
	if headerErr != nil {
		return failOperation(f, headerErr)
	}
	if !cancel {
		var err error
		r.Payloads, err = encodeArgsWithConverter([]any{arg}, f.dataConverter)
		if err != nil {
			return failOperation(f, err)
		}
	}
	p, err := MarshalTransport(r)
	if err != nil {
		return failOperation(f, err)
	}
	if _, err = isolate.Call(OpScheduleExternal, p); err != nil {
		return failOperation(f, err)
	}
	isolate.GoInbound(func() { awaitOperation(f, OpAwaitExternal, r.ID, ctx) })
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
	p := encodeID(id)
	r, err := isolate.Call(op, p)
	completeOperation(f, r, err, ctx)
}
