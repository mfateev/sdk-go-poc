package workflow

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"isolate"
	"maps"
	"reflect"

	"github.com/mfateev/sdk-go-poc/internal/failurecodec"
	"go.temporal.io/sdk/temporal"
	goWorkflow "go.temporal.io/sdk/workflow"
)

type ChildWorkflowOptions = goWorkflow.ChildWorkflowOptions
type Execution = goWorkflow.Execution

type childOptionsKey struct{}

func cloneChildOptions(o ChildWorkflowOptions) ChildWorkflowOptions {
	o.RetryPolicy = cloneActivityOptions(ActivityOptions{RetryPolicy: o.RetryPolicy}).RetryPolicy
	o.Memo, o.SearchAttributes = maps.Clone(o.Memo), maps.Clone(o.SearchAttributes)
	return o
}

func WithChildWorkflowOptions(ctx context.Context, o ChildWorkflowOptions) context.Context {
	r := runOptions(ctx)
	if o.Namespace != "" {
		r.Namespace = o.Namespace
	}
	if o.TaskQueue != "" {
		r.TaskQueue = o.TaskQueue
	}
	r.WorkflowID = o.WorkflowID
	r.WorkflowExecutionTimeout, r.WorkflowRunTimeout, r.WorkflowTaskTimeout = o.WorkflowExecutionTimeout, o.WorkflowRunTimeout, o.WorkflowTaskTimeout
	r.VersioningIntent, r.Priority = o.VersioningIntent, o.Priority
	ctx = context.WithValue(ctx, runOptionsKey{}, r)
	return context.WithValue(ctx, childOptionsKey{}, cloneChildOptions(o))
}

func GetChildWorkflowOptions(ctx context.Context) ChildWorkflowOptions {
	o, _ := ctx.Value(childOptionsKey{}).(ChildWorkflowOptions)
	r := runOptions(ctx)
	o.Namespace, o.TaskQueue, o.WorkflowID = r.Namespace, r.TaskQueue, r.WorkflowID
	o.WorkflowExecutionTimeout, o.WorkflowRunTimeout, o.WorkflowTaskTimeout = r.WorkflowExecutionTimeout, r.WorkflowRunTimeout, r.WorkflowTaskTimeout
	o.VersioningIntent, o.Priority = r.VersioningIntent, r.Priority
	return cloneChildOptions(o)
}

type ChildWorkflowFuture interface {
	Future
	GetChildWorkflowExecution() Future
	SignalChildWorkflow(ctx context.Context, signalName string, data any) Future
}

type childFuture struct {
	*activityFuture
	execution *activityFuture
}

func (f *childFuture) GetChildWorkflowExecution() Future { return f.execution }

// ChildRequest carries copied options and individually encoded memo/search
// values. Decoding JSON into interface{} on the host would round large integers.
type ChildRequest struct {
	ID                     uint64
	Name                   string
	Function               bool
	Payloads               []byte
	Options                ChildWorkflowOptions
	Memo, SearchAttributes map[string][]byte
}

type ChildSignalRequest struct {
	ID                          uint64
	Namespace, WorkflowID, Name string
	Payloads                    []byte
}

func completeOperation(f *activityFuture, response []byte, err error, ctx context.Context) {
	f.err = err
	if err == nil && len(response) != 0 {
		var outcome ActivityOutcome
		f.err = json.Unmarshal(response, &outcome)
		if f.err == nil {
			f.payload = outcome.Payloads
			switch outcome.ErrorKind {
			case "child-already-started":
				f.err = &temporal.ChildWorkflowExecutionAlreadyStartedError{}
			case "namespace-not-found":
				f.err = &temporal.NamespaceNotFoundError{}
			case "":
			default:
				f.err = errors.New("workflow: unknown child operation error kind")
			}
			if f.err == nil && len(outcome.Failure) != 0 {
				var transportErr error
				f.err, transportErr = failurecodec.Decode(outcome.Failure, instanceDataConverter)
				if transportErr != nil {
					f.err = fmt.Errorf("workflow: decode operation failure: %w", transportErr)
				}
				if outcome.Canceled && ctx.Err() != nil {
					f.err = failurecodec.WithCancellation(f.err, ctx.Err())
				}
			} else if f.err == nil && outcome.Failed {
				f.err = errors.New(outcome.Error)
			}
		}
	}
	f.ready.Store(true)
	close(f.done)
}

func encodeOptionValues(values map[string]any) (map[string][]byte, error) {
	if values == nil {
		return nil, nil
	}
	result := make(map[string][]byte, len(values))
	for key, value := range values {
		raw, err := encodeActivityArgs([]any{value})
		if err != nil {
			return nil, err
		}
		result[key] = raw
	}
	return result, nil
}

// ExecuteChildWorkflow schedules before returning. The pinned host SDK owns
// child IDs, retries, parent-close policy and cancellation completion semantics.
func ExecuteChildWorkflow(ctx context.Context, fn any, args ...any) ChildWorkflowFuture {
	assertWritable()
	f := &childFuture{activityFuture: &activityFuture{done: make(chan struct{})}, execution: &activityFuture{done: make(chan struct{})}}
	fail := func(err error) ChildWorkflowFuture {
		completeOperation(f.activityFuture, nil, err, context.Background())
		completeOperation(f.execution, nil, err, context.Background())
		return f
	}
	if ctx == nil {
		return fail(errors.New("workflow: nil context"))
	}
	if err := ctx.Err(); err != nil {
		return fail(err)
	}
	name, function, err := validateWorkflowReference(fn, args)
	if err != nil {
		return fail(err)
	}
	if function {
		typ := reflect.TypeOf(fn)
		if typ.IsVariadic() || typ.NumOut() < 1 || typ.NumOut() > 2 || !typ.Out(typ.NumOut()-1).Implements(reflect.TypeFor[error]()) {
			return fail(errors.New("workflow: child reference must return error or (result, error) and cannot be variadic"))
		}
	}
	o := GetChildWorkflowOptions(ctx)
	if o.WorkflowExecutionTimeout < 0 || o.WorkflowRunTimeout < 0 || o.WorkflowTaskTimeout < 0 {
		return fail(errors.New("workflow: negative child workflow timeout"))
	}
	// Interface-key map iteration needs a separate deterministic-key audit.
	// Fail explicitly rather than silently dropping typed visibility attributes.
	// Size itself iterates the SDK's interface-key map, including when nil.
	// Inspect only its length using the pinned SDK layout, without visiting keys.
	attributes := reflect.ValueOf(o.TypedSearchAttributes)
	if attributes.NumField() != 1 || attributes.Type().Field(0).Name != "untypedValue" || attributes.Field(0).Kind() != reflect.Map {
		return fail(errors.New("workflow: unexpected pinned SDK search attribute layout"))
	}
	if attributes.Field(0).Len() != 0 {
		return fail(errors.New("workflow: typed child search attributes are outside the isolate POC subset"))
	}
	payloads, err := encodeActivityArgs(args)
	if err != nil {
		panic(err)
	}
	memo, err := encodeOptionValues(o.Memo)
	if err != nil {
		return fail(err)
	}
	search, err := encodeOptionValues(o.SearchAttributes)
	if err != nil {
		return fail(err)
	}
	o.Memo, o.SearchAttributes = nil, nil
	id := nextCallID.Add(1)
	raw, err := json.Marshal(ChildRequest{ID: id, Name: name, Function: function, Payloads: payloads, Options: o, Memo: memo, SearchAttributes: search})
	if err != nil {
		return fail(err)
	}
	if _, err := isolate.Call(OpScheduleChild, raw); err != nil {
		return fail(err)
	}
	stop := context.AfterFunc(ctx, func() { p, _ := json.Marshal(id); _, _ = isolate.Call(OpCancelChild, p) })
	go func() {
		defer stop()
		p, _ := json.Marshal(id)
		r, err := isolate.Call(OpAwaitChild, p)
		completeOperation(f.activityFuture, r, err, ctx)
	}()
	go func() {
		p, _ := json.Marshal(id)
		r, err := isolate.Call(OpAwaitChildExecution, p)
		completeOperation(f.execution, r, err, ctx)
	}()
	return f
}

func (f *childFuture) SignalChildWorkflow(ctx context.Context, name string, data any) Future {
	assertWritable()
	var execution Execution
	if err := f.execution.Get(ctx, &execution); err != nil {
		return f.execution
	}
	result := &activityFuture{done: make(chan struct{})}
	fail := func(err error) Future { completeOperation(result, nil, err, context.Background()); return result }
	if ctx == nil {
		return fail(errors.New("workflow: nil context"))
	}
	payloads, err := encodeActivityArgs([]any{data})
	if err != nil {
		return fail(err)
	}
	id := nextCallID.Add(1)
	raw, err := json.Marshal(ChildSignalRequest{ID: id, Namespace: runOptions(ctx).Namespace, WorkflowID: execution.ID, Name: name, Payloads: payloads})
	if err != nil {
		return fail(err)
	}
	if _, err := isolate.Call(OpSignalChild, raw); err != nil {
		return fail(err)
	}
	go func() {
		p, _ := json.Marshal(id)
		r, err := isolate.Call(OpAwaitChildSignal, p)
		completeOperation(result, r, err, ctx)
	}()
	return result
}
