package workflow

import (
	"context"
	"errors"
	"fmt"
	"github.com/mfateev/sdk-go-poc/internal/activityref"
	"github.com/mfateev/sdk-go-poc/internal/headerwire"
	commonpb "go.temporal.io/api/common/v1"
	goWorkflow "go.temporal.io/sdk/workflow"
	"google.golang.org/protobuf/proto"
	"isolate"
	"reflect"
)

type ContinueAsNewError = goWorkflow.ContinueAsNewError
type ContinueAsNewErrorOptions = goWorkflow.ContinueAsNewErrorOptions
type ContinueAsNewVersioningBehavior = goWorkflow.ContinueAsNewVersioningBehavior

const (
	ContinueAsNewVersioningBehaviorUnspecified       = goWorkflow.ContinueAsNewVersioningBehaviorUnspecified
	ContinueAsNewVersioningBehaviorAutoUpgrade       = goWorkflow.ContinueAsNewVersioningBehaviorAutoUpgrade
	ContinueAsNewVersioningBehaviorUseRampingVersion = goWorkflow.ContinueAsNewVersioningBehaviorUseRampingVersion
)

// validateWorkflowReference follows SDK name/argument validation. Both marked
// native-context functions and ordinary SDK workflow references are supported;
// references identify host registrations, and never invoke the function here.
func validateWorkflowReference(fn any, args []any) (string, bool, error) {
	if name, ok := fn.(string); ok {
		if name == "" {
			return "", false, errors.New("workflow: empty workflow name")
		}
		return name, false, nil
	}
	name, err := activityref.Name(fn)
	if err != nil {
		return "", false, err
	}
	typ := reflect.TypeOf(fn)
	if typ.NumIn() == 0 || (typ.In(0) != reflect.TypeFor[context.Context]() && typ.In(0) != reflect.TypeFor[goWorkflow.Context]()) {
		return "", false, errors.New("workflow: workflow reference requires context first")
	}
	if len(args) != typ.NumIn()-1 {
		return "", false, fmt.Errorf("workflow: got %d workflow arguments, want %d", len(args), typ.NumIn()-1)
	}
	for i, arg := range args {
		if arg != nil && !reflect.TypeOf(arg).AssignableTo(typ.In(i+1)) {
			return "", false, fmt.Errorf("workflow: argument %d is %T, want %v", i, arg, typ.In(i+1))
		}
	}
	return name, true, nil
}

// NewContinueAsNewError prepares a fresh run using the current SDK's error type.
// Return this error from the workflow; construction emits no history command.
func newContinueAsNewError(ctx context.Context, fn any, args ...any) error {
	name, function, err := validateWorkflowReference(fn, args)
	if err != nil {
		panic(err)
	}
	raw, err := encodeContextArgs(ctx, args)
	if err != nil {
		panic(err)
	}
	input, err := decodePayloads(raw, GetDataConverter(ctx))
	if err != nil {
		panic(err)
	}
	o := runOptions(ctx)
	e := &ContinueAsNewError{WorkflowType: &goWorkflow.Type{Name: name}, Input: input,
		TaskQueueName: o.TaskQueue, WorkflowExecutionTimeout: o.WorkflowExecutionTimeout,
		WorkflowRunTimeout: o.WorkflowRunTimeout, WorkflowTaskTimeout: o.WorkflowTaskTimeout, VersioningIntent: o.VersioningIntent}
	fields, err := outgoingHeader(ctx)
	if err != nil {
		panic(err)
	}
	e.Header, err = headerwire.Decode(fields)
	if err != nil {
		panic(err)
	}
	if function {
		resolved, err := isolate.Call(OpResolveWorkflowName, []byte(name))
		if err != nil {
			panic(err)
		}
		e.WorkflowType.Name = string(resolved)
	}
	return e
}
func NewContinueAsNewErrorWithOptions(ctx context.Context, options ContinueAsNewErrorOptions, fn any, args ...any) error {
	e := NewContinueAsNewError(ctx, fn, args...).(*ContinueAsNewError)
	if options.RetryPolicy != nil {
		e.RetryPolicy = cloneActivityOptions(ActivityOptions{RetryPolicy: options.RetryPolicy}).RetryPolicy
	}
	e.BackoffStartInterval = options.BackoffStartInterval
	e.InitialVersioningBehavior = options.InitialVersioningBehavior
	return e
}
func IsContinueAsNewError(err error) bool { var e *ContinueAsNewError; return errors.As(err, &e) }

// ContinueAsNewRequest carries only copied configuration and encoded payloads.
type ContinueAsNewRequest struct {
	Name                      string                          `json:"name"`
	Payloads                  *commonpb.Payloads              `json:"payloads"`
	Header                    *commonpb.Header                `json:"header,omitempty"`
	Options                   RunOptions                      `json:"options"`
	RetryPolicy               *RetryPolicy                    `json:"retry_policy,omitempty"`
	BackoffStartInterval      int64                           `json:"backoff_start_interval,omitempty"`
	InitialVersioningBehavior ContinueAsNewVersioningBehavior `json:"initial_versioning_behavior,omitempty"`
}

func continuationRequest(cause error) (*ContinueAsNewRequest, error) {
	var e *ContinueAsNewError
	if !errors.As(cause, &e) {
		return nil, nil
	}
	if e.WorkflowType == nil || e.WorkflowType.Name == "" {
		return nil, errors.New("workflow: continue-as-new requires a workflow type")
	}
	r := &ContinueAsNewRequest{Name: e.WorkflowType.Name, Payloads: e.Input,
		Options: RunOptions{TaskQueue: e.TaskQueueName, WorkflowExecutionTimeout: e.WorkflowExecutionTimeout,
			WorkflowRunTimeout: e.WorkflowRunTimeout, WorkflowTaskTimeout: e.WorkflowTaskTimeout, VersioningIntent: e.VersioningIntent},
		RetryPolicy: e.RetryPolicy, BackoffStartInterval: int64(e.BackoffStartInterval), InitialVersioningBehavior: e.InitialVersioningBehavior}
	if e.Header != nil {
		r.Header = proto.Clone(e.Header).(*commonpb.Header)
	}
	return r, nil
}
