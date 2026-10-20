package workflow

import (
	"context"
	"errors"
	"fmt"
	commonpb "go.temporal.io/api/common/v1"
	failurepb "go.temporal.io/api/failure/v1"
	"isolate"
	"reflect"
	"slices"
	"sync/atomic"
	"time"

	"github.com/mfateev/sdk-go-poc/internal/activityref"
	"github.com/mfateev/sdk-go-poc/internal/failurecodec"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/temporal"
	goWorkflow "go.temporal.io/sdk/workflow"
)

// ActivityOptions retains the pinned SDK's option fields and defaults.
type ActivityOptions = goWorkflow.ActivityOptions
type RetryPolicy = temporal.RetryPolicy
type Priority = temporal.Priority

type activityOptionsKey struct{}

func cloneActivityOptions(o ActivityOptions) ActivityOptions {
	if o.RetryPolicy != nil {
		p := *o.RetryPolicy
		p.NonRetryableErrorTypes = slices.Clone(p.NonRetryableErrorTypes)
		o.RetryPolicy = &p
	}
	return o
}

// WithActivityOptions copies options onto a standard Go context. As in the SDK,
// an empty TaskQueue preserves the task queue already present on the context.
func WithActivityOptions(ctx context.Context, options ActivityOptions) context.Context {
	if options.TaskQueue == "" {
		options.TaskQueue = GetActivityOptions(ctx).TaskQueue
	}
	return context.WithValue(ctx, activityOptionsKey{}, cloneActivityOptions(options))
}

func GetActivityOptions(ctx context.Context) ActivityOptions {
	if ctx == nil {
		return ActivityOptions{}
	}
	o, _ := ctx.Value(activityOptionsKey{}).(ActivityOptions)
	return cloneActivityOptions(o)
}

func withActivityOption(ctx context.Context, change func(*ActivityOptions)) context.Context {
	o := GetActivityOptions(ctx)
	change(&o)
	return context.WithValue(ctx, activityOptionsKey{}, o)
}
func WithTaskQueue(ctx context.Context, name string) context.Context {
	return withActivityOption(ctx, func(o *ActivityOptions) { o.TaskQueue = name })
}
func WithScheduleToCloseTimeout(ctx context.Context, d time.Duration) context.Context {
	return withActivityOption(ctx, func(o *ActivityOptions) { o.ScheduleToCloseTimeout = d })
}
func WithScheduleToStartTimeout(ctx context.Context, d time.Duration) context.Context {
	return withActivityOption(ctx, func(o *ActivityOptions) { o.ScheduleToStartTimeout = d })
}
func WithStartToCloseTimeout(ctx context.Context, d time.Duration) context.Context {
	return withActivityOption(ctx, func(o *ActivityOptions) { o.StartToCloseTimeout = d })
}
func WithHeartbeatTimeout(ctx context.Context, d time.Duration) context.Context {
	return withActivityOption(ctx, func(o *ActivityOptions) { o.HeartbeatTimeout = d })
}
func WithWaitForCancellation(ctx context.Context, wait bool) context.Context {
	return withActivityOption(ctx, func(o *ActivityOptions) { o.WaitForCancellation = wait })
}
func WithRetryPolicy(ctx context.Context, p RetryPolicy) context.Context {
	return withActivityOption(ctx, func(o *ActivityOptions) {
		o.RetryPolicy = cloneActivityOptions(ActivityOptions{RetryPolicy: &p}).RetryPolicy
	})
}
func WithPriority(ctx context.Context, p Priority) context.Context {
	return withActivityOption(ctx, func(o *ActivityOptions) { o.Priority = p })
}

// Future follows the SDK result contract, using native synchronization internally.
// Get is repeatable and never cancels the operation. Activity cancellation follows
// the context passed to ExecuteActivity, including WaitForCancellation.
type Future interface {
	Get(ctx context.Context, valuePtr any) error
	IsReady() bool
	// ToChannel returns a new buffered channel carrying one result, then closes
	// it. Receiving or ignoring the channel does not cancel the operation.
	ToChannel() <-chan FutureResult
}

// FutureResult carries an encoded success value or an error. On success Value
// is non-nil: use Value.Get(&result) to extract the caller's chosen type, or
// HasValue to check whether an error-only activity returned a value. On failure
// Value is nil. Extraction uses the same converter and checks as Future.Get.
type FutureResult struct {
	Value converter.EncodedValue
	Err   error
}

type futureValue struct {
	future   *activityFuture
	hasValue bool
}

func (v *futureValue) HasValue() bool { return v.hasValue }
func (v *futureValue) Get(valuePtr any) error {
	return v.future.Get(context.Background(), valuePtr)
}

type activityFuture struct {
	// Execution handles are SDK metadata, encoded with built-in JSON.
	dataConverter    converter.DataConverter
	converterContext context.Context
	ready            atomic.Bool
	done             chan struct{}
	payload          *commonpb.Payloads
	err              error
}

func (f *activityFuture) ToChannel() <-chan FutureResult {
	results := make(chan FutureResult, 1)
	isolate.GoInbound(func() {
		defer close(results)
		<-f.done
		if f.err != nil {
			results <- FutureResult{Err: f.err}
			return
		}
		payloads, err := decodePayloads(f.payload, futureDataConverter(f))
		if err == nil && len(payloads.Payloads) > 1 {
			err = fmt.Errorf("workflow: operation returned %d payloads, want 1", len(payloads.Payloads))
		}
		if err != nil {
			results <- FutureResult{Err: err}
			return
		}
		results <- FutureResult{Value: &futureValue{future: f, hasValue: len(payloads.Payloads) != 0}}
	})
	return results
}

func (f *activityFuture) IsReady() bool {
	if isolate.IsReadOnly() {
		return f.ready.Load()
	}
	select {
	case <-f.done:
		return true
	default:
		return false
	}
}
func (f *activityFuture) Get(ctx context.Context, valuePtr any) error {
	if ctx == nil {
		return errors.New("workflow: nil context")
	}
	// Like SDK Future.Get, a canceled waiting context does not abandon the wait.
	// The execution context controls cancellation of the underlying activity.
	if isolate.IsReadOnly() {
		if !f.ready.Load() {
			return errors.New("workflow: read-only handlers cannot wait on an incomplete future")
		}
	} else {
		<-f.done
	}
	if f.err != nil || valuePtr == nil || f.payload == nil {
		return f.err
	}
	value := reflect.ValueOf(valuePtr)
	if value.Kind() != reflect.Pointer || value.IsNil() {
		return errors.New("valuePtr parameter is not a non-nil pointer")
	}
	if isUnsupportedProtoValue(valuePtr) || isUnsupportedProtoValue(value.Elem().Interface()) {
		return errors.New("workflow: legacy gogo protobuf messages are unsupported; use Google protobuf")
	}
	payloads, err := decodePayloads(f.payload, futureDataConverter(f))
	if err != nil {
		return err
	}
	if len(payloads.Payloads) == 0 {
		return nil
	}
	if len(payloads.Payloads) != 1 {
		return fmt.Errorf("workflow: operation returned %d payloads, want 1", len(payloads.Payloads))
	}
	dc := futureDataConverter(f)
	return dc.FromPayloads(payloads, valuePtr)
}

// ExecuteActivity accepts a registered function reference or activity type name,
// with the same argument/result convention as the Temporal Go SDK. Activities
// run on the host. Options come from WithActivityOptions, not a positional timeout.
// Scheduling is acknowledged before returning, even if the Future is ignored.
func executeActivity(ctx context.Context, activity any, args ...any) Future {
	assertWritable()
	f := &activityFuture{done: make(chan struct{})}
	fail := func(err error) Future { f.err = err; f.ready.Store(true); close(f.done); return f }
	if ctx == nil {
		return fail(errors.New("workflow: nil context"))
	}
	if s := GetSessionInfo(ctx); s != nil && s.SessionState == SessionStateFailed && activity != sessionCreationActivity {
		return fail(ErrSessionFailed)
	}
	if err := ctx.Err(); err != nil {
		return fail(err)
	}
	name, byName := activity.(string)
	if byName {
		if name == "" {
			return fail(errors.New("workflow: empty activity name"))
		}
	} else {
		var err error
		name, err = activityref.Name(activity)
		if err != nil {
			return fail(err)
		}
		typ := reflect.TypeOf(activity)
		if typ.NumOut() < 1 || typ.NumOut() > 2 || !typ.Out(typ.NumOut()-1).Implements(reflect.TypeFor[error]()) {
			return fail(errors.New("workflow: activity must return (result, error) or error"))
		}
		if err := activityref.ValidateArgs(activity, args); err != nil {
			return fail(err)
		}
	}
	options := GetActivityOptions(ctx)
	if s := GetSessionInfo(ctx); s != nil && s.SessionState == SessionStateOpen && name != sessionCreationActivity {
		options.TaskQueue = s.taskqueue
	}
	if options.ScheduleToCloseTimeout < 0 || options.ScheduleToStartTimeout < 0 || options.StartToCloseTimeout < 0 || options.HeartbeatTimeout < 0 {
		return fail(errors.New("workflow: negative activity timeout"))
	}
	if options.StartToCloseTimeout == 0 && options.ScheduleToCloseTimeout == 0 {
		return fail(errors.New("workflow: activity requires StartToCloseTimeout or ScheduleToCloseTimeout"))
	}
	f.converterContext = ctx
	f.dataConverter = GetDataConverter(ctx)
	payloads, err := encodeArgsWithConverter(args, f.dataConverter)
	if err != nil {
		panic(err)
	} // SDK serialization errors fail the task.
	id := nextCallID.Add(1)
	header, err := outgoingHeader(ctx)
	if err != nil {
		panic(err)
	}
	request, err := MarshalTransport(ActivityPayloadRequest{ID: id, Name: name, Function: !byName, Payloads: payloads, Options: &options, Header: header})
	if err != nil {
		panic(err)
	}
	if _, err = isolate.Call(OpScheduleActivity, request); err != nil {
		return fail(err)
	}
	stop := context.AfterFunc(ctx, func() { p := encodeID(id); _, _ = isolate.Call(OpCancelActivity, p) })
	// Publish available results during input admission so priority handlers
	// waiting on this future become runnable before ordinary workflow code.
	isolate.GoInbound(func() {
		defer stop()
		p := encodeID(id)
		response, err := isolate.Call(OpAwaitActivity, p)
		f.err = err
		if err == nil {
			var outcome ActivityOutcome
			if f.err = UnmarshalTransport(response, &outcome); f.err == nil {
				f.payload = outcome.Payloads
				if outcome.Failure != nil {
					var transportErr error
					f.err, transportErr = decodeFailure(outcome.Failure, f.dataConverter)
					if transportErr != nil {
						f.err = fmt.Errorf("workflow: decode activity failure: %w", transportErr)
					} else if outcome.Canceled {
						f.err = failurecodec.WithCancellation(f.err, ctx.Err())
					}
				} else if outcome.Failed || outcome.Error != "" {
					if outcome.Canceled && ctx.Err() != nil {
						f.err = ctx.Err()
					} else if outcome.Canceled {
						f.err = temporal.NewCanceledError()
					} else {
						f.err = errors.New(outcome.Error)
					}
				}
			}
		}
		f.ready.Store(true)
		close(f.done)
	})
	return f
}

// ActivityOutcome carries the copied result of an activity completion.
type ActivityOutcome struct {
	ErrorKind string             `json:"error_kind,omitempty"`
	Failure   *failurepb.Failure `json:"failure,omitempty"`
	Failed    bool               `json:"failed,omitempty"`
	Payloads  *commonpb.Payloads `json:"payloads"`
	Error     string             `json:"error,omitempty"`
	Canceled  bool               `json:"canceled,omitempty"`
}

func futureDataConverter(f *activityFuture) converter.DataConverter {
	if isolate.IsReadOnly() && f.converterContext != nil {
		return GetDataConverter(f.converterContext)
	}
	if f.dataConverter != nil {
		return f.dataConverter
	}
	return rootDataConverter()
}
