// Workflow tracing adapted from go.temporal.io/sdk v1.49.0.
// Copyright (c) 2020 Temporal Technologies Inc. MIT License.
package interceptor

import (
	"context"
	"fmt"
	"github.com/mfateev/sdk-go-poc/internal/tracingheader"
	"github.com/mfateev/sdk-go-poc/workflow"
	"github.com/nexus-rpc/sdk-go/nexus"
	commonpb "go.temporal.io/api/common/v1"
	sdkinterceptor "go.temporal.io/sdk/interceptor"
	"go.temporal.io/sdk/log"
	"time"
)

// Tracer and its span contracts retain the SDK v1 API. Tracers instantiated by
// an isolate factory must keep state private and export observations via sinks.
// Ordinary SDK client/activity tracing continues to use the upstream interceptor.
type Tracer = sdkinterceptor.Tracer
type BaseTracer = sdkinterceptor.BaseTracer
type TracerOptions = sdkinterceptor.TracerOptions
type TracerSpan = sdkinterceptor.TracerSpan
type TracerSpanRef = sdkinterceptor.TracerSpanRef
type TracerStartSpanOptions = sdkinterceptor.TracerStartSpanOptions
type TracerFinishSpanOptions = sdkinterceptor.TracerFinishSpanOptions

const (
	workflowIDTagKey = "temporalWorkflowID"
	runIDTagKey      = "temporalRunID"
	updateIDTagKey   = "temporalUpdateID"
)

type tracingInterceptor struct {
	WorkerInterceptorBase
	tracer  Tracer
	options TracerOptions
}

// NewTracingInterceptor constructs the workflow interceptor inside an isolate.
// Unlike the upstream combined interceptor, this returns a workflow-only chain.
// Spans are constructed on replay; the configured sink suppresses their export.
func NewTracingInterceptor(tracer Tracer) WorkerInterceptor {
	if tracer == nil {
		panic("interceptor: nil tracer")
	}
	options := tracer.Options()
	if options.SpanContextKey == nil || options.HeaderKey == "" {
		panic("interceptor: tracing requires SpanContextKey and HeaderKey")
	}
	return &tracingInterceptor{tracer: tracer, options: options}
}
func (t *tracingInterceptor) InterceptWorkflow(ctx context.Context, next WorkflowInboundInterceptor) WorkflowInboundInterceptor {
	return &tracingWorkflowInboundInterceptor{WorkflowInboundInterceptorBase: WorkflowInboundInterceptorBase{Next: next}, root: t, info: workflow.GetInfo(ctx)}
}

type tracingWorkflowInboundInterceptor struct {
	WorkflowInboundInterceptorBase
	root        *tracingInterceptor
	spanCounter uint64
	info        *workflow.Info // Stable execution identity; no per-span Call/yield.
}

// newIdempotencyKey combines the original run ID with the private span counter.
// Synthetic namespace and workflow IDs used by offline replay cannot change it.
func (t *tracingWorkflowInboundInterceptor) newIdempotencyKey() string {
	t.spanCounter++
	runID := t.info.OriginalRunID
	if runID == "" {
		runID = t.info.WorkflowExecution.RunID
	}
	return fmt.Sprintf("WorkflowInboundInterceptor:%s:%d", runID, t.spanCounter)
}

func (t *tracingWorkflowInboundInterceptor) Init(outbound WorkflowOutboundInterceptor) error {
	i := &tracingWorkflowOutboundInterceptor{root: t.root, info: t.info}
	i.Next = outbound
	return t.Next.Init(i)
}

func (t *tracingWorkflowInboundInterceptor) ExecuteWorkflow(
	ctx context.Context,
	in *ExecuteWorkflowInput,
) (any, error) {
	// Start span reading from header
	span, ctx, err := t.root.startSpanFromWorkflowContext(ctx, &TracerStartSpanOptions{
		Operation: "RunWorkflow",
		Name:      t.info.WorkflowType.Name,
		Tags: map[string]string{
			workflowIDTagKey: t.info.WorkflowExecution.ID,
			runIDTagKey:      t.info.WorkflowExecution.RunID,
		},
		FromHeader:     true,
		Time:           t.info.WorkflowStartTime,
		IdempotencyKey: t.newIdempotencyKey(),
	}, t.root.workflowHeaderReader(ctx), t.root.workflowHeaderWriter(ctx))
	if err != nil {
		return nil, err
	}
	var finishOpts TracerFinishSpanOptions
	defer span.Finish(&finishOpts)

	ret, err := t.Next.ExecuteWorkflow(ctx, in)
	if !workflow.IsContinueAsNewError(err) {
		finishOpts.Error = err
	}
	return ret, err
}

func (t *tracingWorkflowInboundInterceptor) HandleSignal(ctx context.Context, in *HandleSignalInput) error {
	// Construct spans on replay too; the host sink suppresses duplicate exports.
	if t.root.options.DisableSignalTracing {
		return t.Next.HandleSignal(ctx, in)
	}
	// Start span reading from header
	info := t.info
	span, ctx, err := t.root.startSpanFromWorkflowContext(ctx, &TracerStartSpanOptions{
		Operation: "HandleSignal",
		Name:      in.SignalName,
		Tags: map[string]string{
			workflowIDTagKey: info.WorkflowExecution.ID,
			runIDTagKey:      info.WorkflowExecution.RunID,
		},
		FromHeader:     true,
		Time:           time.Now(),
		IdempotencyKey: t.newIdempotencyKey(),
	}, t.root.workflowHeaderReader(ctx), t.root.workflowHeaderWriter(ctx))
	if err != nil {
		return err
	}
	var finishOpts TracerFinishSpanOptions
	defer span.Finish(&finishOpts)

	err = t.Next.HandleSignal(ctx, in)
	if !workflow.IsContinueAsNewError(err) {
		finishOpts.Error = err
	}
	return err
}

func (t *tracingWorkflowInboundInterceptor) HandleQuery(
	ctx context.Context,
	in *HandleQueryInput,
) (any, error) {
	// Construct spans on replay too; the host sink suppresses duplicate exports.
	if t.root.options.DisableQueryTracing {
		return t.Next.HandleQuery(ctx, in)
	}
	// Start span reading from header
	info := t.info
	span, ctx, err := t.root.startSpanFromWorkflowContext(ctx, &TracerStartSpanOptions{
		Operation: "HandleQuery",
		Name:      in.QueryType,
		Tags: map[string]string{
			workflowIDTagKey: info.WorkflowExecution.ID,
			runIDTagKey:      info.WorkflowExecution.RunID,
		},
		FromHeader: true,
		Time:       time.Now(),
		// We intentionally do not set IdempotencyKey here because queries are not recorded in
		// workflow history. When the tracing interceptor's span counter is reset between workflow
		// replays, old queries will not be processed which could result in idempotency key
		// collisions with other queries or signals.
	}, t.root.workflowHeaderReader(ctx), t.root.workflowHeaderWriter(ctx))
	if err != nil {
		return nil, err
	}
	var finishOpts TracerFinishSpanOptions
	defer span.Finish(&finishOpts)

	val, err := t.Next.HandleQuery(ctx, in)
	if !workflow.IsContinueAsNewError(err) {
		finishOpts.Error = err
	}
	return val, err
}

func (t *tracingWorkflowInboundInterceptor) ValidateUpdate(
	ctx context.Context,
	in *UpdateInput,
) error {
	// Construct spans on replay too; the host sink suppresses duplicate exports.
	if t.root.options.DisableUpdateTracing {
		return t.Next.ValidateUpdate(ctx, in)
	}
	// Start span reading from header
	info := t.info
	currentUpdateInfo := workflow.GetCurrentUpdateInfo(ctx)
	span, ctx, err := t.root.startSpanFromWorkflowContext(ctx, &TracerStartSpanOptions{
		Operation: "ValidateUpdate",
		Name:      in.Name,
		Tags: map[string]string{
			workflowIDTagKey: info.WorkflowExecution.ID,
			runIDTagKey:      info.WorkflowExecution.RunID,
			updateIDTagKey:   currentUpdateInfo.ID,
		},
		FromHeader: true,
		Time:       time.Now(),
		// We intentionally do not set IdempotencyKey here because validation is not run on
		// replay. When the tracing interceptor's span counter is reset between workflow
		// replays, the validator will not be processed which could result in impotency key
		// collisions with other requests.
	}, t.root.workflowHeaderReader(ctx), t.root.workflowHeaderWriter(ctx))
	if err != nil {
		return err
	}
	var finishOpts TracerFinishSpanOptions
	defer span.Finish(&finishOpts)

	err = t.Next.ValidateUpdate(ctx, in)
	if !workflow.IsContinueAsNewError(err) {
		finishOpts.Error = err
	}
	return err
}

func (t *tracingWorkflowInboundInterceptor) ExecuteUpdate(
	ctx context.Context,
	in *UpdateInput,
) (any, error) {
	// Construct spans on replay too; the host sink suppresses duplicate exports.
	if t.root.options.DisableUpdateTracing {
		return t.Next.ExecuteUpdate(ctx, in)
	}
	// Start span reading from header
	info := t.info
	currentUpdateInfo := workflow.GetCurrentUpdateInfo(ctx)
	span, ctx, err := t.root.startSpanFromWorkflowContext(ctx, &TracerStartSpanOptions{
		// Using operation name "HandleUpdate" to match other SDKs and by consistence with other operations
		Operation: "HandleUpdate",
		Name:      in.Name,
		Tags: map[string]string{
			workflowIDTagKey: info.WorkflowExecution.ID,
			runIDTagKey:      info.WorkflowExecution.RunID,
			updateIDTagKey:   currentUpdateInfo.ID,
		},
		FromHeader:     true,
		Time:           time.Now(),
		IdempotencyKey: t.newIdempotencyKey(),
	}, t.root.workflowHeaderReader(ctx), t.root.workflowHeaderWriter(ctx))
	if err != nil {
		return nil, err
	}
	var finishOpts TracerFinishSpanOptions
	defer span.Finish(&finishOpts)

	val, err := t.Next.ExecuteUpdate(ctx, in)
	if !workflow.IsContinueAsNewError(err) {
		finishOpts.Error = err
	}
	return val, err
}

type tracingWorkflowOutboundInterceptor struct {
	WorkflowOutboundInterceptorBase
	root *tracingInterceptor
	info *workflow.Info // Stable execution identity; no per-span Call/yield.
}

func (t *tracingWorkflowOutboundInterceptor) ExecuteActivity(
	ctx context.Context,
	activityType string,
	args ...any,
) workflow.Future {
	// Start span writing to header
	span, ctx, err := t.startOperationSpan(ctx, "StartActivity", activityType, true, t.root.workflowHeaderWriter(ctx))
	if err != nil {
		return err
	}
	defer span.Finish(&TracerFinishSpanOptions{})

	return t.Next.ExecuteActivity(ctx, activityType, args...)
}

func (t *tracingWorkflowOutboundInterceptor) ExecuteLocalActivity(
	ctx context.Context,
	activityType string,
	args ...any,
) workflow.Future {
	// Start span writing to header
	span, ctx, err := t.startOperationSpan(ctx, "StartActivity", activityType, true, t.root.workflowHeaderWriter(ctx))
	if err != nil {
		return err
	}
	defer span.Finish(&TracerFinishSpanOptions{})

	return t.Next.ExecuteLocalActivity(ctx, activityType, args...)
}

func (t *tracingWorkflowOutboundInterceptor) GetLogger(ctx context.Context) log.Logger {
	if span, _ := ctx.Value(t.root.options.SpanContextKey).(TracerSpan); span != nil {
		return t.root.tracer.GetLogger(t.Next.GetLogger(ctx), span)
	}
	return t.Next.GetLogger(ctx)
}

func (t *tracingWorkflowOutboundInterceptor) ExecuteChildWorkflow(
	ctx context.Context,
	childWorkflowType string,
	args ...any,
) workflow.ChildWorkflowFuture {
	// Start span writing to header
	span, ctx, errFut := t.startOperationSpan(ctx, "StartChildWorkflow", childWorkflowType, false, t.root.workflowHeaderWriter(ctx))
	if errFut != nil {
		return childWorkflowFuture{errFut}
	}
	defer span.Finish(&TracerFinishSpanOptions{})

	return t.Next.ExecuteChildWorkflow(ctx, childWorkflowType, args...)
}

func (t *tracingWorkflowOutboundInterceptor) SignalExternalWorkflow(
	ctx context.Context,
	workflowID string,
	runID string,
	signalName string,
	arg any,
) workflow.Future {
	// Start span writing to header if enabled
	if !t.root.options.DisableSignalTracing {
		var span TracerSpan
		var futErr workflow.Future
		span, ctx, futErr = t.startOperationSpan(ctx, "SignalExternalWorkflow", signalName, false, t.root.workflowHeaderWriter(ctx))
		if futErr != nil {
			return futErr
		}
		defer span.Finish(&TracerFinishSpanOptions{})
	}

	return t.Next.SignalExternalWorkflow(ctx, workflowID, runID, signalName, arg)
}

func (t *tracingWorkflowOutboundInterceptor) SignalChildWorkflow(
	ctx context.Context,
	workflowID string,
	signalName string,
	arg any,
) workflow.Future {
	// Start span writing to header if enabled
	if !t.root.options.DisableSignalTracing {
		var span TracerSpan
		var futErr workflow.Future
		span, ctx, futErr = t.startOperationSpan(ctx, "SignalChildWorkflow", signalName, false, t.root.workflowHeaderWriter(ctx))
		if futErr != nil {
			return futErr
		}
		defer span.Finish(&TracerFinishSpanOptions{})
	}

	return t.Next.SignalChildWorkflow(ctx, workflowID, signalName, arg)
}

func (t *tracingWorkflowOutboundInterceptor) ExecuteNexusOperation(ctx context.Context, input ExecuteNexusOperationInput) workflow.NexusOperationFuture {
	// Start span writing to header
	var ok bool
	var operationName string
	if operationName, ok = input.Operation.(string); ok {
	} else if regOp, ok := input.Operation.(interface{ Name() string }); ok {
		operationName = regOp.Name()
	} else {
		return nexusOperationFuture{workflowFutureFromErr(ctx, fmt.Errorf("unexpected operation type: %v", input.Operation))}
	}
	span, ctx, futErr := t.startOperationSpan(ctx, "StartNexusOperation", input.Client.Service()+"/"+operationName, false, t.root.nexusHeaderWriter(input.NexusHeader))
	if futErr != nil {
		return nexusOperationFuture{futErr}
	}
	defer span.Finish(&TracerFinishSpanOptions{})

	return t.Next.ExecuteNexusOperation(ctx, input)
}

func (t *tracingWorkflowOutboundInterceptor) NewContinueAsNewError(
	ctx context.Context,
	wfn any,
	args ...any,
) error {
	if span, _ := ctx.Value(t.root.options.SpanContextKey).(TracerSpan); span != nil {
		if err := t.root.writeSpanToHeader(span, WorkflowHeader(ctx)); err != nil {
			return err
		}
	}
	err := t.Next.NewContinueAsNewError(ctx, wfn, args...)
	return err
}

type nopSpan struct{}

func (nopSpan) Finish(*TracerFinishSpanOptions) {}

// Span always returned, even in replay. futErr is non-nil on error.
func (t *tracingWorkflowOutboundInterceptor) startOperationSpan(
	ctx context.Context,
	operation string,
	name string,
	dependedOn bool,
	headerWriter func(TracerSpan) error,
) (span TracerSpan, newCtx context.Context, futErr workflow.Future) {
	info := t.info
	span, newCtx, err := t.root.startSpanFromWorkflowContext(ctx, &TracerStartSpanOptions{
		Operation:  operation,
		Name:       name,
		DependedOn: dependedOn,
		Tags: map[string]string{
			workflowIDTagKey: info.WorkflowExecution.ID,
			runIDTagKey:      info.WorkflowExecution.RunID,
		},
		ToHeader: true,
		Time:     time.Now(),
	}, t.root.workflowHeaderReader(ctx), headerWriter)
	if err != nil {
		return nopSpan{}, ctx, workflowFutureFromErr(ctx, err)
	}
	return span, newCtx, nil
}

func (t *tracingInterceptor) startSpanFromWorkflowContext(
	ctx context.Context,
	options *TracerStartSpanOptions,
	headerReader func() (TracerSpanRef, error),
	headerWriter func(span TracerSpan) error,
) (TracerSpan, context.Context, error) {
	span, err := t.startSpan(ctx, options, headerReader, headerWriter)
	if err != nil {
		return nil, nil, err
	}
	return span, t.tracer.ContextWithSpan(context.WithValue(ctx, t.options.SpanContextKey, span), span), nil
}

// Note, this does not put the span on the context
func (t *tracingInterceptor) startSpan(
	ctx interface{ Value(any) any },
	options *TracerStartSpanOptions,
	headerReader func() (TracerSpanRef, error),
	headerWriter func(span TracerSpan) error,
) (TracerSpan, error) {
	// Get parent span from header if not already present and allowed
	if options.Parent == nil && options.FromHeader {
		if span, err := headerReader(); err != nil && !t.options.AllowInvalidParentSpans {
			return nil, err
		} else if span != nil {
			options.Parent = span
		}
	}

	// If no parent span, try to get from context
	if options.Parent == nil {
		options.Parent, _ = ctx.Value(t.options.SpanContextKey).(TracerSpan)
	}

	// Start the span
	var span TracerSpan
	var err error
	if tracer, ok := t.tracer.(interface {
		StartSpanWithContext(context.Context, *TracerStartSpanOptions) (TracerSpan, error)
	}); ok {
		// Native sink tracers retain the read-only observation scope context.
		span, err = tracer.StartSpanWithContext(ctx.(context.Context), options)
	} else {
		span, err = t.tracer.StartSpan(options)
	}
	if err != nil {
		return nil, err
	}

	// Put span in header if wanted
	if options.ToHeader {
		if err := headerWriter(span); err != nil {
			return nil, err
		}
	}
	return span, nil
}

func (t *tracingInterceptor) workflowHeaderReader(ctx context.Context) func() (TracerSpanRef, error) {
	header := WorkflowHeader(ctx)
	return func() (TracerSpanRef, error) {
		return t.readSpanFromHeader(header)
	}
}

func (t *tracingInterceptor) workflowHeaderWriter(ctx context.Context) func(TracerSpan) error {
	header := WorkflowHeader(ctx)
	return func(span TracerSpan) error {
		return t.writeSpanToHeader(span, header)
	}
}

func (t *tracingInterceptor) nexusHeaderReader(header nexus.Header) func() (TracerSpanRef, error) {
	return func() (TracerSpanRef, error) {
		return t.readSpanFromNexusHeader(header)
	}
}

func (t *tracingInterceptor) nexusHeaderWriter(header nexus.Header) func(TracerSpan) error {
	return func(span TracerSpan) error {
		return t.writeSpanToNexusHeader(span, header)
	}
}

func (t *tracingInterceptor) readSpanFromHeader(header map[string]*commonpb.Payload) (TracerSpanRef, error) {
	// Get from map
	payload := header[t.options.HeaderKey]
	if payload == nil {
		return nil, nil
	}
	// Convert from the payload
	var data map[string]string
	if err := tracingheader.Decode(payload, &data); err != nil {
		return nil, err
	}
	// Unmarshal
	return t.tracer.UnmarshalSpan(data)
}

func (t *tracingInterceptor) writeSpanToHeader(span TracerSpan, header map[string]*commonpb.Payload) error {
	// Serialize span to map
	data, err := t.tracer.MarshalSpan(span)
	if err != nil || len(data) == 0 {
		return err
	}
	// Convert to payload
	payload, err := tracingheader.Encode(data)
	if err != nil {
		return err
	}
	// Put on header
	header[t.options.HeaderKey] = payload
	return nil
}

func (t *tracingInterceptor) writeSpanToNexusHeader(span TracerSpan, header nexus.Header) error {
	// Serialize span to map
	data, err := t.tracer.MarshalSpan(span)
	if err != nil || len(data) == 0 {
		return err
	}
	// Put on header
	for k, v := range data {
		header.Set(k, v)
	}
	return nil
}

func (t *tracingInterceptor) readSpanFromNexusHeader(header nexus.Header) (TracerSpanRef, error) {
	return t.tracer.UnmarshalSpan(header)
}

func workflowFutureFromErr(_ context.Context, err error) workflow.Future {
	return tracingErrorFuture{err}
}

type tracingErrorFuture struct{ err error }

func (f tracingErrorFuture) Get(context.Context, any) error { return f.err }
func (f tracingErrorFuture) IsReady() bool                  { return true }
func (f tracingErrorFuture) ToChannel() <-chan workflow.FutureResult {
	out := make(chan workflow.FutureResult, 1)
	out <- workflow.FutureResult{Err: f.err}
	close(out)
	return out
}

type nexusOperationFuture struct{ workflow.Future }

func (f nexusOperationFuture) GetNexusOperationExecution() workflow.Future { return f.Future }

type childWorkflowFuture struct{ workflow.Future }

func (f childWorkflowFuture) GetChildWorkflowExecution() workflow.Future { return f.Future }
func (f childWorkflowFuture) SignalChildWorkflow(context.Context, string, any) workflow.Future {
	return f.Future
}
