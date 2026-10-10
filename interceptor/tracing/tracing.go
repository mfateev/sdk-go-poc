// Workflow tracing adapted from go.temporal.io/sdk v1.49.0.
// Copyright (c) 2020 Temporal Technologies Inc. MIT License.
package tracing

import (
	"context"
	"errors"
	"fmt"
	"github.com/mfateev/sdk-go-poc/interceptor"
	"github.com/mfateev/sdk-go-poc/internal/tracingheader"
	"github.com/mfateev/sdk-go-poc/workflow"
	"github.com/nexus-rpc/sdk-go/nexus"
	commonpb "go.temporal.io/api/common/v1"
	sdktracing "go.temporal.io/sdk/interceptor/tracing"
	"go.temporal.io/sdk/log"
)

// WorkflowTracer has the SDK v2 tracer methods, with standard Go contexts.
// Its implementation must be constructed privately by the isolate factory.
type WorkflowTracer = sdktracing.Tracer
type tracerCommon = sdktracing.Tracer
type BaseTracer = sdktracing.BaseTracer
type TracerOptions = sdktracing.TracerOptions
type TracerSpanRef = sdktracing.TracerSpanRef
type TracerSpan = sdktracing.TracerSpan
type TracerStartSpanOptions = sdktracing.TracerStartSpanOptions
type TracerFinishSpanOptions = sdktracing.TracerFinishSpanOptions
type SpanDirection = sdktracing.SpanDirection

const (
	SpanDirectionUnspecified = sdktracing.SpanDirectionUnspecified
	SpanDirectionInbound     = sdktracing.SpanDirectionInbound
	SpanDirectionOutbound    = sdktracing.SpanDirectionOutbound
	workflowIDTagKey         = "temporalWorkflowID"
	runIDTagKey              = "temporalRunID"
	updateIDTagKey           = "temporalUpdateID"
	nexusServiceTagKey       = "temporalNexusService"
	nexusOperationTagKey     = "temporalNexusOperation"
	nexusEndpointTagKey      = "temporalNexusEndpoint"
)

type tracingInterceptor struct {
	interceptor.WorkerInterceptorBase
	workflowTracer WorkflowTracer
}

// NewTracingInterceptor constructs the workflow portion of SDK v2 tracing.
// Configure ordinary host client/activity interception using the upstream API.
func NewTracingInterceptor(tracer WorkflowTracer) interceptor.WorkerInterceptor {
	if tracer == nil || tracer.Options().HeaderKey == "" {
		panic("tracing: tracer requires HeaderKey")
	}
	return &tracingInterceptor{workflowTracer: tracer}
}
func (t *tracingInterceptor) InterceptWorkflow(ctx context.Context, next interceptor.WorkflowInboundInterceptor) interceptor.WorkflowInboundInterceptor {
	return &tracingWorkflowInboundInterceptor{WorkflowInboundInterceptorBase: interceptor.WorkflowInboundInterceptorBase{Next: next}, root: t, info: workflow.GetInfo(ctx)}
}

type tracingWorkflowInboundInterceptor struct {
	interceptor.WorkflowInboundInterceptorBase
	root *tracingInterceptor
	info *workflow.Info // Stable execution identity; no per-span Call/yield.
}

func (t *tracingWorkflowInboundInterceptor) Init(outbound interceptor.WorkflowOutboundInterceptor) error {
	i := &tracingWorkflowOutboundInterceptor{root: t.root, info: t.info}
	i.Next = outbound
	return t.Next.Init(i)
}

func (t *tracingWorkflowInboundInterceptor) ExecuteWorkflow(
	ctx context.Context,
	in *interceptor.ExecuteWorkflowInput,
) (ret any, err error) {
	info := t.info
	ctx, endSpan, err := startInboundWorkflowSpan(t.root.workflowTracer, ctx, &TracerStartSpanOptions{
		Operation: "RunWorkflow",
		Name:      info.WorkflowType.Name,
		Tags:      workflowTags(info),
	}, t.root.workflowHeaderReader(t.root.workflowTracer, ctx))
	if err != nil {
		return nil, err
	}

	var spanErr error
	defer endSpan(&spanErr)
	ret, err = t.Next.ExecuteWorkflow(ctx, in)
	if !isContinueAsNewError(err) {
		spanErr = err
	}
	return ret, err
}

func (t *tracingWorkflowInboundInterceptor) HandleSignal(ctx context.Context, in *interceptor.HandleSignalInput) (err error) {
	info := t.info
	ctx, endSpan, err := startInboundWorkflowSpan(t.root.workflowTracer, ctx, &TracerStartSpanOptions{
		Operation: "HandleSignal",
		Name:      in.SignalName,
		Tags:      workflowTags(info),
	}, t.root.workflowHeaderReader(t.root.workflowTracer, ctx))
	if err != nil {
		return err
	}
	defer endSpan(&err)

	return t.Next.HandleSignal(ctx, in)
}

func (t *tracingWorkflowInboundInterceptor) HandleQuery(
	ctx context.Context,
	in *interceptor.HandleQueryInput,
) (val any, err error) {
	info := t.info
	ctx, endSpan, err := startInboundWorkflowSpan(t.root.workflowTracer, ctx, &TracerStartSpanOptions{
		Operation: "HandleQuery",
		Name:      in.QueryType,
		Tags:      workflowTags(info),
	}, t.root.workflowHeaderReader(t.root.workflowTracer, ctx))
	if err != nil {
		return nil, err
	}
	defer endSpan(&err)

	return t.Next.HandleQuery(ctx, in)
}

func (t *tracingWorkflowInboundInterceptor) ValidateUpdate(
	ctx context.Context,
	in *interceptor.UpdateInput,
) (err error) {
	info := t.info
	currentUpdateInfo := workflow.GetCurrentUpdateInfo(ctx)
	ctx, endSpan, err := startInboundWorkflowSpan(t.root.workflowTracer, ctx, &TracerStartSpanOptions{
		Operation: "ValidateUpdate",
		Name:      in.Name,
		Tags:      workflowTagsWithUpdate(info, currentUpdateInfo.ID),
	}, t.root.workflowHeaderReader(t.root.workflowTracer, ctx))
	if err != nil {
		return err
	}
	defer endSpan(&err)

	return t.Next.ValidateUpdate(ctx, in)
}

func (t *tracingWorkflowInboundInterceptor) ExecuteUpdate(
	ctx context.Context,
	in *interceptor.UpdateInput,
) (val any, err error) {
	info := t.info
	currentUpdateInfo := workflow.GetCurrentUpdateInfo(ctx)
	ctx, endSpan, err := startInboundWorkflowSpan(t.root.workflowTracer, ctx, &TracerStartSpanOptions{
		Operation: "HandleUpdate",
		Name:      in.Name,
		Tags:      workflowTagsWithUpdate(info, currentUpdateInfo.ID),
	}, t.root.workflowHeaderReader(t.root.workflowTracer, ctx))
	if err != nil {
		return nil, err
	}
	defer endSpan(&err)

	return t.Next.ExecuteUpdate(ctx, in)
}

type tracingWorkflowOutboundInterceptor struct {
	interceptor.WorkflowOutboundInterceptorBase
	root *tracingInterceptor
	info *workflow.Info // Stable execution identity; no per-span Call/yield.
}

func (t *tracingWorkflowOutboundInterceptor) ExecuteActivity(
	ctx context.Context,
	activityType string,
	args ...any,
) workflow.Future {
	info := t.info
	ctx, endSpan, err := startOutboundWorkflowSpan(t.root.workflowTracer, ctx, &TracerStartSpanOptions{
		Operation:  "StartActivity",
		Name:       activityType,
		Tags:       workflowTags(info),
		DependedOn: true,
	}, t.root.workflowHeaderWriter(t.root.workflowTracer, ctx))
	if err != nil {
		return workflowFutureFromErr(ctx, err)
	}
	defer endSpan(nil)

	return t.Next.ExecuteActivity(ctx, activityType, args...)
}

func (t *tracingWorkflowOutboundInterceptor) ExecuteLocalActivity(
	ctx context.Context,
	activityType string,
	args ...any,
) workflow.Future {
	info := t.info
	ctx, endSpan, err := startOutboundWorkflowSpan(t.root.workflowTracer, ctx, &TracerStartSpanOptions{
		Operation:  "StartActivity",
		Name:       activityType,
		Tags:       workflowTags(info),
		DependedOn: true,
	}, t.root.workflowHeaderWriter(t.root.workflowTracer, ctx))
	if err != nil {
		return workflowFutureFromErr(ctx, err)
	}
	defer endSpan(nil)

	return t.Next.ExecuteLocalActivity(ctx, activityType, args...)
}

func (t *tracingWorkflowOutboundInterceptor) GetLogger(ctx context.Context) log.Logger {
	if span := t.root.workflowTracer.SpanFromContext(ctx); span != nil {
		return t.root.workflowTracer.GetLogger(t.Next.GetLogger(ctx), span)
	}
	return t.Next.GetLogger(ctx)
}

func (t *tracingWorkflowOutboundInterceptor) ExecuteChildWorkflow(
	ctx context.Context,
	childWorkflowType string,
	args ...any,
) workflow.ChildWorkflowFuture {
	info := t.info
	ctx, endSpan, err := startOutboundWorkflowSpan(t.root.workflowTracer, ctx, &TracerStartSpanOptions{
		Operation: "StartChildWorkflow",
		Name:      childWorkflowType,
		Tags:      workflowTags(info),
	}, t.root.workflowHeaderWriter(t.root.workflowTracer, ctx))
	if err != nil {
		return childWorkflowFuture{workflowFutureFromErr(ctx, err)}
	}
	defer endSpan(nil)

	return t.Next.ExecuteChildWorkflow(ctx, childWorkflowType, args...)
}

func (t *tracingWorkflowOutboundInterceptor) SignalExternalWorkflow(
	ctx context.Context,
	workflowID string,
	runID string,
	signalName string,
	arg any,
) workflow.Future {
	info := t.info
	ctx, endSpan, err := startOutboundWorkflowSpan(t.root.workflowTracer, ctx, &TracerStartSpanOptions{
		Operation: "SignalExternalWorkflow",
		Name:      signalName,
		Tags:      workflowTags(info),
	}, t.root.workflowHeaderWriter(t.root.workflowTracer, ctx))
	if err != nil {
		return workflowFutureFromErr(ctx, err)
	}
	defer endSpan(nil)

	return t.Next.SignalExternalWorkflow(ctx, workflowID, runID, signalName, arg)
}

func (t *tracingWorkflowOutboundInterceptor) SignalChildWorkflow(
	ctx context.Context,
	workflowID string,
	signalName string,
	arg any,
) workflow.Future {
	info := t.info
	ctx, endSpan, err := startOutboundWorkflowSpan(t.root.workflowTracer, ctx, &TracerStartSpanOptions{
		Operation: "SignalChildWorkflow",
		Name:      signalName,
		Tags:      workflowTags(info),
	}, t.root.workflowHeaderWriter(t.root.workflowTracer, ctx))
	if err != nil {
		return workflowFutureFromErr(ctx, err)
	}
	defer endSpan(nil)

	return t.Next.SignalChildWorkflow(ctx, workflowID, signalName, arg)
}

func (t *tracingWorkflowOutboundInterceptor) ExecuteNexusOperation(ctx context.Context, input interceptor.ExecuteNexusOperationInput) workflow.NexusOperationFuture {
	var ok bool
	var operationName string
	if operationName, ok = input.Operation.(string); ok {
	} else if regOp, ok := input.Operation.(interface{ Name() string }); ok {
		operationName = regOp.Name()
	} else {
		return nexusOperationFuture{workflowFutureFromErr(ctx, fmt.Errorf("unexpected operation type: %v", input.Operation))}
	}
	info := t.info
	ctx, endSpan, err := startOutboundWorkflowSpan(t.root.workflowTracer, ctx, &TracerStartSpanOptions{
		Operation: "StartNexusOperation",
		Name:      input.Client.Service() + "/" + operationName,
		Tags:      workflowTagsWithNexus(info, input.Client.Endpoint(), input.Client.Service(), operationName),
	}, t.root.nexusHeaderWriter(t.root.workflowTracer, input.NexusHeader))
	if err != nil {
		return nexusOperationFuture{workflowFutureFromErr(ctx, err)}
	}
	defer endSpan(nil)

	return t.Next.ExecuteNexusOperation(ctx, input)
}

func (t *tracingWorkflowOutboundInterceptor) NewContinueAsNewError(
	ctx context.Context,
	wfn any,
	args ...any,
) error {
	info := t.info
	ctx, endSpan, err := startOutboundWorkflowSpan(t.root.workflowTracer, ctx, &TracerStartSpanOptions{
		Operation: "ContinueAsNew",
		Name:      info.WorkflowType.Name,
		Tags:      workflowTags(info),
	}, t.root.workflowHeaderWriter(t.root.workflowTracer, ctx))
	if err != nil {
		return err
	}

	var spanErr error
	defer endSpan(&spanErr)
	err = t.Next.NewContinueAsNewError(ctx, wfn, args...)
	if !isContinueAsNewError(err) {
		spanErr = err
	}
	return err
}

func (t *tracingInterceptor) workflowHeaderReader(tracer WorkflowTracer, ctx context.Context) func() (TracerSpanRef, error) {
	header := interceptor.WorkflowHeader(ctx)
	return func() (TracerSpanRef, error) {
		return t.readSpanFromHeader(tracer, header)
	}
}

func (t *tracingInterceptor) workflowHeaderWriter(tracer WorkflowTracer, ctx context.Context) func(TracerSpanRef) error {
	header := interceptor.WorkflowHeader(ctx)
	return func(span TracerSpanRef) error {
		return t.writeSpanToHeader(tracer, span, header)
	}
}

func (t *tracingInterceptor) nexusHeaderReader(tracer tracerCommon, header nexus.Header) func() (TracerSpanRef, error) {
	return func() (TracerSpanRef, error) {
		return t.readSpanFromNexusHeader(tracer, header)
	}
}

func (t *tracingInterceptor) nexusHeaderWriter(tracer tracerCommon, header nexus.Header) func(TracerSpanRef) error {
	return func(span TracerSpanRef) error {
		return t.writeSpanToNexusHeader(tracer, span, header)
	}
}

func (t *tracingInterceptor) readSpanFromHeader(tracer tracerCommon, header map[string]*commonpb.Payload) (TracerSpanRef, error) {
	payload := header[tracer.Options().HeaderKey]
	if payload == nil {
		return nil, nil
	}
	var data map[string]string
	if err := tracingheader.Decode(payload, &data); err != nil {
		return nil, err
	}
	return tracer.UnmarshalSpan(data)
}

func (t *tracingInterceptor) writeSpanToHeader(tracer tracerCommon, span TracerSpanRef, header map[string]*commonpb.Payload) error {
	data, err := tracer.MarshalSpan(span)
	if err != nil || len(data) == 0 {
		return err
	}
	payload, err := tracingheader.Encode(data)
	if err != nil {
		return err
	}
	header[tracer.Options().HeaderKey] = payload
	return nil
}

func (t *tracingInterceptor) writeSpanToNexusHeader(tracer tracerCommon, span TracerSpanRef, header nexus.Header) error {
	data, err := tracer.MarshalSpan(span)
	if err != nil || len(data) == 0 {
		return err
	}
	for k, v := range data {
		header.Set(k, v)
	}
	return nil
}

func (t *tracingInterceptor) readSpanFromNexusHeader(tracer tracerCommon, header nexus.Header) (TracerSpanRef, error) {
	return tracer.UnmarshalSpan(header)
}

func nexusTags(endpoint, service, operation string) map[string]string {
	tags := map[string]string{
		nexusServiceTagKey:   service,
		nexusOperationTagKey: operation,
	}
	if endpoint != "" {
		tags[nexusEndpointTagKey] = endpoint
	}
	return tags
}

func workflowExecutionTags(workflowID, runID string) map[string]string {
	tags := map[string]string{workflowIDTagKey: workflowID}
	if runID != "" {
		tags[runIDTagKey] = runID
	}
	return tags
}

func workflowTags(info *workflow.Info) map[string]string {
	return workflowExecutionTags(info.WorkflowExecution.ID, info.WorkflowExecution.RunID)
}

func workflowTagsWithUpdate(info *workflow.Info, updateID string) map[string]string {
	tags := workflowTags(info)
	tags[updateIDTagKey] = updateID
	return tags
}

func workflowTagsWithNexus(info *workflow.Info, endpoint, service, operation string) map[string]string {
	tags := workflowTags(info)
	tags[nexusServiceTagKey] = service
	tags[nexusOperationTagKey] = operation
	if endpoint != "" {
		tags[nexusEndpointTagKey] = endpoint
	}
	return tags
}

func startInboundWorkflowSpan(
	t WorkflowTracer,
	ctx context.Context,
	options *TracerStartSpanOptions,
	headerReader func() (TracerSpanRef, error),
) (context.Context, func(err *error), error) {
	createSpan := t.Options().AddTemporalSpans

	curr, err := parentFromHeader(t, headerReader)
	if err != nil {
		return ctx, nil, err
	}

	// If there is no span in the headers, use the current span from the context.
	if curr == nil {
		curr = t.SpanFromContext(ctx)
	}

	if createSpan {
		options.Direction = SpanDirectionInbound
		options.Parent = curr
		curr = t.CreateSpan(ctx, options)
	}

	ctx = t.ContextWithSpan(ctx, curr)

	return ctx, finishSpan(curr, createSpan), nil
}

func startOutboundWorkflowSpan(
	t WorkflowTracer,
	ctx context.Context,
	options *TracerStartSpanOptions,
	headerWriter func(TracerSpanRef) error,
) (context.Context, func(err *error), error) {
	createSpan := t.Options().AddTemporalSpans

	curr := t.SpanFromContext(ctx)

	if createSpan {
		options.Direction = SpanDirectionOutbound
		options.Parent = curr
		curr = t.CreateSpan(ctx, options)
		ctx = t.ContextWithSpan(ctx, curr)
	}

	finish, err := writeSpanHeader(curr, createSpan, headerWriter)
	return ctx, finish, err
}

func isContinueAsNewError(err error) bool {
	var continueAsNewErr *workflow.ContinueAsNewError
	return errors.As(err, &continueAsNewErr)
}

func parentFromHeader(t tracerCommon, read func() (TracerSpanRef, error)) (TracerSpanRef, error) {
	span, err := read()
	if err != nil && !t.Options().AllowInvalidParentSpans {
		return nil, err
	}
	return span, nil
}

func finishSpan(span TracerSpanRef, created bool) func(err *error) {
	if !created {
		return func(err *error) {}
	}

	if span, ok := span.(TracerSpan); ok {
		return func(err *error) {
			opts := &TracerFinishSpanOptions{}
			if err != nil {
				opts.Error = *err
			}
			span.Finish(opts)
		}
	}

	return func(err *error) {}
}

func writeSpanHeader(
	span TracerSpanRef,
	created bool,
	headerWriter func(TracerSpanRef) error,
) (func(err *error), error) {
	finish := finishSpan(span, created)

	if headerWriter == nil {
		return finish, nil
	}

	if err := headerWriter(span); err != nil {
		finish(&err)
		return nil, err
	}

	return finish, nil
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
