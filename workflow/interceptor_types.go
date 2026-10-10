package workflow

import (
	"context"
	"github.com/nexus-rpc/sdk-go/nexus"
	"go.temporal.io/sdk/client"
	sdkinterceptor "go.temporal.io/sdk/interceptor"
	"go.temporal.io/sdk/log"
	"go.temporal.io/sdk/temporal"
	"time"
)

// WorkerInterceptor constructs the workflow chain inside an isolate.
// Implementations must embed WorkerInterceptorBase. Activity/client interceptors
// remain ordinary SDK interceptors on the host.
type WorkerInterceptor interface {
	InterceptWorkflow(context.Context, WorkflowInboundInterceptor) WorkflowInboundInterceptor
	mustEmbedWorkerInterceptorBase()
}
type WorkerInterceptorBase struct{}

func (WorkerInterceptorBase) mustEmbedWorkerInterceptorBase() {}
func (WorkerInterceptorBase) InterceptWorkflow(_ context.Context, next WorkflowInboundInterceptor) WorkflowInboundInterceptor {
	return &WorkflowInboundInterceptorBase{Next: next}
}

type ExecuteWorkflowInput = sdkinterceptor.ExecuteWorkflowInput
type HandleSignalInput = sdkinterceptor.HandleSignalInput
type HandleQueryInput = sdkinterceptor.HandleQueryInput
type UpdateInput = sdkinterceptor.UpdateInput

// WorkflowInboundInterceptor matches the SDK lifecycle with native contexts.
// Query and validator calls run in a read-only scope with a freshly built chain.
type WorkflowInboundInterceptor interface {
	Init(WorkflowOutboundInterceptor) error
	ExecuteWorkflow(ctx context.Context, in *ExecuteWorkflowInput) (any, error)
	HandleSignal(ctx context.Context, in *HandleSignalInput) error
	HandleQuery(ctx context.Context, in *HandleQueryInput) (any, error)
	ValidateUpdate(ctx context.Context, in *UpdateInput) error
	ExecuteUpdate(ctx context.Context, in *UpdateInput) (any, error)
	mustEmbedWorkflowInboundInterceptorBase()
}
type WorkflowInboundInterceptorBase struct{ Next WorkflowInboundInterceptor }

func (WorkflowInboundInterceptorBase) mustEmbedWorkflowInboundInterceptorBase() {}
func (b *WorkflowInboundInterceptorBase) Init(out WorkflowOutboundInterceptor) error {
	return b.Next.Init(out)
}
func (b *WorkflowInboundInterceptorBase) ExecuteWorkflow(ctx context.Context, in *ExecuteWorkflowInput) (any, error) {
	return b.Next.ExecuteWorkflow(ctx, in)
}
func (b *WorkflowInboundInterceptorBase) HandleSignal(ctx context.Context, in *HandleSignalInput) error {
	return b.Next.HandleSignal(ctx, in)
}
func (b *WorkflowInboundInterceptorBase) HandleQuery(ctx context.Context, in *HandleQueryInput) (any, error) {
	return b.Next.HandleQuery(ctx, in)
}
func (b *WorkflowInboundInterceptorBase) ValidateUpdate(ctx context.Context, in *UpdateInput) error {
	return b.Next.ValidateUpdate(ctx, in)
}
func (b *WorkflowInboundInterceptorBase) ExecuteUpdate(ctx context.Context, in *UpdateInput) (any, error) {
	return b.Next.ExecuteUpdate(ctx, in)
}

// WorkflowOutboundInterceptor covers the supported SDK operations. Native go,
// channel, select and time primitives use the deterministic runtime directly.
type WorkflowOutboundInterceptor interface {
	ExecuteActivity(ctx context.Context, activityType string, args ...any) Future
	ExecuteLocalActivity(ctx context.Context, activityType string, args ...any) Future
	ExecuteChildWorkflow(ctx context.Context, workflowType string, args ...any) ChildWorkflowFuture
	GetInfo(ctx context.Context) *Info
	GetCurrentUpdateInfo(ctx context.Context) *UpdateInfo
	GetLogger(ctx context.Context) log.Logger
	GetMetricsHandler(ctx context.Context) client.MetricsHandler
	Now(ctx context.Context) time.Time
	Sleep(ctx context.Context, duration time.Duration) error
	RequestCancelExternalWorkflow(ctx context.Context, workflowID, runID string) Future
	SignalExternalWorkflow(ctx context.Context, workflowID, runID, signalName string, arg any) Future
	SignalChildWorkflow(ctx context.Context, workflowID, signalName string, arg any) Future
	GetSignalChannel(ctx context.Context, signalName string) <-chan SignalResult
	GetVersion(ctx context.Context, changeID string, minSupported, maxSupported Version) Version
	SetQueryHandler(ctx context.Context, queryType string, handler any) error
	SetQueryHandlerWithOptions(ctx context.Context, queryType string, handler any, options QueryHandlerOptions) error
	SetUpdateHandler(ctx context.Context, updateName string, handler any, options UpdateHandlerOptions) error
	IsReplaying(ctx context.Context) bool
	NewContinueAsNewError(ctx context.Context, fn any, args ...any) error
	ExecuteNexusOperation(ctx context.Context, input ExecuteNexusOperationInput) NexusOperationFuture
	RequestCancelNexusOperation(ctx context.Context, input RequestCancelNexusOperationInput)
	GetTypedSearchAttributes(ctx context.Context) temporal.SearchAttributes
	UpsertSearchAttributes(ctx context.Context, attributes map[string]any) error
	UpsertTypedSearchAttributes(ctx context.Context, attributes ...temporal.SearchAttributeUpdate) error
	UpsertMemo(ctx context.Context, memo map[string]any) error
	GetSignalChannelWithOptions(ctx context.Context, signalName string, options SignalChannelOptions) <-chan SignalResult
	HasLastCompletionResult(ctx context.Context) bool
	GetLastCompletionResult(ctx context.Context, values ...any) error
	GetLastError(ctx context.Context) error
	mustEmbedWorkflowOutboundInterceptorBase()
}
type WorkflowOutboundInterceptorBase struct{ Next WorkflowOutboundInterceptor }

func (WorkflowOutboundInterceptorBase) mustEmbedWorkflowOutboundInterceptorBase() {}
func (b *WorkflowOutboundInterceptorBase) ExecuteActivity(ctx context.Context, activityType string, args ...any) Future {
	return b.Next.ExecuteActivity(ctx, activityType, args...)
}
func (b *WorkflowOutboundInterceptorBase) ExecuteLocalActivity(ctx context.Context, activityType string, args ...any) Future {
	return b.Next.ExecuteLocalActivity(ctx, activityType, args...)
}
func (b *WorkflowOutboundInterceptorBase) ExecuteChildWorkflow(ctx context.Context, workflowType string, args ...any) ChildWorkflowFuture {
	return b.Next.ExecuteChildWorkflow(ctx, workflowType, args...)
}
func (b *WorkflowOutboundInterceptorBase) GetInfo(ctx context.Context) *Info {
	return b.Next.GetInfo(ctx)
}
func (b *WorkflowOutboundInterceptorBase) GetCurrentUpdateInfo(ctx context.Context) *UpdateInfo {
	return b.Next.GetCurrentUpdateInfo(ctx)
}
func (b *WorkflowOutboundInterceptorBase) GetLogger(ctx context.Context) log.Logger {
	return b.Next.GetLogger(ctx)
}
func (b *WorkflowOutboundInterceptorBase) GetMetricsHandler(ctx context.Context) client.MetricsHandler {
	return b.Next.GetMetricsHandler(ctx)
}
func (b *WorkflowOutboundInterceptorBase) Now(ctx context.Context) time.Time { return b.Next.Now(ctx) }
func (b *WorkflowOutboundInterceptorBase) Sleep(ctx context.Context, duration time.Duration) error {
	return b.Next.Sleep(ctx, duration)
}
func (b *WorkflowOutboundInterceptorBase) RequestCancelExternalWorkflow(ctx context.Context, workflowID, runID string) Future {
	return b.Next.RequestCancelExternalWorkflow(ctx, workflowID, runID)
}
func (b *WorkflowOutboundInterceptorBase) SignalExternalWorkflow(ctx context.Context, workflowID, runID, signalName string, arg any) Future {
	return b.Next.SignalExternalWorkflow(ctx, workflowID, runID, signalName, arg)
}
func (b *WorkflowOutboundInterceptorBase) SignalChildWorkflow(ctx context.Context, workflowID, signalName string, arg any) Future {
	return b.Next.SignalChildWorkflow(ctx, workflowID, signalName, arg)
}
func (b *WorkflowOutboundInterceptorBase) GetSignalChannel(ctx context.Context, signalName string) <-chan SignalResult {
	return b.Next.GetSignalChannel(ctx, signalName)
}
func (b *WorkflowOutboundInterceptorBase) GetVersion(ctx context.Context, changeID string, minSupported, maxSupported Version) Version {
	return b.Next.GetVersion(ctx, changeID, minSupported, maxSupported)
}
func (b *WorkflowOutboundInterceptorBase) SetQueryHandler(ctx context.Context, queryType string, handler any) error {
	return b.Next.SetQueryHandler(ctx, queryType, handler)
}
func (b *WorkflowOutboundInterceptorBase) SetQueryHandlerWithOptions(ctx context.Context, queryType string, handler any, options QueryHandlerOptions) error {
	return b.Next.SetQueryHandlerWithOptions(ctx, queryType, handler, options)
}
func (b *WorkflowOutboundInterceptorBase) SetUpdateHandler(ctx context.Context, updateName string, handler any, options UpdateHandlerOptions) error {
	return b.Next.SetUpdateHandler(ctx, updateName, handler, options)
}
func (b *WorkflowOutboundInterceptorBase) IsReplaying(ctx context.Context) bool {
	return b.Next.IsReplaying(ctx)
}
func (b *WorkflowOutboundInterceptorBase) NewContinueAsNewError(ctx context.Context, fn any, args ...any) error {
	return b.Next.NewContinueAsNewError(ctx, fn, args...)
}
func (b *WorkflowOutboundInterceptorBase) ExecuteNexusOperation(ctx context.Context, input ExecuteNexusOperationInput) NexusOperationFuture {
	return b.Next.ExecuteNexusOperation(ctx, input)
}
func (b *WorkflowOutboundInterceptorBase) RequestCancelNexusOperation(ctx context.Context, input RequestCancelNexusOperationInput) {
	b.Next.RequestCancelNexusOperation(ctx, input)
}

type ExecuteNexusOperationInput struct {
	Client           NexusClient
	Operation, Input any
	Options          NexusOperationOptions
	NexusHeader      nexus.Header
}
type RequestCancelNexusOperationInput struct {
	Client    NexusClient
	Operation any
	Token     string
	seq       uint64
}

func (b *WorkflowOutboundInterceptorBase) GetTypedSearchAttributes(ctx context.Context) temporal.SearchAttributes {
	return b.Next.GetTypedSearchAttributes(ctx)
}

func (b *WorkflowOutboundInterceptorBase) UpsertSearchAttributes(ctx context.Context, attributes map[string]any) error {
	return b.Next.UpsertSearchAttributes(ctx, attributes)
}

func (b *WorkflowOutboundInterceptorBase) UpsertTypedSearchAttributes(ctx context.Context, attributes ...temporal.SearchAttributeUpdate) error {
	return b.Next.UpsertTypedSearchAttributes(ctx, attributes...)
}

func (b *WorkflowOutboundInterceptorBase) UpsertMemo(ctx context.Context, memo map[string]any) error {
	return b.Next.UpsertMemo(ctx, memo)
}

func (b *WorkflowOutboundInterceptorBase) GetSignalChannelWithOptions(ctx context.Context, signalName string, options SignalChannelOptions) <-chan SignalResult {
	return b.Next.GetSignalChannelWithOptions(ctx, signalName, options)
}

func (b *WorkflowOutboundInterceptorBase) HasLastCompletionResult(ctx context.Context) bool {
	return b.Next.HasLastCompletionResult(ctx)
}

func (b *WorkflowOutboundInterceptorBase) GetLastCompletionResult(ctx context.Context, values ...any) error {
	return b.Next.GetLastCompletionResult(ctx, values...)
}

func (b *WorkflowOutboundInterceptorBase) GetLastError(ctx context.Context) error {
	return b.Next.GetLastError(ctx)
}
