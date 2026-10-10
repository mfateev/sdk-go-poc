// Package interceptor provides SDK-style workflow interception with native Go
// contexts. Factories and workflow chains belong to an isolate; host activity
// and client interceptors remain in go.temporal.io/sdk/interceptor.
package interceptor

import (
	"context"
	"github.com/mfateev/sdk-go-poc/workflow"
	commonpb "go.temporal.io/api/common/v1"
)

type WorkerInterceptor = workflow.WorkerInterceptor
type WorkerInterceptorBase = workflow.WorkerInterceptorBase
type WorkflowInboundInterceptor = workflow.WorkflowInboundInterceptor
type WorkflowInboundInterceptorBase = workflow.WorkflowInboundInterceptorBase
type WorkflowOutboundInterceptor = workflow.WorkflowOutboundInterceptor
type WorkflowOutboundInterceptorBase = workflow.WorkflowOutboundInterceptorBase
type ExecuteWorkflowInput = workflow.ExecuteWorkflowInput
type HandleSignalInput = workflow.HandleSignalInput
type HandleQueryInput = workflow.HandleQueryInput
type UpdateInput = workflow.UpdateInput
type ExecuteNexusOperationInput = workflow.ExecuteNexusOperationInput
type RequestCancelNexusOperationInput = workflow.RequestCancelNexusOperationInput

// WorkflowHeader exposes the private header map for the current intercepted
// operation. Changes are propagated to the corresponding outbound command.
func WorkflowHeader(ctx context.Context) map[string]*commonpb.Payload {
	return workflow.InterceptorHeader(ctx)
}
