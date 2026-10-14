package dynamic

import (
	"context"
	"errors"

	"github.com/mfateev/sdk-go-poc/interceptor"
	"go.temporal.io/sdk/converter"
)

//temporal:workflow
func NewInterceptors([]byte) ([]interceptor.WorkerInterceptor, error) {
	return []interceptor.WorkerInterceptor{&dynamicInterceptor{}}, nil
}

type dynamicInterceptor struct {
	interceptor.WorkerInterceptorBase
}

func (*dynamicInterceptor) InterceptWorkflow(_ context.Context, next interceptor.WorkflowInboundInterceptor) interceptor.WorkflowInboundInterceptor {
	return &dynamicInbound{WorkflowInboundInterceptorBase: interceptor.WorkflowInboundInterceptorBase{Next: next}}
}

type dynamicInbound struct {
	interceptor.WorkflowInboundInterceptorBase
}

func (in *dynamicInbound) ExecuteWorkflow(ctx context.Context, input *interceptor.ExecuteWorkflowInput) (any, error) {
	if len(input.Args) != 1 {
		return nil, errors.New("dynamic workflow interceptor arguments missing")
	}
	if _, ok := input.Args[0].(converter.EncodedValues); !ok {
		return nil, errors.New("dynamic workflow interceptor needs encoded values")
	}
	return in.Next.ExecuteWorkflow(ctx, input)
}
func checkDynamicArguments(name string, args []any) error {
	if name == "named" {
		return nil
	}
	if len(args) != 2 || args[0] != name {
		return errors.New("dynamic handler interceptor lost requested name")
	}
	if _, ok := args[1].(converter.EncodedValues); !ok {
		return errors.New("dynamic handler interceptor needs encoded values")
	}
	return nil
}
func (in *dynamicInbound) HandleQuery(ctx context.Context, input *interceptor.HandleQueryInput) (any, error) {
	if err := checkDynamicArguments(input.QueryType, input.Args); err != nil {
		return nil, err
	}
	if input.QueryType == "intercepted" {
		return "query intercepted", nil
	}
	return in.Next.HandleQuery(ctx, input)
}
func (in *dynamicInbound) ValidateUpdate(ctx context.Context, input *interceptor.UpdateInput) error {
	if err := checkDynamicArguments(input.Name, input.Args); err != nil {
		return err
	}
	return in.Next.ValidateUpdate(ctx, input)
}
func (in *dynamicInbound) ExecuteUpdate(ctx context.Context, input *interceptor.UpdateInput) (any, error) {
	if err := checkDynamicArguments(input.Name, input.Args); err != nil {
		return nil, err
	}
	return in.Next.ExecuteUpdate(ctx, input)
}
