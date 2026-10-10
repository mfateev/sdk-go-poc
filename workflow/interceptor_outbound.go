package workflow

import (
	"context"
	"isolate"
	"time"

	"github.com/mfateev/sdk-go-poc/internal/activityref"
)

func operationContext(ctx context.Context) context.Context {
	out, err := withInterceptorHeader(ctx, nil)
	if err != nil {
		panic(err)
	}
	return out
}

func GetCurrentUpdateInfo(ctx context.Context) *UpdateInfo {
	if ctx != nil {
		if out := currentOutbound(ctx); out != nil {
			return out.GetCurrentUpdateInfo(ctx)
		}
	}
	return getCurrentUpdateInfo(ctx)
}
func Sleep(ctx context.Context, duration time.Duration) error {
	assertWritable()
	if ctx != nil {
		if out := currentOutbound(ctx); out != nil {
			return out.Sleep(ctx, duration)
		}
	}
	return sleep(ctx, duration)
}
func RequestCancelExternalWorkflow(ctx context.Context, workflowID, runID string) Future {
	assertWritable()
	if ctx != nil {
		if out := currentOutbound(ctx); out != nil {
			return out.RequestCancelExternalWorkflow(ctx, workflowID, runID)
		}
	}
	return requestCancelExternalWorkflow(ctx, workflowID, runID)
}
func SignalExternalWorkflow(ctx context.Context, workflowID, runID, signalName string, arg any) Future {
	assertWritable()
	if ctx != nil {
		if out := currentOutbound(ctx); out != nil {
			ctx = operationContext(ctx)
			return out.SignalExternalWorkflow(ctx, workflowID, runID, signalName, arg)
		}
	}
	return signalExternalWorkflow(ctx, workflowID, runID, signalName, arg)
}
func GetSignalChannel(ctx context.Context, signalName string) <-chan SignalResult {
	assertWritable()
	if ctx != nil {
		if out := currentOutbound(ctx); out != nil {
			return out.GetSignalChannel(ctx, signalName)
		}
	}
	return getSignalChannel(ctx, signalName)
}
func GetVersion(ctx context.Context, changeID string, minSupported, maxSupported Version) Version {
	assertWritable()
	if ctx != nil {
		if out := currentOutbound(ctx); out != nil {
			return out.GetVersion(ctx, changeID, minSupported, maxSupported)
		}
	}
	return getVersion(ctx, changeID, minSupported, maxSupported)
}
func SetQueryHandlerWithOptions(ctx context.Context, queryType string, handler any, options QueryHandlerOptions) error {
	assertWritable()
	if ctx != nil {
		if out := currentOutbound(ctx); out != nil {
			return out.SetQueryHandlerWithOptions(ctx, queryType, handler, options)
		}
	}
	return setQueryHandlerWithOptions(ctx, queryType, handler, options)
}
func IsReplaying(ctx context.Context) bool {
	if ctx != nil {
		if out := currentOutbound(ctx); out != nil {
			return out.IsReplaying(ctx)
		}
	}
	return isReplaying(ctx)
}
func NewContinueAsNewError(ctx context.Context, fn any, args ...any) error {
	if ctx != nil {
		if out := currentOutbound(ctx); out != nil {
			ctx = operationContext(ctx)
			return out.NewContinueAsNewError(ctx, fn, args...)
		}
	}
	return newContinueAsNewError(ctx, fn, args...)
}

func SetUpdateHandlerWithOptions(ctx context.Context, name string, handler any, options UpdateHandlerOptions) error {
	assertWritable()
	if ctx != nil {
		if out := currentOutbound(ctx); out != nil {
			return out.SetUpdateHandler(ctx, name, handler, options)
		}
	}
	return setUpdateHandlerWithOptions(ctx, name, handler, options)
}

func interceptedName(ctx context.Context, kind string, fn any, args []any) (context.Context, string, error) {
	name, byName := fn.(string)
	if !byName {
		var err error
		if kind == "activity" {
			name, err = activityref.Name(fn)
		} else {
			name, _, err = validateWorkflowReference(fn, args)
		}
		if err != nil {
			return ctx, "", err
		}
		op := OpResolveActivityName
		if kind == "workflow" {
			op = OpResolveWorkflowName
		}
		raw, err := isolate.Call(op, []byte(name))
		if err != nil {
			return ctx, "", err
		}
		name = string(raw)
	}
	ctx = withInterceptedReference(operationContext(ctx), kind, name, fn)
	return ctx, name, nil
}

func ExecuteActivity(ctx context.Context, activity any, args ...any) Future {
	assertWritable()
	if ctx != nil {
		if out := currentOutbound(ctx); out != nil {
			ctx, name, err := interceptedName(ctx, "activity", activity, args)
			if err != nil {
				return failOperation(newOperationFuture(), err)
			}
			return out.ExecuteActivity(ctx, name, args...)
		}
	}
	return executeActivity(ctx, activity, args...)
}
func ExecuteLocalActivity(ctx context.Context, activity any, args ...any) Future {
	assertWritable()
	if ctx != nil {
		if out := currentOutbound(ctx); out != nil {
			ctx, name, err := interceptedName(ctx, "activity", activity, args)
			if err != nil {
				return failOperation(newOperationFuture(), err)
			}
			return out.ExecuteLocalActivity(ctx, name, args...)
		}
	}
	return executeLocalActivity(ctx, activity, args...)
}
func ExecuteChildWorkflow(ctx context.Context, fn any, args ...any) ChildWorkflowFuture {
	assertWritable()
	if ctx != nil {
		if out := currentOutbound(ctx); out != nil {
			ctx, name, err := interceptedName(ctx, "workflow", fn, args)
			if err != nil {
				return &childFuture{activityFuture: failOperation(newOperationFuture(), err), execution: failOperation(newOperationFuture(), err)}
			}
			return out.ExecuteChildWorkflow(ctx, name, args...)
		}
	}
	return executeChildWorkflow(ctx, fn, args...)
}
