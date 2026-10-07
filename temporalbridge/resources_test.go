package temporalbridge

import (
	"context"
	"errors"
	"isolate"
	"testing"
	"time"
)

func TestResourcesStartupDeadlinePreservesDiagnostics(t *testing.T) {
	budget := resourceTaskBudget{start: time.Now().Add(-time.Second), options: ResourceOptions{MaxTaskDuration: time.Millisecond}}
	startup := &isolate.InitializationError{Cause: context.DeadlineExceeded, Pending: &isolate.KillPendingError{LiveGoroutines: 1, Stack: "initializer stack"}}
	err := budget.startupError(startup)
	var resource *isolate.ResourceLimitError
	if !errors.As(err, &resource) || resource.Resource != "task duration" || resource.Stack != startup.Pending.Stack || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("startup=%v", err)
	}
	d := &definition{env: new(effectEnvironment)}
	func() {
		defer func() {
			failure, ok := recover().(*WorkflowTaskError)
			if !ok || !errors.As(failure, &resource) {
				t.Errorf("task failure=%v", failure)
			}
		}()
		d.fail(err)
	}()
	if d.StackTrace() != startup.Pending.Stack {
		t.Fatal("lost startup resource diagnostics")
	}
	prior := &isolate.ResourceLimitError{Resource: "memory", Limit: 1, Usage: 2}
	if budget.startupError(prior) != prior {
		t.Fatal("deadline replaced earlier native limit")
	}
}
func TestResourcesOptionsSnapshotPerDefinition(t *testing.T) {
	options := ResourceOptions{Limits: isolate.ResourceLimits{MaxGoroutines: 10}}
	factory := Factory{ResolveResourceOptions: func() ResourceOptions { return options }}
	first := factory.NewWorkflowDefinition().(*definition)
	options.Limits.MaxGoroutines = 20
	second := factory.NewWorkflowDefinition().(*definition)
	if first.resources.Limits.MaxGoroutines != 10 || second.resources.Limits.MaxGoroutines != 20 {
		t.Fatal("configuration changed cached execution policy")
	}
}
