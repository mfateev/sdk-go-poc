package workflow

import (
	"context"
	"go.temporal.io/sdk/temporal"
	"time"
)

// RunOptions is copied host metadata and continuation configuration. It is an
// internal wire envelope; contexts and SDK objects never cross the boundary.
type RunOptions struct {
	Namespace                string                    `json:"namespace,omitempty"`
	TaskQueue                string                    `json:"task_queue,omitempty"`
	WorkflowID               string                    `json:"workflow_id,omitempty"`
	WorkflowExecutionTimeout time.Duration             `json:"execution_timeout,omitempty"`
	WorkflowRunTimeout       time.Duration             `json:"run_timeout,omitempty"`
	WorkflowTaskTimeout      time.Duration             `json:"task_timeout,omitempty"`
	VersioningIntent         temporal.VersioningIntent `json:"versioning_intent,omitempty"`
	Priority                 Priority                  `json:"priority,omitempty"`
}
type runOptionsKey struct{}

func runOptions(ctx context.Context) RunOptions {
	o, _ := ctx.Value(runOptionsKey{}).(RunOptions)
	return o
}
func withRunOption(ctx context.Context, change func(*RunOptions)) context.Context {
	o := runOptions(ctx)
	change(&o)
	return context.WithValue(ctx, runOptionsKey{}, o)
}
func WithWorkflowNamespace(ctx context.Context, name string) context.Context {
	return withRunOption(ctx, func(o *RunOptions) { o.Namespace = name })
}
func WithWorkflowTaskQueue(ctx context.Context, name string) context.Context {
	if name == "" {
		panic("empty task queue name")
	}
	return withRunOption(ctx, func(o *RunOptions) { o.TaskQueue = name })
}
func WithWorkflowID(ctx context.Context, id string) context.Context {
	return withRunOption(ctx, func(o *RunOptions) { o.WorkflowID = id })
}
func WithWorkflowRunTimeout(ctx context.Context, d time.Duration) context.Context {
	return withRunOption(ctx, func(o *RunOptions) { o.WorkflowRunTimeout = d })
}
func WithWorkflowTaskTimeout(ctx context.Context, d time.Duration) context.Context {
	return withRunOption(ctx, func(o *RunOptions) { o.WorkflowTaskTimeout = d })
}
func WithWorkflowVersioningIntent(ctx context.Context, intent temporal.VersioningIntent) context.Context {
	return withRunOption(ctx, func(o *RunOptions) { o.VersioningIntent = intent })
}
func WithWorkflowPriority(ctx context.Context, priority Priority) context.Context {
	return withRunOption(ctx, func(o *RunOptions) { o.Priority = priority })
}
