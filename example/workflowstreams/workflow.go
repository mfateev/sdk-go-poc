package workflowstreams

import (
	"context"

	"github.com/mfateev/sdk-go-poc/contrib/workflowstreams"
	"github.com/mfateev/sdk-go-poc/workflow"
)

//temporal:workflow
func Host(ctx context.Context, state *workflowstreams.WorkflowStreamState) (int64, error) {
	stream, err := workflowstreams.NewWorkflowStream(ctx, state)
	if err != nil {
		return 0, err
	}
	if err := workflow.SetUpdateHandler(ctx, "truncate", func(_ context.Context, offset int64) error { return stream.Truncate(offset) }); err != nil {
		return 0, err
	}
	commands := workflow.GetSignalChannel[string](ctx, "command")
	for command := range commands {
		switch command {
		case "rollover":
			return 0, stream.NewContinueAsNewError(ctx, Host, func(state *workflowstreams.WorkflowStreamState) []any { return []any{state} })
		case "finish":
			stream.DetachPollers()
			if err := workflow.WaitHandlersFinished(ctx); err != nil {
				return 0, err
			}
			snapshot, err := stream.GetState(0)
			if err != nil {
				return 0, err
			}
			return snapshot.BaseOffset + int64(len(snapshot.Log)), nil
		}
	}
	return 0, ctx.Err()
}
