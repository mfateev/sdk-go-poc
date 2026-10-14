// Package continuation exercises SDK-compatible continuation and versioning.
package continuation

import (
	"context"
	"errors"
	"fmt"
	"github.com/mfateev/sdk-go-poc/workflow"
)

var runs int

//temporal:workflow
func ContinueWorkflow(ctx context.Context, remaining int) (int, error) {
	runs++
	if runs != 1 {
		return 0, errors.New("continue-as-new reused isolate state")
	}
	if remaining > 0 {
		err := workflow.NewContinueAsNewError(ctx, ContinueWorkflow, remaining-1)
		if !workflow.IsContinueAsNewError(err) {
			return 0, errors.New("SDK error identity lost")
		}
		return 0, fmt.Errorf("wrapped continuation: %w", err)
	}
	return runs, nil
}

//temporal:workflow
func LegacyVersionWorkflow(ctx context.Context) (int, error) { return 0, nil }

//temporal:workflow
func VersionWorkflow(ctx context.Context) (int, error) {
	// Replay state is available for diagnostics; never use it to choose commands.
	_ = workflow.IsReplaying(ctx)
	version := workflow.GetVersion(ctx, "poc-change", workflow.DefaultVersion, 1)
	again := workflow.GetVersion(ctx, "poc-change", workflow.DefaultVersion, 2)
	if again != version {
		return 0, errors.New("version changed during one execution")
	}
	if version == workflow.DefaultVersion {
		return 0, nil
	}
	return int(version), nil
}

//temporal:workflow
func UnsupportedVersionWorkflow(ctx context.Context) (int, error) {
	return int(workflow.GetVersion(ctx, "poc-change", 2, 2)), nil
}
