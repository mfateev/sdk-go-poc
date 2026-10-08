package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/mfateev/sdk-go-poc/example/continuation"
	"github.com/mfateev/sdk-go-poc/worker"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
	sdkwf "go.temporal.io/sdk/workflow"
)

func checkHistories(dir string) error {
	for _, name := range []string{"legacy", "version", "continued", "final"} {
		f, err := os.Open(filepath.Join(dir, name+".json"))
		if err != nil {
			return err
		}
		history, err := client.HistoryFromJSON(f, client.HistoryJSONOptions{})
		f.Close()
		if err != nil {
			return err
		}
		var fn any = continuation.VersionWorkflow
		workflowName := "VersionAlias"
		if name == "continued" || name == "final" {
			fn = continuation.ContinueWorkflow
			workflowName = "ContinuedAlias"
		}
		r := worker.NewWorkflowReplayer()
		r.RegisterWorkflowWithOptions(fn, sdkwf.RegisterOptions{Name: workflowName})
		if err := r.ReplayWorkflowHistory(nil, history); err != nil {
			return fmt.Errorf("%s replay: %w", name, err)
		}
		if name != "continued" {
			var expected, actual int
			completion := history.Events[len(history.Events)-1].GetWorkflowExecutionCompletedEventAttributes()
			if completion == nil {
				return fmt.Errorf("%s history is not completed", name)
			}
			if err := converter.GetDefaultDataConverter().FromPayloads(completion.Result, &expected); err != nil {
				return err
			}
			if err := r.(interface{ GetWorkflowResult(string, any) error }).GetWorkflowResult("", &actual); err != nil {
				return err
			}
			if actual != expected {
				return fmt.Errorf("%s computed result %d differs from history %d", name, actual, expected)
			}
		}
		if name == "version" {
			unsupported := worker.NewWorkflowReplayer()
			unsupported.RegisterWorkflowWithOptions(continuation.UnsupportedVersionWorkflow, sdkwf.RegisterOptions{Name: workflowName})
			err := unsupported.ReplayWorkflowHistory(nil, history)
			if err == nil || !strings.Contains(err.Error(), "[TMPRL1100] Workflow code removed support of version 1") || !strings.Contains(err.Error(), `"poc-change"`) {
				return fmt.Errorf("unsupported recorded version accepted or lost range diagnostic: %v", err)
			}
		}
	}
	fmt.Println("saved histories: default version, recorded marker, unsupported range and continuation replay passed")
	return nil
}
