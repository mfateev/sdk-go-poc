// Replay a CLI-exported history with a fresh statically linked isolate.
package main

import (
	"bytes"
	"fmt"
	"isolate"
	"os"

	"github.com/mfateev/sdk-go-poc/temporalbridge"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/worker"
	goWorkflow "go.temporal.io/sdk/workflow"
)

func main() {
	if len(os.Args) != 4 {
		fmt.Fprintln(os.Stderr, "usage: replay <program-name> <workflow-type> <history.json>")
		os.Exit(2)
	}
	program, ok := isolate.LookupProgram(os.Args[1])
	if !ok {
		fmt.Fprintln(os.Stderr, "unknown isolate program:", os.Args[1])
		os.Exit(2)
	}
	workflowName := os.Args[2]
	if workflowName == "" {
		fmt.Fprintln(os.Stderr, "workflow type is empty")
		os.Exit(2)
	}
	replayer := worker.NewWorkflowReplayer()
	replayer.RegisterWorkflowWithOptions(temporalbridge.Factory{Program: program, EntryName: workflowName}, goWorkflow.RegisterOptions{Name: workflowName})
	if err := replayer.ReplayWorkflowHistoryFromJSONFile(nil, os.Args[3]); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if workflowName == "IsolateClock" {
		if err := compareClockResult(replayer, os.Args[3]); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
	}
	fmt.Println("replay passed:", workflowName)
}

func compareClockResult(replayer worker.WorkflowReplayer, filename string) error {
	f, err := os.Open(filename)
	if err != nil {
		return err
	}
	defer f.Close()
	history, err := client.HistoryFromJSON(f, client.HistoryJSONOptions{})
	if err != nil {
		return err
	}
	if len(history.Events) == 0 {
		return fmt.Errorf("clock history is empty")
	}
	completion := history.Events[len(history.Events)-1].GetWorkflowExecutionCompletedEventAttributes()
	if completion == nil || completion.Result == nil {
		return fmt.Errorf("clock history has no completion result")
	}
	var expected []byte
	if err := converter.GetDefaultDataConverter().FromPayloads(completion.Result, &expected); err != nil {
		return err
	}
	getter, ok := replayer.(interface{ GetWorkflowResult(string, any) error })
	if !ok {
		return fmt.Errorf("Temporal replayer does not expose its completion result")
	}
	var actual []byte
	if err := getter.GetWorkflowResult("", &actual); err != nil {
		return err
	}
	if !bytes.Equal(actual, expected) {
		return fmt.Errorf("clock result changed on replay: got %q, history has %q", actual, expected)
	}
	fmt.Println("clock timestamps match history:", string(actual))
	return nil
}
