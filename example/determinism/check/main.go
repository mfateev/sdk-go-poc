// Replay a saved determinism history and compare its complete result.
package main

import (
	"crypto/sha256"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"slices"

	"github.com/mfateev/sdk-go-poc/example/determinism"
	"github.com/mfateev/sdk-go-poc/worker"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
)

func main() {
	history := flag.String("history", "example/determinism/testdata/history-shared-random.json", "CLI-exported completed history")
	flag.Parse()
	if err := check(*history); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func check(filename string) error {
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
		return fmt.Errorf("history is empty")
	}
	completion := history.Events[len(history.Events)-1].GetWorkflowExecutionCompletedEventAttributes()
	if completion == nil {
		return fmt.Errorf("history has no completed workflow result")
	}
	var expected []string
	if err := converter.GetDefaultDataConverter().FromPayloads(completion.Result, &expected); err != nil {
		return err
	}
	r := worker.NewWorkflowReplayer()
	r.RegisterWorkflow(determinism.DeterminismWorkflow)
	r.RegisterActivity(determinism.RecordTrace)
	if err := r.ReplayWorkflowHistory(nil, history); err != nil {
		return err
	}
	getter, ok := r.(interface{ GetWorkflowResult(string, any) error })
	if !ok {
		return fmt.Errorf("replayer does not expose workflow results")
	}
	var actual []string
	if err := getter.GetWorkflowResult("", &actual); err != nil {
		return err
	}
	if !slices.Equal(actual, expected) {
		for i := 0; i < min(len(actual), len(expected)); i++ {
			if actual[i] != expected[i] {
				return fmt.Errorf("trace changed at %d: got %q, history has %q", i, actual[i], expected[i])
			}
		}
		return fmt.Errorf("trace length changed: got %d, history has %d", len(actual), len(expected))
	}
	data, err := json.Marshal(actual)
	if err != nil {
		return err
	}
	fmt.Printf("determinism replay passed: %d observations, sha256 %x\n", len(actual), sha256.Sum256(data))
	return nil
}
