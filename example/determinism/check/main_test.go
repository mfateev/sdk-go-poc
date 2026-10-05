package main

import (
	"bytes"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"

	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
	"google.golang.org/protobuf/encoding/protojson"
)

func TestSavedHistoryChecksComputedTrace(t *testing.T) {
	binary := filepath.Join(t.TempDir(), "replay")
	build := exec.Command(filepath.Join(runtime.GOROOT(), "bin", "go"), "build", "-o", binary, ".")
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build: %v\n%s", err, output)
	}
	filename := "../testdata/history.json"
	if output, err := exec.Command(binary, "-history", filename).CombinedOutput(); err != nil {
		t.Fatalf("saved history: %v\n%s", err, output)
	}
	data, err := os.ReadFile(filename)
	if err != nil {
		t.Fatal(err)
	}
	history, err := client.HistoryFromJSON(bytes.NewReader(data), client.HistoryJSONOptions{})
	if err != nil {
		t.Fatal(err)
	}
	dc := converter.GetDefaultDataConverter()
	changed := false
	for _, event := range history.Events {
		if result := event.GetActivityTaskCompletedEventAttributes(); result != nil {
			var trace []string
			if err := dc.FromPayloads(result.Result, &trace); err != nil {
				t.Fatal(err)
			}
			trace[0] = "corrupted recorded observation"
			result.Result, err = dc.ToPayloads(trace)
			if err != nil {
				t.Fatal(err)
			}
			// Change both recorded results. A checker that only returns the
			// activity result and compares it to completion would falsely pass.
			history.Events[len(history.Events)-1].GetWorkflowExecutionCompletedEventAttributes().Result = result.Result
			changed = true
		}
	}
	if !changed {
		t.Fatal("history has no recorded activity result")
	}
	data, err = protojson.Marshal(history)
	if err != nil {
		t.Fatal(err)
	}
	corrupted := filepath.Join(t.TempDir(), "corrupted.json")
	if err := os.WriteFile(corrupted, data, 0600); err != nil {
		t.Fatal(err)
	}
	if output, err := exec.Command(binary, "-history", corrupted).CombinedOutput(); err == nil {
		t.Fatalf("corrupted trace passed replay:\n%s", output)
	} else if !bytes.Contains(output, []byte("determinism trace changed")) {
		t.Fatalf("replay failed without identifying the changed trace: %v\n%s", err, output)
	}
}
