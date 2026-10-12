package main

import (
	"encoding/hex"
	"fmt"
	example "github.com/mfateev/sdk-go-poc/example/random"
	"github.com/mfateev/sdk-go-poc/worker"
	sdkwf "go.temporal.io/sdk/workflow"
	"strings"
	"time"
)

func checkByteStreams() {
	const golden = "10861bf410d33891bef9b1f2ebddc1af2f5bceffe86c13fdcb8534a08805b1a7"
	for _, tc := range []struct{ mode, want string }{{"bytes", golden}, {"uint64", "9138d310f41b8610"}, {"interleaved", golden[:32]}} {
		registration := new(registrations)
		worker.Wrap(registration).RegisterWorkflow(example.GoldenStreamsWorkflow)
		e := &environment{info: sdkwf.Info{OriginalRunID: "runID", WorkflowExecution: sdkwf.Execution{RunID: "synthetic"}}}
		d := registration.factory.NewWorkflowDefinition()
		func() {
			defer d.Close()
			input, err := e.GetDataConverter().ToPayloads(tc.mode)
			must(err)
			d.Execute(e, nil, input)
			d.OnWorkflowTaskStarted(5 * time.Second)
			must(e.err)
			var value string
			must(e.GetDataConverter().FromPayloads(e.result, &value))
			if value != tc.want {
				panic(fmt.Sprintf("upstream golden %s: got %s want %s", tc.mode, value, tc.want))
			}
		}()
	}
	registration := new(registrations)
	worker.Wrap(registration).RegisterWorkflow(example.ByteStreamsWorkflow)
	e := &environment{info: sdkwf.Info{OriginalRunID: "runID", WorkflowExecution: sdkwf.Execution{RunID: "synthetic"}}}
	d := registration.factory.NewWorkflowDefinition()
	defer d.Close()
	d.Execute(e, nil, nil)
	d.OnWorkflowTaskStarted(5 * time.Second)
	var queried string
	for i := range 4 {
		result, err := e.query("next-bytes", nil, nil)
		must(err)
		var value string
		must(e.GetDataConverter().FromPayloads(result, &value))
		if i > 0 && value != queried {
			panic("byte query changed leftover state")
		}
		queried = value
	}
	if _, err := e.query("bad-bytes", nil, nil); err == nil || !strings.Contains(err.Error(), "read-only") {
		panic(fmt.Sprintf("byte read bypassed ownership protection: %v", err))
	}
	e.activity(e.input, nil)
	d.OnWorkflowTaskStarted(5 * time.Second)
	must(e.err)
	var trace example.ByteTrace
	must(e.GetDataConverter().FromPayloads(e.result, &trace))
	if trace.After != queried {
		panic("scratch byte query advanced workflow state")
	}
	bytes, err := hex.DecodeString(trace.Before + trace.After)
	must(err)
	if len(bytes) != 19 {
		panic("byte stream length changed")
	}
}
