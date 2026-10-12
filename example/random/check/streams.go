package main

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	rand "math/rand/v2"
	"reflect"
	"slices"
	"strings"
	"time"

	example "github.com/mfateev/sdk-go-poc/example/random"
	"github.com/mfateev/sdk-go-poc/temporalbridge"
	"github.com/mfateev/sdk-go-poc/worker"
	"go.temporal.io/sdk/converter"
	bindings "go.temporal.io/sdk/internalbindings"
	sdkwf "go.temporal.io/sdk/workflow"
)

type rejectedUpdate struct{ err error }

func (r *rejectedUpdate) Accept()             { panic("random validator probe was accepted") }
func (r *rejectedUpdate) Reject(err error)    { r.err = err }
func (r *rejectedUpdate) Complete(any, error) { panic("random validator probe completed") }

var _ bindings.UpdateCallbacks = (*rejectedUpdate)(nil)

func runStreams(factory temporalbridge.Factory, originalRunID, currentRunID, mode string, queries, replay bool) example.StreamTrace {
	e := &environment{replay: replay, info: sdkwf.Info{BinaryChecksum: "synthetic-checksum", Namespace: "namespace", TaskQueueName: "streams", WorkflowType: sdkwf.Type{Name: "StreamsWorkflow"}, WorkflowExecution: sdkwf.Execution{ID: "workflow", RunID: currentRunID}, OriginalRunID: originalRunID}}
	d := factory.NewWorkflowDefinition()
	defer d.Close()
	input, err := e.GetDataConverter().ToPayloads(mode)
	must(err)
	d.Execute(e, nil, input)
	d.OnWorkflowTaskStarted(5 * time.Second)
	if e.activity == nil || e.completes != 0 {
		panic(fmt.Sprintf("named stream activity not scheduled: %v", e.err))
	}
	if queries {
		expected := expectedStream(originalRunID, "primary", 7)[4:7]
		for i := range 4 {
			result, err := e.query("named", nil, nil)
			must(err)
			var values []uint64
			must(e.GetDataConverter().FromPayloads(result, &values))
			if !slices.Equal(values, expected) {
				panic(fmt.Sprintf("query changed stream snapshot: got %v want %v", values, expected))
			}
			result, err = e.query("new-name", nil, nil)
			must(err)
			must(e.GetDataConverter().FromPayloads(result, &values))
			if !slices.Equal(values, expectedStream(originalRunID, "query-only", 2)) {
				panic("scratch lookup did not reuse the generator")
			}
			_, err = e.query("bad-named", nil, nil)
			if err == nil || !strings.Contains(err.Error(), "read-only") {
				panic(fmt.Sprintf("named stream bypassed captured-state protection: %v", err))
			}
			if !replay {
				callback := new(rejectedUpdate)
				e.update("probe", fmt.Sprint(i), nil, nil, callback)
				d.OnWorkflowTaskStarted(5 * time.Second)
				if callback.err == nil || !strings.Contains(callback.err.Error(), "probe rejected") {
					panic(fmt.Sprintf("validator result: %v", callback.err))
				}
			}
		}
	}
	e.activity(e.input, nil)
	d.OnWorkflowTaskStarted(5 * time.Second)
	if e.completes != 1 {
		panic("named stream workflow did not complete")
	}
	must(e.err)
	var trace example.StreamTrace
	must(e.GetDataConverter().FromPayloads(e.result, &trace))
	if queries {
		result, err := e.query("named", nil, nil)
		must(err)
		var values []uint64
		must(e.GetDataConverter().FromPayloads(result, &values))
		if !slices.Equal(values, expectedStream(originalRunID, "primary", 10)[7:]) {
			panic("completed query changed stream position")
		}
	}
	return trace
}

func expectedStream(runID, name string, count int) []uint64 {
	data := []byte("isolate/workflow-random-stream/v1")
	data = binary.LittleEndian.AppendUint64(data, uint64(len(runID)))
	data = append(data, runID...)
	data = binary.LittleEndian.AppendUint64(data, uint64(len(name)))
	data = append(data, name...)
	source := rand.NewChaCha8(sha256.Sum256(data))
	values := make([]uint64, count)
	for i := range values {
		values[i] = source.Uint64()
	}
	return values
}

func checkStreams() {
	checkAcceptedStreamUpdates()
	registration := new(registrations)
	w := worker.Wrap(registration)
	w.RegisterWorkflow(example.StreamsWorkflow)
	w.RegisterActivity(example.EchoStream)
	baseline := runStreams(registration.factory, "original", "live", "normal", false, false)
	if !slices.Equal(baseline.Primary, expectedStream("original", "primary", 7)) {
		panic("persistent stream sequence changed")
	}
	expected := expectedStream("original", "concurrent", 256)
	slices.Sort(expected)
	if !slices.Equal(baseline.Concurrent, expected) {
		panic("concurrent readers lost or duplicated draws")
	}
	for _, scenario := range []struct {
		mode            string
		queries, replay bool
	}{
		{"normal", true, false}, {"normal", true, true}, {"other-noise", true, false}, {"other-noise", true, true},
	} {
		trace := runStreams(registration.factory, "original", "synthetic-replay", scenario.mode, scenario.queries, scenario.replay)
		if !reflect.DeepEqual(trace, baseline) {
			panic(fmt.Sprintf("queries, validators, unrelated draws or eviction changed named streams: %#v", trace))
		}
	}
	defaultOnly := runStreams(registration.factory, "original", "live", "default-only", false, false)
	if !slices.Equal(defaultOnly.Default, baseline.Default) {
		panic("named stream creation or draws consumed default randomness")
	}
	another := runStreams(registration.factory, "another-original", "live", "normal", false, false)
	if slices.Equal(another.Primary, baseline.Primary) {
		panic("different executions reused named streams")
	}
	fallback := runStreams(registration.factory, "", "original", "normal", false, false)
	if !reflect.DeepEqual(fallback, baseline) {
		panic("missing original identity did not use the current run ID")
	}
	fmt.Printf("named streams passed: primary=%v default=%v\n", baseline.Primary, baseline.Default)
}

type acceptedDraw struct {
	accepted, completed bool
	value               uint64
	dc                  converter.DataConverter
}

func (a *acceptedDraw) Accept()          { a.accepted = true }
func (a *acceptedDraw) Reject(err error) { panic(err) }
func (a *acceptedDraw) Complete(value any, err error) {
	must(err)
	raw := value.(converter.RawValue)
	must(a.dc.FromPayload(raw.Payload(), &a.value))
	a.completed = true
}
func checkAcceptedStreamUpdates() {
	registration := new(registrations)
	worker.Wrap(registration).RegisterWorkflow(example.UpdateStreamsWorkflow)
	for _, replay := range []bool{false, true} {
		e := &environment{replay: replay, info: sdkwf.Info{WorkflowType: sdkwf.Type{Name: "UpdateStreamsWorkflow"}, TaskQueueName: "streams", WorkflowExecution: sdkwf.Execution{ID: "workflow", RunID: fmt.Sprint(replay)}, OriginalRunID: "original"}}
		definition := registration.factory.NewWorkflowDefinition()
		func() {
			defer definition.Close()
			definition.Execute(e, nil, nil)
			definition.OnWorkflowTaskStarted(5 * time.Second)
			if e.activity == nil {
				panic("accepted-update fixture did not suspend")
			}
			expected := expectedStream("original", "primary", 5)
			for i := range 3 {
				callback := &acceptedDraw{dc: e.GetDataConverter()}
				e.update("draw", fmt.Sprint(i), nil, nil, callback)
				definition.OnWorkflowTaskStarted(5 * time.Second)
				if !callback.accepted || !callback.completed || callback.value != expected[i+1] {
					panic(fmt.Sprintf("accepted update draw %d advanced incorrectly: %v", i, callback))
				}
			}
			e.activity(e.input, nil)
			definition.OnWorkflowTaskStarted(5 * time.Second)
			must(e.err)
			var trace example.StreamTrace
			must(e.GetDataConverter().FromPayloads(e.result, &trace))
			if !slices.Equal(trace.Primary, []uint64{expected[0], expected[4]}) {
				panic("validator advanced the accepted-update stream")
			}
		}()
	}
}
