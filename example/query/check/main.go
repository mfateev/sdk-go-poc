package main

import (
	"flag"
	"fmt"
	"isolate"
	"runtime"
	"strings"
	"time"

	"github.com/mfateev/sdk-go-poc/example/query"
	"github.com/mfateev/sdk-go-poc/temporalbridge"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/sdk/converter"
	bindings "go.temporal.io/sdk/internalbindings"
	goWorkflow "go.temporal.io/sdk/workflow"
)

type environment struct {
	bindings.WorkflowEnvironment
	query    func(string, *commonpb.Payloads, *commonpb.Header) (*commonpb.Payloads, error)
	signal   func(string, *commonpb.Payloads, *commonpb.Header) error
	timer    bindings.ResultHandler
	complete int
	result   *commonpb.Payloads
	err      error
	now      time.Time
}

func (*environment) RegisterCancelHandler(func()) {}
func (e *environment) RegisterSignalHandler(h func(string, *commonpb.Payloads, *commonpb.Header) error) {
	e.signal = h
}
func (e *environment) RegisterQueryHandler(h func(string, *commonpb.Payloads, *commonpb.Header) (*commonpb.Payloads, error)) {
	e.query = h
}
func (*environment) GetDataConverter() converter.DataConverter {
	return converter.GetDefaultDataConverter()
}
func (*environment) WorkflowInfo() *goWorkflow.Info {
	return &goWorkflow.Info{TaskQueueName: "query-test"}
}
func (e *environment) Now() time.Time { return e.now }
func (e *environment) Complete(result *commonpb.Payloads, err error) {
	e.complete++
	e.result, e.err = result, err
}
func (e *environment) NewTimer(d time.Duration, _ goWorkflow.TimerOptions, callback bindings.ResultHandler) *bindings.TimerID {
	if d != time.Minute {
		panic("query emitted a timer")
	}
	e.timer = callback
	return nil
}
func (e *environment) queryState(expected int) {
	input, err := e.GetDataConverter().ToPayloads("hello")
	check(err)
	output, err := e.query("state", input, nil)
	check(err)
	var value struct {
		Prefix                            string
		Count, Map, Byte, Package, Atomic int
		UTC                               string
	}
	check(e.GetDataConverter().FromPayloads(output, &value))
	if value.Prefix != "hello" || value.Count != expected || value.Map != expected || value.Byte != 7 || value.Package != expected || value.Atomic != expected || value.UTC != "UTC" {
		panic(fmt.Sprintf("state=%+v", value))
	}
}
func check(err error) {
	if err != nil {
		panic(err)
	}
}
func main() {
	address := flag.String("address", "", "optional live Temporal server")
	coldID := flag.String("cold-id", "", "completed workflow to query in this fresh worker process")
	queue := flag.String("queue", "", "task queue for the fresh worker")
	flag.Parse()
	if *coldID != "" {
		check(checkCold(*address, *queue, *coldID))
		return
	}
	for _, procs := range []int{1, 2, 8} {
		runtime.GOMAXPROCS(procs)
		run()
	}
	fmt.Println("running/completed queries, read-only writes and cache eviction passed")
	if *address != "" {
		check(checkLive(*address))
	}
}
func run() {
	h, ok := isolate.LookupFunction(query.QueryWorkflow)
	if !ok {
		panic("missing marked workflow")
	}
	definitions := make([]bindings.WorkflowDefinition, 2)
	envs := make([]*environment, 2)
	for i := range envs {
		e := &environment{now: time.Date(2025, 1, 2, 0, 0, 0, 0, time.UTC)}
		d := (temporalbridge.Factory{Function: h}).NewWorkflowDefinition()
		definitions[i], envs[i] = d, e
		input, err := e.GetDataConverter().ToPayloads(10 + i)
		check(err)
		d.Execute(e, nil, input)
		d.OnWorkflowTaskStarted(5 * time.Second)
		if e.query == nil || e.timer == nil || e.complete != 0 {
			panic("query workflow did not suspend")
		}
	}
	for i, e := range envs {
		e.queryState(10 + i)
		ready, err := e.query("ready", nil, nil)
		check(err)
		var isReady bool
		check(e.GetDataConverter().FromPayloads(ready, &isReady))
		if !isReady {
			panic("query could not inspect future readiness")
		}
		for range 4 {
			_, err := e.query("random", nil, nil)
			check(err)
		}
		for _, mode := range []string{"exit", "goexit", "environment", "mutex", "waitgroup", "cond", "field", "map", "delete", "clear", "slice", "copy", "package", "atomic", "reflect", "callback", "error-callback", "goroutine", "channel", "select", "activity", "child", "timer", "raw", "panic", "error"} {
			input, err := e.GetDataConverter().ToPayloads(mode)
			check(err)
			if _, err := e.query("bad", input, nil); err == nil {
				panic("query mutation accepted: " + mode)
			}
			e.queryState(10 + i)
		}
		if _, err := e.query("unknown", nil, nil); err == nil || !strings.Contains(err.Error(), "KnownQueryTypes") {
			panic("unknown query accepted")
		}
		if _, err := e.query("state", nil, nil); err == nil {
			panic("missing arguments accepted")
		}
		e.queryState(10 + i)
		e.now = e.now.Add(time.Minute)
		e.timer(nil, nil)
		definitions[i].OnWorkflowTaskStarted(5 * time.Second)
		if e.complete != 1 {
			panic("workflow did not complete once")
		}
		check(e.err)
		var result int
		check(e.GetDataConverter().FromPayloads(e.result, &result))
		if result != 11+i {
			panic("wrong completion")
		}
		for range 8 {
			e.queryState(11 + i)
		}
		definitions[i].OnWorkflowTaskStarted(time.Second)
		if e.complete != 1 {
			panic("query repeated completion")
		}
		definitions[i].Close()
		if _, err := e.query("state", nil, nil); err == nil {
			panic("evicted state remained queryable")
		}
	}
}

func (*environment) RegisterUpdateHandler(func(string, string, *commonpb.Payloads, *commonpb.Header, bindings.UpdateCallbacks)) {
}
