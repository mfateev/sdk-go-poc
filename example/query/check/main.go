package main

import (
	"flag"
	"fmt"
	"isolate"
	"regexp"
	"runtime"
	"slices"
	"strings"
	"time"

	"github.com/mfateev/sdk-go-poc/example/query"
	"github.com/mfateev/sdk-go-poc/temporalbridge"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/sdk/client"
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

func (e *environment) stackTrace() string {
	output, err := e.query(client.QueryTypeStackTrace, nil, nil)
	check(err)
	var stack string
	check(e.GetDataConverter().FromPayloads(output, &stack))
	if strings.Contains(stack, "main.run(") || strings.Contains(stack, "main.(*environment)") {
		panic("stack query included host goroutines")
	}
	return stack
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
	scenario := flag.String("scenario", "queries", "queries, dispatch-close or stack-depth")
	flag.Parse()
	if *coldID != "" {
		check(checkCold(*address, *queue, *coldID))
		return
	}
	for _, procs := range []int{1, 2, 8} {
		runtime.GOMAXPROCS(procs)
		switch *scenario {
		case "queries":
			run()
			runStackQuery()
		case "dispatch-close":
			runDispatchClose()
		case "stack-depth":
			runStackDepth()
		default:
			panic("unknown query checker scenario: " + *scenario)
		}
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
	definitions := make([]bindings.WorkflowDefinition, 3)
	envs := make([]*environment, 3)
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
	var baselineRandom []uint64
	for i, e := range envs {
		// The first instance receives no queries while running. Compare its
		// workflow draws with instances subjected to valid and invalid queries.
		if i != 0 {
			stack := e.stackTrace()
			if !strings.Contains(stack, "query.QueryWorkflow(") || strings.Contains(stack, "workflow.serveQueries()") {
				panic("stack query omitted workflow or included query service")
			}
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
		}
		e.now = e.now.Add(time.Minute)
		e.timer(nil, nil)
		definitions[i].OnWorkflowTaskStarted(5 * time.Second)
		if e.complete != 1 {
			panic("workflow did not complete once")
		}
		check(e.err)
		if stack := e.stackTrace(); stack != "" {
			panic("completed stack query retained SDK service goroutines: " + stack)
		}
		var result int
		check(e.GetDataConverter().FromPayloads(e.result, &result))
		if result != 11+i {
			panic("wrong completion")
		}
		snapshot, err := e.query("random-state", nil, nil)
		check(err)
		var draws []uint64
		check(e.GetDataConverter().FromPayloads(snapshot, &draws))
		if len(draws) != 2 || draws[0] == draws[1] {
			panic("workflow random stream did not advance")
		}
		if baselineRandom == nil {
			baselineRandom = draws
		} else if !slices.Equal(draws, baselineRandom) {
			panic("queries changed workflow random stream")
		}
		for range 8 {
			e.queryState(11 + i)
		}
		definitions[i].OnWorkflowTaskStarted(time.Second)
		if e.complete != 1 {
			panic("query repeated completion")
		}
		definitions[i].Close()
		if _, err := e.query(client.QueryTypeStackTrace, nil, nil); err == nil {
			panic("evicted stacks remained queryable")
		}
		if _, err := e.query("state", nil, nil); err == nil {
			panic("evicted state remained queryable")
		}
	}
}

func runStackQuery() {
	h, ok := isolate.LookupFunction(query.StackWorkflow)
	if !ok {
		panic("missing stack workflow")
	}
	e := &environment{now: time.Date(2025, 1, 2, 0, 0, 0, 0, time.UTC)}
	d := (temporalbridge.Factory{Function: h}).NewWorkflowDefinition()
	defer d.Close()
	d.Execute(e, nil, nil)
	d.OnWorkflowTaskStarted(5 * time.Second)
	for range 3 {
		stack := e.stackTrace()
		if !strings.Contains(stack, "query.StackWorkflow(") || strings.Count(stack, "query.stackWait(") != 2 || strings.Contains(stack, "workflow.serveQueries()") {
			panic("stack query without registered handlers returned wrong goroutines: " + stack)
		}
		if d.StackTrace() != stack {
			panic("SDK stack trace differs from built-in query")
		}
	}
	if e.timer == nil || e.complete != 0 {
		panic("stack query changed workflow progress")
	}
	e.now = e.now.Add(time.Minute)
	e.timer(nil, nil)
	d.OnWorkflowTaskStarted(5 * time.Second)
	if e.complete != 1 {
		panic("stack-query workflow did not complete")
	}
	check(e.err)
}

func runDispatchClose() {
	h, ok := isolate.LookupFunction(query.DispatchCloseWorkflow)
	if !ok {
		panic("missing dispatch-close workflow")
	}
	e := &environment{now: time.Date(2025, 1, 2, 0, 0, 0, 0, time.UTC)}
	d := (temporalbridge.Factory{Function: h}).NewWorkflowDefinition()
	defer d.Close()
	resources := d.(interface{ Resources() isolate.ResourceStats })
	if resources.Resources().LiveGoroutines != 0 {
		panic("workflow executed before dispatch")
	}
	d.Execute(e, nil, nil)
	d.OnWorkflowTaskStarted(5 * time.Second)
	if e.complete != 0 {
		panic("blocked workflow completed")
	}
	stack := e.stackTrace()
	if count := len(regexp.MustCompile(`(?m)^goroutine [0-9]+ \[`).FindAllString(stack, -1)); count != 11 {
		panic(fmt.Sprintf("dispatch-close stacks=%d, want root and ten children: %s", count, stack))
	}
	if !strings.Contains(stack, "query.DispatchCloseWorkflow(") || strings.Count(stack, "query.dispatchCloseChild(") != 10 {
		panic("dispatch-close dump omitted blocked root or children: " + stack)
	}
	for _, goroutine := range strings.Split(stack, "\n\ngoroutine ") {
		if !strings.Contains(goroutine, "[chan receive]:") {
			panic("dispatch-close goroutine was not blocked on receive: " + goroutine)
		}
	}
	output, err := e.query("history", nil, nil)
	check(err)
	var history []string
	check(e.GetDataConverter().FromPayloads(output, &history))
	if !slices.Equal(history, []string{"root"}) {
		panic(fmt.Sprintf("dispatch-close history=%v, want root only", history))
	}
	before := resources.Resources().LiveGoroutines
	d.Close()
	after := resources.Resources().LiveGoroutines
	if before-after < 11 || after != 0 || e.complete != 0 {
		panic(fmt.Sprintf("dispatch close did not drain blocked goroutines: before=%d after=%d completes=%d", before, after, e.complete))
	}
	if _, err := e.query(client.QueryTypeStackTrace, nil, nil); err == nil {
		panic("closed workflow stack remained queryable")
	}
}

func runStackDepth() {
	h, ok := isolate.LookupFunction(query.StackDepthWorkflow)
	if !ok {
		panic("missing stack-depth workflow")
	}
	for _, depth := range []int{2, 100} {
		e := &environment{now: time.Date(2025, 1, 2, 0, 0, 0, 0, time.UTC)}
		d := (temporalbridge.Factory{Function: h}).NewWorkflowDefinition()
		input, err := e.GetDataConverter().ToPayloads(depth)
		check(err)
		d.Execute(e, nil, input)
		d.OnWorkflowTaskStarted(5 * time.Second)
		stack := e.stackTrace()
		// The native API exposes no frame-omission depth. Require a readable
		// full trace, retaining both the blocked leaf and workflow entry;
		// deep traces use Go's bounded traceback with an explicit elision.
		if !strings.Contains(stack, "time.Sleep(") || !strings.Contains(stack, "query.StackDepthWorkflow(") {
			panic("stack-depth trace lost blocking leaf or workflow entry: " + stack)
		}
		frames := strings.Count(stack, "query.stackTraceDepth(")
		lines := len(strings.Split(stack, "\n"))
		if depth == 2 && (frames != 3 || lines <= 3 || lines >= 100) {
			panic(fmt.Sprintf("shallow native trace: frames=%d lines=%d", frames, lines))
		}
		if depth == 100 && (frames <= 3 || frames >= 101 || lines > 210 || !strings.Contains(stack, "frames elided")) {
			panic(fmt.Sprintf("deep native trace was not bounded: frames=%d lines=%d\n%s", frames, lines, stack))
		}
		if e.complete != 0 || e.timer == nil {
			panic("stack-depth query advanced workflow")
		}
		d.Close()
	}
}

func (*environment) RegisterUpdateHandler(func(string, string, *commonpb.Payloads, *commonpb.Header, bindings.UpdateCallbacks)) {
}
