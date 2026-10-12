package main

import (
	"errors"
	"flag"
	"fmt"
	"isolate"
	"strings"
	"time"

	"github.com/mfateev/sdk-go-poc/example/converter/custom"
	"github.com/mfateev/sdk-go-poc/example/signals"
	"github.com/mfateev/sdk-go-poc/temporalbridge"
	"github.com/mfateev/sdk-go-poc/worker"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
	bindings "go.temporal.io/sdk/internalbindings"
	"go.temporal.io/sdk/log"
	sdkworker "go.temporal.io/sdk/worker"
	sdkwf "go.temporal.io/sdk/workflow"
)

type registration struct {
	sdkworker.Worker
	factory temporalbridge.Factory
}

func (r *registration) RegisterWorkflowWithOptions(fn any, _ sdkwf.RegisterOptions) {
	r.factory = fn.(temporalbridge.Factory)
}

type environment struct {
	bindings.WorkflowEnvironment
	dc          converter.DataConverter
	signal      func(string, *commonpb.Payloads, *commonpb.Header) error
	cancel      func()
	result      *commonpb.Payloads
	err         error
	completed   bool
	timer       bindings.ResultHandler
	replay      bool
	diagnostics *diagnostics
}

func (*environment) WorkflowInfo() *sdkwf.Info {
	return &sdkwf.Info{WorkflowType: sdkwf.Type{Name: "Workflow"}, WorkflowExecution: sdkwf.Execution{ID: "typed-signals", RunID: "run"}, OriginalRunID: "run", TaskQueueName: "typed-signals"}
}
func (*environment) Now() time.Time                              { return time.Unix(1700000000, 0) }
func (e *environment) GetDataConverter() converter.DataConverter { return e.dc }
func (e *environment) IsReplaying() bool                         { return e.replay }
func (e *environment) GetLogger() log.Logger                     { return e.diagnostics }
func (e *environment) GetMetricsHandler() client.MetricsHandler  { return e.diagnostics }
func (e *environment) RegisterCancelHandler(fn func())           { e.cancel = fn }
func (*environment) RegisterQueryHandler(func(string, *commonpb.Payloads, *commonpb.Header) (*commonpb.Payloads, error)) {
}
func (*environment) RegisterUpdateHandler(func(string, string, *commonpb.Payloads, *commonpb.Header, bindings.UpdateCallbacks)) {
}
func (e *environment) RegisterSignalHandler(fn func(string, *commonpb.Payloads, *commonpb.Header) error) {
	e.signal = fn
}
func (e *environment) NewTimer(_ time.Duration, _ sdkwf.TimerOptions, callback bindings.ResultHandler) *bindings.TimerID {
	e.timer = callback
	id := bindings.TimerID{}
	return &id
}
func (e *environment) Complete(result *commonpb.Payloads, err error) {
	e.result, e.err, e.completed = result, err, true
}
func must(err error) {
	if err != nil {
		panic(err)
	}
}

func check(mode string, configured bool, intercepted bool) {
	r := new(registration)
	w := worker.Wrap(r)
	w.RegisterWorkflow(signals.Workflow)
	if intercepted {
		must(worker.SetIsolateInterceptors(w, worker.InterceptorOptions{Factory: signals.NewInterceptors}))
	}
	dc := converter.GetDefaultDataConverter()
	if configured {
		var err error
		dc, err = custom.New([]byte("typed-signals"))
		must(err)
		must(worker.SetIsolateDataConverter(w, worker.DataConverterOptions{Factory: custom.New, Config: []byte("typed-signals")}))
	}
	e := &environment{dc: dc, diagnostics: new(diagnostics)}
	d := r.factory.NewWorkflowDefinition()
	defer d.Close()
	input, err := dc.ToPayloads(mode)
	must(err)
	d.Execute(e, nil, input)
	var fault any
	want := []signals.Message{{ID: 9007199254740993, Text: "first", Data: []byte{1}}, {ID: -9007199254740993, Text: "second", Data: []byte{2}}, {ID: 3, Text: "third", Data: []byte{3}}}
	func() {
		defer func() { fault = recover() }()
		d.OnWorkflowTaskStarted(5 * time.Second)
		if e.completed {
			panic("workflow completed before receiving signals")
		}
		if mode == "detached" {
			e.cancel()
			d.OnWorkflowTaskStarted(5 * time.Second)
			if e.completed {
				panic("workflow cancellation closed a detached channel")
			}
		}
		if mode == "cancel" {
			e.cancel()
		} else {
			for _, value := range want {
				var payloads *commonpb.Payloads
				if mode == "malformed" {
					bad, err := dc.ToPayloads("wrong signal type")
					must(err)
					must(e.signal("message", bad, nil))
				}
				payloads, err = dc.ToPayloads(value)
				must(err)
				must(e.signal("message", payloads, nil))
			}
		}
		d.OnWorkflowTaskStarted(5 * time.Second)
	}()
	if mode == "conflict" {
		task, ok := fault.(*temporalbridge.WorkflowTaskError)
		var failure *isolate.PanicError
		if !ok || !errors.As(task, &failure) || e.completed {
			panic(fmt.Sprintf("invalid signal should fail task: fault=%v completed=%t", fault, e.completed))
		}
		text := "already registered"
		if !strings.Contains(failure.Message, text) {
			panic(failure)
		}
		return
	}
	if fault != nil || !e.completed || e.err != nil {
		panic(fmt.Sprintf("typed signal completion: fault=%v completed=%t err=%v", fault, e.completed, e.err))
	}
	if mode == "malformed" {
		e.diagnostics.check(3, false)
	}
	var got []signals.Message
	must(dc.FromPayloads(e.result, &got))
	if mode == "cancel" {
		if len(got) != 0 {
			panic("cancelled channel returned a signal")
		}
		return
	}
	if len(got) != len(want) {
		panic(fmt.Sprintf("lost signals: %+v", got))
	}
	for i := range want {
		if got[i].ID != want[i].ID || got[i].Text != want[i].Text || len(got[i].Data) != 1 || got[i].Data[0] != byte(i+2) || want[i].Data[0] != byte(i+1) {
			panic(fmt.Sprintf("signal %d = %+v, want %+v", i, got[i], want[i]))
		}
	}
}

func checkDrain(mode string, intercepted bool) {
	r := new(registration)
	w := worker.Wrap(r)
	w.RegisterWorkflow(signals.DrainWorkflow)
	if intercepted {
		must(worker.SetIsolateInterceptors(w, worker.InterceptorOptions{Factory: signals.NewInterceptors}))
	}
	dc := converter.GetDefaultDataConverter()
	e := &environment{dc: dc, diagnostics: new(diagnostics)}
	d := r.factory.NewWorkflowDefinition()
	defer d.Close()
	input, err := dc.ToPayloads(mode)
	must(err)
	d.Execute(e, nil, input)
	if mode != "startup" {
		d.OnWorkflowTaskStarted(5 * time.Second)
		if e.completed {
			panic("drain completed before wakeup")
		}
	}
	send := func(name string, value any) {
		payloads, err := dc.ToPayloads(value)
		must(err)
		must(e.signal(name, payloads, nil))
	}
	for value := range 17 {
		name := "number"
		if intercepted {
			name = "renamed"
			send("filtered", value)
		}
		send(name, value)
	}
	for value := range 11 {
		send("other", 100+value)
	}
	if mode == "late" {
		send("start", true)
	}
	if mode == "timer" {
		if e.timer == nil {
			panic("timer not scheduled")
		}
		// Queue the ordinary wakeup before signal admission, reproducing the race.
		e.timer(nil, nil)
	}
	d.OnWorkflowTaskStarted(5 * time.Second)
	if !e.completed || e.err != nil {
		panic(fmt.Sprintf("drain completion: %t %v", e.completed, e.err))
	}
	var got []int
	must(dc.FromPayloads(e.result, &got))
	if len(got) != 28 {
		panic(fmt.Sprintf("partial %s drain: %v", mode, got))
	}
	for index, value := range got {
		want := index
		if index >= 17 {
			want = 100 + index - 17
		}
		if value != want {
			panic(fmt.Sprintf("%s drain lost order: %v", mode, got))
		}
	}
}

func main() {
	corruptMode := flag.String("corrupt-mode", "", "run a corrupt-signal port")
	live := flag.Bool("live", false, "check a local Temporal dev server and replay its signal histories")
	flag.Parse()
	if *corruptMode != "" {
		for _, configured := range []bool{false, true} {
			for _, intercepted := range []bool{false, true} {
				checkCorrupt(*corruptMode, configured, intercepted, false)
			}
		}
		checkCorrupt(*corruptMode, false, false, true)
		return
	}
	if *live {
		checkLive()
		return
	}
	for _, intercepted := range []bool{false, true} {
		for _, mode := range []string{"startup", "late", "wake", "timer"} {
			checkDrain(mode, intercepted)
		}
	}
	for _, configured := range []bool{false, true} {
		for _, intercepted := range []bool{false, true} {
			for _, mode := range []string{"delivery", "cancel", "detached", "conflict", "malformed"} {
				check(mode, configured, intercepted)
			}
		}
	}
	fmt.Println("typed signals: proactive drains, interception, decoding, ordering, identity, cancellation and converters passed")
}
