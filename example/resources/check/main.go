package main

import (
	"errors"
	"fmt"
	"isolate"
	"os"
	"time"

	"github.com/mfateev/sdk-go-poc/example/resources"
	"github.com/mfateev/sdk-go-poc/temporalbridge"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/sdk/converter"
	bindings "go.temporal.io/sdk/internalbindings"
	"go.temporal.io/sdk/log"
	goWorkflow "go.temporal.io/sdk/workflow"
)

type resourceEnvironment struct {
	bindings.WorkflowEnvironment
	completes, cancels int
	signal             func(string, *commonpb.Payloads, *commonpb.Header) error
	cancel             func()
}

func (*resourceEnvironment) GetDataConverter() converter.DataConverter {
	return converter.GetDefaultDataConverter()
}
func (*resourceEnvironment) Now() time.Time { return time.Unix(1700000000, 0) }
func (*resourceEnvironment) WorkflowInfo() *goWorkflow.Info {
	return &goWorkflow.Info{WorkflowExecution: goWorkflow.Execution{ID: "resource-test", RunID: "run"}, WorkflowType: goWorkflow.Type{Name: "ResourceWorkflow"}}
}
func (*resourceEnvironment) IsReplaying() bool                    { return false }
func (*resourceEnvironment) GetLogger() log.Logger                { return resourceLogger{} }
func (e *resourceEnvironment) Complete(*commonpb.Payloads, error) { e.completes++ }
func (e *resourceEnvironment) RegisterCancelHandler(fn func())    { e.cancel = fn }
func (e *resourceEnvironment) RegisterSignalHandler(fn func(string, *commonpb.Payloads, *commonpb.Header) error) {
	e.signal = fn
}
func (*resourceEnvironment) NewTimer(time.Duration, goWorkflow.TimerOptions, bindings.ResultHandler) *bindings.TimerID {
	return nil
}
func (e *resourceEnvironment) RequestCancelTimer(bindings.TimerID) { e.cancels++ }

type resourceLogger struct{}

func (resourceLogger) Debug(string, ...any) {}
func (resourceLogger) Info(string, ...any)  {}
func (resourceLogger) Warn(string, ...any)  {}
func (resourceLogger) Error(string, ...any) {}

func resourceDefinition(t *check, input resources.Input, options temporalbridge.ResourceOptions) (bindings.WorkflowDefinition, *resourceEnvironment) {
	t.Helper()
	handle, ok := isolate.LookupFunction(resources.ResourceWorkflow)
	if !ok {
		t.Fatal("missing compiled resource workflow")
	}
	d := (temporalbridge.Factory{Function: handle, ResolveResourceOptions: func() temporalbridge.ResourceOptions { return options }}).NewWorkflowDefinition()
	e := new(resourceEnvironment)
	payloads, err := e.GetDataConverter().ToPayloads(input)
	if err != nil {
		t.Fatal(err)
	}
	d.Execute(e, nil, payloads)
	return d, e
}
func runResourceTask(d bindings.WorkflowDefinition) (failure any) {
	defer func() { failure = recover() }()
	d.OnWorkflowTaskStarted(5 * time.Second)
	return nil
}

func checkWorkflowLimits(t *check) {
	for _, mode := range []string{"goroutines", "memory", "small", "stack", "iterator"} {
		t.Run(mode, func(t *check) {
			options := temporalbridge.ResourceOptions{Limits: isolate.ResourceLimits{MaxGoroutines: 2, MaxMemoryBytes: 2 << 20}}
			if mode == "iterator" {
				options.Limits.MaxGoroutines = 2
			}
			if mode == "goroutines" || mode == "iterator" {
				options.Limits.MaxMemoryBytes = 0
			}
			if mode == "small" {
				options.Limits.MaxMemoryBytes = 768 << 10
			}
			if mode == "stack" {
				options.Limits.MaxMemoryBytes = 512 << 10
			}
			var events []temporalbridge.ResourceEvent
			options.Observer = func(event temporalbridge.ResourceEvent) { events = append(events, event) }
			d, e := resourceDefinition(t, resources.Input{Mode: mode}, options)
			failure, ok := runResourceTask(d).(*temporalbridge.WorkflowTaskError)
			var limit *isolate.ResourceLimitError
			if !ok || !errors.As(failure, &limit) || limit.Stack == "" {
				t.Fatalf("limit failure=%v", failure)
			}
			if e.completes != 0 || e.cancels != 0 {
				t.Fatal("limit completed execution or canceled server work")
			}
			if mode == "goroutines" || mode == "iterator" {
				if limit.Resource != "goroutines" {
					t.Fatalf("wrong resource %v", limit)
				}
			} else if limit.Resource != "memory" {
				t.Fatalf("wrong resource %v", limit)
			}
			seen := false
			for _, event := range events {
				if event.Kind == "limit" {
					seen = true
					if event.WorkflowID != "resource-test" || event.Error == "" {
						t.Fatalf("event=%+v", event)
					}
				}
			}
			if !seen {
				t.Fatal("missing resource failure event")
			}
			d.Close()
			if err := d.(interface{ CloseError() error }).CloseError(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func checkWatchdogs(t *check) {
	for _, mode := range []string{"yield", "busy"} {
		t.Run(mode, func(t *check) {
			options := temporalbridge.ResourceOptions{MaxTaskDuration: 30 * time.Millisecond}
			want := "task duration"
			if mode == "busy" {
				options = temporalbridge.ResourceOptions{MaxNoProgressDuration: 30 * time.Millisecond}
				want = "no progress"
			}
			d, e := resourceDefinition(t, resources.Input{Mode: mode, Iterations: 500000000}, options)
			failure, ok := runResourceTask(d).(*temporalbridge.WorkflowTaskError)
			var limit *isolate.ResourceLimitError
			if !ok || !errors.As(failure, &limit) || limit.Resource != want {
				t.Fatalf("watchdog=%v", failure)
			}
			if e.completes != 0 {
				t.Fatal("watchdog completed execution")
			}
			// The finite uninterrupted loop may remain pending at the first deadline.
			// Retry only cleanup; it must never resume a closed definition's callbacks.
			deadline := time.Now().Add(5 * time.Second)
			for {
				d.Close()
				if d.(interface{ CloseError() error }).CloseError() == nil {
					break
				}
				if time.Now().After(deadline) {
					t.Fatal("finite loop failed to drain")
				}
			}
			e.cancel()
			if err := e.signal("late", nil, nil); err != nil {
				t.Fatal(err)
			}
			if e.completes != 0 || e.cancels != 0 {
				t.Fatal("late callback revived limit failure")
			}
		})
	}
}

func checkCachedIdle(t *check) {
	d, e := resourceDefinition(t, resources.Input{Mode: "memory"}, temporalbridge.ResourceOptions{})
	if failure := runResourceTask(d); failure != nil || e.completes != 1 {
		t.Fatalf("unlimited failure=%v completes=%d", failure, e.completes)
	}
	var events []temporalbridge.ResourceEvent
	d, e = resourceDefinition(t, resources.Input{Mode: "cached"}, temporalbridge.ResourceOptions{MaxTaskDuration: time.Second, MaxNoProgressDuration: 20 * time.Millisecond, Observer: func(event temporalbridge.ResourceEvent) { events = append(events, event) }})
	if failure := runResourceTask(d); failure != nil {
		t.Fatal(failure)
	}
	before := d.(interface{ Resources() isolate.ResourceStats }).Resources()
	time.Sleep(60 * time.Millisecond)
	if failure := runResourceTask(d); failure != nil {
		t.Fatal(failure)
	}
	after := d.(interface{ Resources() isolate.ResourceStats }).Resources()
	if before.LiveGoroutines == 0 || after.LiveGoroutines != before.LiveGoroutines || after.Progress != before.Progress {
		t.Fatalf("cached accounting before=%+v after=%+v", before, after)
	}
	d.Close()
	if e.completes != 0 || e.cancels != 0 {
		t.Fatal("eviction completed workflow or canceled timer")
	}
	if len(events) == 0 || d.(interface{ Resources() isolate.ResourceStats }).Resources().LiveGoroutines != 0 {
		t.Fatal("missing cleanup accounting")
	}
}

// This binary is built with go build, which discovers directive entry points.
// The test wrapper compiles and executes it with the actual custom toolchain.
type check struct{ name string }

func (*check) Helper()             {}
func (t *check) Fatal(args ...any) { panic(t.name + ": " + fmt.Sprint(args...)) }
func (t *check) Fatalf(format string, args ...any) {
	panic(t.name + ": " + fmt.Sprintf(format, args...))
}
func (t *check) Run(name string, fn func(*check)) { fn(&check{name: t.name + "/" + name}) }
func main() {
	checkWorkflowLimits(&check{name: "admission"})
	checkWatchdogs(&check{name: "watchdogs"})
	checkCachedIdle(&check{name: "cached"})
	checkObserverPanic()
	if len(os.Args) == 3 && os.Args[1] == "live" {
		if err := checkLiveServer(os.Args[2]); err != nil {
			panic(err)
		}
	}
	fmt.Println("compiled resource controls passed")
}

func checkObserverPanic() {
	d, e := resourceDefinition(&check{name: "observer"}, resources.Input{Mode: "cached"}, temporalbridge.ResourceOptions{Observer: func(event temporalbridge.ResourceEvent) { panic("observer failure") }})
	if failure := runResourceTask(d); failure != "observer failure" {
		panic(fmt.Sprintf("observer failure=%v", failure))
	}
	d.Close()
	if d.(interface{ CloseError() error }).CloseError() != nil || d.(interface{ Resources() isolate.ResourceStats }).Resources().LiveGoroutines != 0 || e.completes != 0 {
		panic("observer panic failed to clean up")
	}
}
