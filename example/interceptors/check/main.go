package main

import (
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/mfateev/sdk-go-poc/example/interceptors"
	"github.com/mfateev/sdk-go-poc/temporalbridge"
	"github.com/mfateev/sdk-go-poc/worker"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
	bindings "go.temporal.io/sdk/internalbindings"
	"go.temporal.io/sdk/log"
	sdkworker "go.temporal.io/sdk/worker"
	sdkwf "go.temporal.io/sdk/workflow"
)

type registrations struct {
	sdkworker.Worker
	factory temporalbridge.Factory
}

func (r *registrations) RegisterWorkflowWithOptions(fn any, _ sdkwf.RegisterOptions) {
	r.factory = fn.(temporalbridge.Factory)
}
func (*registrations) RegisterActivityWithOptions(any, activity.RegisterOptions) {}

type environment struct {
	bindings.WorkflowEnvironment
	replay             bool
	completes, metrics int
	result             *commonpb.Payloads
	err                error
	query              func(string, *commonpb.Payloads, *commonpb.Header) (*commonpb.Payloads, error)
	update             func(string, string, *commonpb.Payloads, *commonpb.Header, bindings.UpdateCallbacks)
	signal             func(string, *commonpb.Payloads, *commonpb.Header) error
}

func (*environment) RegisterCancelHandler(func()) {}
func (e *environment) RegisterQueryHandler(fn func(string, *commonpb.Payloads, *commonpb.Header) (*commonpb.Payloads, error)) {
	e.query = fn
}
func (e *environment) RegisterUpdateHandler(fn func(string, string, *commonpb.Payloads, *commonpb.Header, bindings.UpdateCallbacks)) {
	e.update = fn
}
func (e *environment) RegisterSignalHandler(fn func(string, *commonpb.Payloads, *commonpb.Header) error) {
	e.signal = fn
}
func (*environment) WorkflowInfo() *sdkwf.Info {
	return &sdkwf.Info{WorkflowType: sdkwf.Type{Name: "Workflow"}, WorkflowExecution: sdkwf.Execution{ID: "workflow", RunID: "run"}, OriginalRunID: "run", Namespace: "default", TaskQueueName: "interceptors"}
}
func (*environment) Now() time.Time { return time.Date(2025, 1, 2, 0, 0, 0, 0, time.UTC) }
func (*environment) GetDataConverter() converter.DataConverter {
	return converter.GetDefaultDataConverter()
}
func (e *environment) IsReplaying() bool { return e.replay }
func (e *environment) Complete(result *commonpb.Payloads, err error) {
	e.completes++
	e.result, e.err = result, err
}
func (*environment) GetLogger() log.Logger                      { return logger{} }
func (e *environment) GetMetricsHandler() client.MetricsHandler { return metrics{e: e} }
func (*environment) GenerateSequence() int64                    { return 1 }
func (e *environment) ExecuteActivity(params bindings.ExecuteActivityParams, callback bindings.ResultHandler) bindings.ActivityID {
	if params.ActivityType.Name != "ActivityAlias" || string(params.Header.Fields["activity"].Data) != "BA" {
		panic("outbound alias, order or header lost")
	}
	var input string
	must(e.GetDataConverter().FromPayloads(params.Input, &input))
	result, err := e.GetDataConverter().ToPayloads(input + "-activity")
	must(err)
	callback(result, nil)
	return bindings.ActivityID{}
}

type logger struct{}

func (logger) Debug(string, ...any)               {}
func (logger) Info(string, ...any)                {}
func (logger) Warn(message string, fields ...any) { fmt.Println("diagnostic:", message, fields) }
func (logger) Error(string, ...any)               {}

type metrics struct{ e *environment }

func (m metrics) WithTags(tags map[string]string) client.MetricsHandler {
	if tags["kind"] != "workflow" {
		panic("metric tags lost")
	}
	return m
}
func (m metrics) Counter(name string) client.MetricsCounter {
	if name != "runs" {
		panic("metric name lost")
	}
	return m
}
func (m metrics) Gauge(string) client.MetricsGauge { return m }
func (m metrics) Timer(string) client.MetricsTimer { return m }
func (m metrics) Inc(value int64)                  { m.e.metrics += int(value) }
func (metrics) Update(float64)                     {}
func (metrics) Record(time.Duration)               {}

type outcome struct {
	accepted, completed bool
	err                 error
}

func (o *outcome) Accept()                   { o.accepted = true }
func (o *outcome) Reject(err error)          { o.err = err }
func (o *outcome) Complete(_ any, err error) { o.completed, o.err = true, err }
func header(name string) *commonpb.Header {
	return &commonpb.Header{Fields: map[string]*commonpb.Payload{name: {Data: []byte(name + "-header")}}}
}
func must(err error) {
	if err != nil {
		panic(err)
	}
}

func check(replay bool, taskTimeout time.Duration) {
	registrations := &registrations{}
	w := worker.Wrap(registrations)
	w.RegisterWorkflow(interceptors.Workflow)
	w.RegisterActivityWithOptions(interceptors.Activity, activity.RegisterOptions{Name: "ActivityAlias"})
	config := []byte("copied")
	must(worker.SetIsolateInterceptors(w, worker.InterceptorOptions{Factory: interceptors.NewInterceptors, Config: config}))
	config[0] = 'X'
	must(worker.SetIsolateDataConverter(w, worker.DataConverterOptions{Factory: interceptors.NewConverter, Config: []byte("json")}))
	var observations []string
	must(worker.RegisterSink(w, interceptors.SinkOp, func(event worker.SinkEvent) { observations = append(observations, string(event.Payload)) }, worker.SinkOptions{EnableReplay: true}))
	var logs []worker.LogEvent
	must(worker.SetIsolateLogHandler(w, func(event worker.LogEvent) { logs = append(logs, event) }))

	// A shared factory creates independent metadata, configuration and chains.
	for run := range 2 {
		env := &environment{replay: replay}
		input, err := env.GetDataConverter().ToPayloads("hello")
		must(err)
		d := registrations.factory.NewWorkflowDefinition()
		defer d.Close()
		d.Execute(env, header("start"), input)
		d.OnWorkflowTaskStarted(taskTimeout)
		checkQuery := func(name string, want int) {
			result, err := env.query(name, nil, header("query"))
			if name == "bad" {
				if err == nil || !strings.Contains(err.Error(), "read-only") {
					panic(fmt.Sprintf("invalid query error: %v", err))
				}
				return
			}
			must(err)
			var got int
			must(env.GetDataConverter().FromPayloads(result, &got))
			if got != want {
				panic(fmt.Sprintf("query state=%d want=%d", got, want))
			}
		}
		signalInput, signalErr := env.GetDataConverter().ToPayloads([]byte("queued"))
		must(signalErr)
		startHooks := len(observations)
		must(env.signal("unused", signalInput, header("signal")))
		d.OnWorkflowTaskStarted(taskTimeout)
		if len(observations) != startHooks+2 {
			panic("signal hook waited for a channel receiver")
		}
		startHooks = len(observations)
		must(env.signal("filtered", signalInput, header("signal")))
		d.OnWorkflowTaskStarted(taskTimeout)
		if len(observations) != startHooks+1 {
			panic("signal interceptor could not filter delivery")
		}
		checkQuery("state", 7)
		checkQuery("bad", 7)
		checkQuery("state", 7)
		if !replay {
			rejected := new(outcome)
			env.update("bad", fmt.Sprintf("bad-%d", run), nil, header("update"), rejected)
			d.OnWorkflowTaskStarted(taskTimeout)
			if rejected.err == nil || rejected.accepted {
				panic("invalid validator accepted or killed workflow")
			}
			accepted := new(outcome)
			env.update("bump", fmt.Sprintf("bump-%d", run), nil, header("update"), accepted)
			d.OnWorkflowTaskStarted(taskTimeout)
			if !accepted.accepted || !accepted.completed || accepted.err != nil {
				panic(fmt.Sprintf("update=%+v", accepted))
			}
			checkQuery("state", 8)
		}
		must(env.signal("renamed", nil, header("signal")))
		d.OnWorkflowTaskStarted(taskTimeout)
		if env.completes != 1 || env.err != nil {
			panic(fmt.Sprintf("complete=%d error=%v", env.completes, env.err))
		}
		var result string
		must(env.GetDataConverter().FromPayloads(env.result, &result))
		if result != "helloAB-activityBA" {
			panic("interceptor input or output not applied: " + result)
		}
		wantMetrics := 1
		if replay {
			wantMetrics = 0
		}
		if env.metrics != wantMetrics {
			panic("metric replay suppression failed")
		}
		checkQuery("state", 7+wantMetrics) // Completed query state remains available.
		d.Close()
	}
	if len(logs) != 2 || logs[0].Level != "info" || logs[0].Message != "workflow-log" || logs[0].Replay != replay || strings.Join(logs[0].Fields, ",") != "input,helloAB" {
		panic(fmt.Sprintf("logs=%+v", logs))
	}
	for _, required := range []string{"A.enter", "B.enter", "A.activity", "B.activity", "A.query", "B.query", "A.signal", "B.signal", "A.exit", "B.exit"} {
		found := false
		for _, observed := range observations {
			if observed == required {
				found = true
			}
		}
		if !found {
			panic("missing hook: " + required)
		}
	}
}
func checkBufferedSignals(taskTimeout time.Duration) {
	registrations := &registrations{}
	w := worker.Wrap(registrations)
	w.RegisterWorkflowWithOptions(interceptors.Immediate, sdkwf.RegisterOptions{Name: "Workflow"})
	must(worker.SetIsolateInterceptors(w, worker.InterceptorOptions{Factory: interceptors.NewInterceptors, Config: []byte("copied")}))
	var hooks []string
	must(worker.RegisterSink(w, interceptors.SinkOp, func(event worker.SinkEvent) { hooks = append(hooks, string(event.Payload)) }, worker.SinkOptions{}))
	env := &environment{}
	input, err := env.GetDataConverter().ToPayloads("hello")
	must(err)
	d := registrations.factory.NewWorkflowDefinition()
	defer d.Close()
	d.Execute(env, header("start"), input)
	for range 3 {
		must(env.signal("unused", nil, header("signal")))
	}
	d.OnWorkflowTaskStarted(taskTimeout)
	if env.completes != 1 || env.err != nil {
		panic(fmt.Sprintf("immediate completion: %d %v", env.completes, env.err))
	}
	for _, name := range []string{"A.signal", "B.signal"} {
		count := 0
		for _, hook := range hooks {
			if hook == name {
				count++
			}
		}
		if count != 3 {
			panic(fmt.Sprintf("buffered signals: %s=%d", name, count))
		}
	}
}
func main() {
	taskTimeout := flag.Duration("task-timeout", 30*time.Second, "synthetic workflow task deadline")
	flag.Parse()
	check(false, *taskTimeout)
	check(true, *taskTimeout)
	checkBufferedSignals(*taskTimeout)
	fmt.Fprintln(os.Stdout, "native workflow interceptors, copied factories, read-only state and byte observations passed")
}
