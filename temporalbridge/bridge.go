// Package temporalbridge connects one statically linked isolate program to
// Temporal's Go workflow worker. It is a trusted, serial POC adapter.
package temporalbridge

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"isolate"
	"time"

	"github.com/mfateev/sdk-go-poc/workflow"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/sdk/converter"
	bindings "go.temporal.io/sdk/internalbindings"
	"go.temporal.io/sdk/worker"
	goWorkflow "go.temporal.io/sdk/workflow"
	"google.golang.org/protobuf/proto"
)

// Register makes a configured isolate program available as a Temporal workflow.
// A workflow.Run dispatcher selects a handler registered under the Temporal
// workflow name. Programs with their own main may ignore that entry name.
func Register(w worker.Worker, name string, program isolate.Program) {
	RegisterEntry(w, name, program, name)
}

// RegisterEntry maps a Temporal workflow name to a possibly different handler
// name in the isolate program. Several names may use the same program.
func RegisterEntry(w worker.Worker, name string, program isolate.Program, entryName string) {
	if entryName == "" {
		panic("temporalbridge: workflow entry name is empty")
	}
	w.RegisterWorkflowWithOptions(Factory{Program: program, EntryName: entryName}, goWorkflow.RegisterOptions{Name: name})
}

// Factory creates one definition, and therefore one isolate, per execution.
type Factory struct {
	Program   isolate.Program
	EntryName string
	Function  isolate.Handle
}

func (f Factory) NewWorkflowDefinition() bindings.WorkflowDefinition {
	if f.Function.Name() != "" {
		f.Program = f.Function.Program(func() { _ = workflow.RunFunction(f.Function) })
		f.EntryName = f.Function.Name()
	}
	return &definition{program: f.Program, entryName: f.EntryName}
}

type reply struct {
	command *isolate.Command
	payload []byte
	err     error
}

type signalWaiter struct {
	name    string
	command *isolate.Command
}

type definition struct {
	program     isolate.Program
	entryName   string
	env         bindings.WorkflowEnvironment
	input       *commonpb.Payloads
	instance    *isolate.Isolate
	started     bool
	completed   bool
	pending     []reply
	signals     []workflow.Signal
	wantSignals []signalWaiter
}

// Execute must be asynchronous. History callbacks only queue data here.
func (d *definition) Execute(env bindings.WorkflowEnvironment, _ *commonpb.Header, input *commonpb.Payloads) {
	d.env, d.input = env, input
	env.RegisterSignalHandler(func(name string, payloads *commonpb.Payloads, _ *commonpb.Header) error {
		var payload []byte
		if err := env.GetDataConverter().FromPayloads(payloads, &payload); err != nil {
			return err
		}
		d.signals = append(d.signals, workflow.Signal{Name: name, Input: payload})
		return nil
	})
}

func (d *definition) OnWorkflowTaskStarted(deadline time.Duration) {
	if d.completed {
		return
	}
	configured, ok := d.env.GetDataConverter().(*converter.CompositeDataConverter)
	if !ok || configured != converter.GetDefaultDataConverter() {
		d.fail(errors.New("isolate POC requires Temporal's default data converter"))
		return
	}
	clock, ok := d.env.(interface{ Now() time.Time })
	if !ok {
		d.fail(errors.New("Temporal workflow environment does not expose history time"))
		return
	}
	now := clock.Now()
	resuming := len(d.pending) != 0 || d.hasDeliverableSignal()
	newInstance := !d.started
	if !d.started {
		d.started = true
		var err error
		d.instance, err = isolate.New(isolate.Config{Program: d.program, InitialTime: &now, TimerOp: workflow.OpSleep})
		if err == nil {
			err = d.instance.Start()
		}
		if err != nil {
			d.fail(err)
			return
		}
	} else if err := d.instance.AdvanceTime(now); err != nil {
		d.fail(err)
		return
	}
	if !newInstance && !resuming {
		// A signal or unrelated history event need not wake a serial workflow.
		return
	}
	for _, r := range d.pending {
		r.command.Reply(r.payload, r.err)
	}
	d.pending = nil
	d.deliverWaitingSignals()
	// This timeout detects a broken serial adapter. A native quiescence barrier
	// must replace the one-command-at-a-time assumption for concurrent code.
	if deadline <= 0 {
		deadline = time.Second
	}
	timer := time.NewTimer(deadline)
	defer timer.Stop()
	for !d.completed {
		select {
		case command := <-d.instance.Commands():
			if command == nil {
				d.fail(errors.New("isolate command stream closed"))
				return
			}
			blocked, err := d.handle(command)
			if err != nil {
				command.Reply(nil, err)
				d.fail(err)
				return
			}
			if blocked {
				return
			}
		case <-d.instance.Done():
			if !d.completed {
				if err := d.instance.Wait(); err != nil {
					d.fail(fmt.Errorf("isolate returned without workflow.Complete: %w", err))
				} else {
					d.fail(errors.New("isolate returned without workflow.Complete"))
				}
			}
			return
		case <-timer.C:
			d.fail(errors.New("isolate did not reach a host operation before the workflow task deadline"))
			return
		}
	}
}

// handle returns true when the isolate is waiting for a future history event.
func (d *definition) handle(command *isolate.Command) (bool, error) {
	switch command.Op {
	case workflow.OpInput:
		input, err := d.inputBytes()
		if err != nil {
			return false, err
		}
		command.Reply(input, nil)
	case workflow.OpStart:
		if d.entryName == "" {
			return false, errors.New("workflow entry name is not configured")
		}
		input, err := d.inputBytes()
		if err != nil {
			return false, err
		}
		payload, err := json.Marshal(workflow.Start{Name: d.entryName, Input: input})
		if err != nil {
			return false, err
		}
		command.Reply(payload, nil)
	case workflow.OpStartPayloads:
		if d.entryName == "" {
			return false, errors.New("workflow entry name is not configured")
		}
		var input []byte
		if d.input != nil {
			var err error
			input, err = proto.MarshalOptions{Deterministic: true}.Marshal(d.input)
			if err != nil {
				return false, err
			}
		}
		payload, err := json.Marshal(workflow.PayloadStart{Name: d.entryName, Payloads: input})
		if err != nil {
			return false, err
		}
		command.Reply(payload, nil)
	case workflow.OpActivity:
		var request workflow.ActivityRequest
		if err := json.Unmarshal(command.Payload, &request); err != nil {
			return false, err
		}
		if request.Name == "" || request.StartToCloseTimeout <= 0 {
			return false, errors.New("invalid activity request")
		}
		input, err := d.env.GetDataConverter().ToPayloads(request.Input)
		if err != nil {
			return false, err
		}
		params := bindings.ExecuteActivityParams{
			ExecuteActivityOptions: bindings.ExecuteActivityOptions{
				TaskQueueName:       d.env.WorkflowInfo().TaskQueueName,
				StartToCloseTimeout: request.StartToCloseTimeout,
				ScheduleID:          d.env.GenerateSequence(),
			},
			ActivityType: bindings.ActivityType{Name: request.Name},
			Input:        input,
		}
		d.env.ExecuteActivity(params, func(result *commonpb.Payloads, cause error) {
			d.queueResult(command, result, cause)
		})
		return true, nil
	case workflow.OpSleep:
		var duration time.Duration
		if err := json.Unmarshal(command.Payload, &duration); err != nil {
			return false, err
		}
		if duration < 0 {
			return false, errors.New("negative timer duration")
		}
		if duration == 0 {
			command.Reply(nil, nil)
			return false, nil
		}
		d.env.NewTimer(duration, goWorkflow.TimerOptions{}, func(result *commonpb.Payloads, cause error) {
			d.pending = append(d.pending, reply{command: command, err: cause})
		})
		return true, nil
	case workflow.OpSignal:
		name := string(command.Payload)
		index := d.signalIndex(name)
		if index < 0 {
			d.wantSignals = append(d.wantSignals, signalWaiter{name: name, command: command})
			return true, nil
		}
		payload, _ := json.Marshal(d.signals[index])
		d.signals = append(d.signals[:index], d.signals[index+1:]...)
		command.Reply(payload, nil)
	case workflow.OpComplete:
		var completion workflow.Completion
		if err := json.Unmarshal(command.Payload, &completion); err != nil {
			return false, err
		}
		var result *commonpb.Payloads
		var err error
		if completion.Error != "" {
			err = errors.New(completion.Error)
		} else {
			result, err = d.env.GetDataConverter().ToPayloads(completion.Result)
		}
		if err == nil || completion.Error != "" {
			d.env.Complete(result, err)
			d.completed = true
			command.Reply(nil, nil)
			return true, nil
		}
		return false, err
	case workflow.OpCompletePayloads:
		var completion workflow.PayloadCompletion
		if err := json.Unmarshal(command.Payload, &completion); err != nil {
			return false, err
		}
		var result *commonpb.Payloads
		var err error
		if completion.Error != "" {
			err = errors.New(completion.Error)
		} else if len(completion.Payloads) != 0 {
			result = new(commonpb.Payloads)
			if err := proto.Unmarshal(completion.Payloads, result); err != nil {
				return false, fmt.Errorf("decode workflow result payloads: %w", err)
			}
		}
		d.env.Complete(result, err)
		d.completed = true
		command.Reply(nil, nil)
		return true, nil
	default:
		return false, fmt.Errorf("unknown isolate operation %d", command.Op)
	}
	return false, nil
}

func (d *definition) inputBytes() ([]byte, error) {
	var input []byte
	if d.input != nil {
		if err := d.env.GetDataConverter().FromPayloads(d.input, &input); err != nil {
			return nil, err
		}
	}
	return input, nil
}

func (d *definition) signalIndex(name string) int {
	for index, signal := range d.signals {
		if name == "" || signal.Name == name {
			return index
		}
	}
	return -1
}

func (d *definition) hasDeliverableSignal() bool {
	for _, signal := range d.signals {
		if d.waiterIndex(signal.Name) >= 0 {
			return true
		}
	}
	return false
}

func (d *definition) waiterIndex(name string) int {
	for index, waiter := range d.wantSignals {
		if waiter.name == "" || waiter.name == name {
			return index
		}
	}
	return -1
}

func (d *definition) deliverWaitingSignals() {
	for signalIndex := 0; signalIndex < len(d.signals); {
		waiterIndex := d.waiterIndex(d.signals[signalIndex].Name)
		if waiterIndex < 0 {
			signalIndex++
			continue
		}
		payload, _ := json.Marshal(d.signals[signalIndex])
		d.wantSignals[waiterIndex].command.Reply(payload, nil)
		d.wantSignals = append(d.wantSignals[:waiterIndex], d.wantSignals[waiterIndex+1:]...)
		d.signals = append(d.signals[:signalIndex], d.signals[signalIndex+1:]...)
	}
}

func (d *definition) queueResult(command *isolate.Command, result *commonpb.Payloads, cause error) {
	var payload []byte
	if cause == nil && result != nil {
		cause = d.env.GetDataConverter().FromPayloads(result, &payload)
	}
	d.pending = append(d.pending, reply{command: command, payload: payload, err: cause})
}

func (d *definition) fail(err error) {
	if d.completed {
		return
	}
	d.completed = true
	d.env.Complete(nil, err)
}

func (d *definition) StackTrace() string { return "isolate workflow stack trace unavailable" }

func (d *definition) Close() {
	if d.instance == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	_ = d.instance.Kill(ctx)
}
