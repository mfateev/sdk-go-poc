// Package temporalbridge connects one statically linked isolate program to
// Temporal's Go workflow worker. It is a trusted POC adapter using native
// deterministic dispatch and an exact runtime suspension barrier.
package temporalbridge

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"isolate"
	"time"

	"github.com/mfateev/sdk-go-poc/internal/failurecodec"
	"github.com/mfateev/sdk-go-poc/workflow"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/sdk/converter"
	bindings "go.temporal.io/sdk/internalbindings"
	"go.temporal.io/sdk/temporal"
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
	// ResolveActivity maps function references to host registration aliases.
	// It runs only on the host and is never passed into an isolate.
	ResolveActivity func(string) string
	// ResolveWorkflow maps function references to workflow registration aliases.
	ResolveWorkflow func(string) string
	// ResolveLogHandler reads worker configuration on the host for each record.
	ResolveLogHandler func() LogHandler
	// ResolveResourceOptions snapshots host policy for each new execution.
	ResolveResourceOptions func() ResourceOptions
}

func (f Factory) NewWorkflowDefinition() bindings.WorkflowDefinition {
	if f.Function.Name() != "" {
		// The runtime entry wrapper transfers compiler-created metadata by
		// value. This dispatcher captures no host-owned closure state.
		f.Program = f.Function.ProgramWithHandle(func(handle isolate.Handle) { _ = workflow.RunFunction(handle) })
		f.EntryName = f.Function.Name()
	}
	var resources ResourceOptions
	if f.ResolveResourceOptions != nil {
		resources = f.ResolveResourceOptions()
	}
	if err := resources.Validate(); err != nil {
		panic(err)
	}
	return &definition{resources: resources, program: f.Program, entryName: f.EntryName, resolveWorkflow: f.ResolveWorkflow, resolveActivity: f.ResolveActivity, resolveLogHandler: f.ResolveLogHandler}
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

type callState struct {
	id      uint64
	command *isolate.Command
	done    bool
	cancel  func()
}

type definition struct {
	queryHandlers      map[string]workflow.QueryHandlerOptions
	queryWaiter        *isolate.Command
	querySequence      uint64
	retainedCompletion *workflowCompletion
	activities         map[uint64]*activityState
	resources          ResourceOptions
	lastResources      isolate.ResourceStats
	resourceIdentity   ResourceEvent
	canceled           bool
	cancelWaiter       *isolate.Command
	calls              map[uint64]*callState
	callsByCommand     map[*isolate.Command]*callState
	earlyCancel        map[uint64]bool
	resolveWorkflow    func(string) string
	resolveActivity    func(string) string
	resolveLogHandler  func() LogHandler
	failureStack       string
	closed             bool
	closeErr           error
	program            isolate.Program
	entryName          string
	env                bindings.WorkflowEnvironment
	input              *commonpb.Payloads
	instance           *isolate.Isolate
	started            bool
	completed          bool
	pending            []reply
	immediate          []reply
	signals            []workflow.Signal
	wantSignals        []signalWaiter
}

// Execute must be asynchronous. History callbacks only queue data here.
func (d *definition) Execute(env bindings.WorkflowEnvironment, _ *commonpb.Header, input *commonpb.Payloads) {
	if d.closed {
		return
	}
	d.env, d.input = env, input
	env.RegisterCancelHandler(func() {
		if d.completed || d.closed {
			return
		}
		d.canceled = true
		if d.cancelWaiter != nil {
			d.pending = append(d.pending, reply{command: d.cancelWaiter})
			d.cancelWaiter = nil
		}
	})
	env.RegisterSignalHandler(func(name string, payloads *commonpb.Payloads, _ *commonpb.Header) error {
		if d.completed || d.closed {
			return nil
		}
		var payload []byte
		if err := env.GetDataConverter().FromPayloads(payloads, &payload); err != nil {
			return err
		}
		d.signals = append(d.signals, workflow.Signal{Name: name, Input: payload})
		return nil
	})
}

func (d *definition) OnWorkflowTaskStarted(deadline time.Duration) {
	// A host log handler may panic. Always release the instance before handing
	// the panic to the SDK's Workflow Task failure path.
	defer func() {
		if p := recover(); p != nil {
			d.Close()
			panic(p)
		}
	}()
	if d.completed || d.closed {
		return
	}
	if deadline <= 0 {
		deadline = time.Second
	}
	budget := resourceTaskBudget{start: time.Now(), options: d.resources}
	if d.resources.MaxTaskDuration > 0 && d.resources.MaxTaskDuration < deadline {
		deadline = d.resources.MaxTaskDuration
	}
	taskContext, cancelTask := context.WithTimeout(context.Background(), deadline)
	defer cancelTask()
	defer func() { d.observeResources("task", nil) }()
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
		d.instance, err = isolate.NewContext(taskContext, isolate.Config{Program: d.program, Deterministic: true, InitialTime: &now, TimerOp: workflow.OpSleep, LogHandler: d.writeLog, ResourceLimits: d.resources.Limits})
		if err == nil {
			err = d.instance.Start()
		}
		if err != nil {
			d.failTask(budget.startupError(err))
			return
		}
		d.observeResources("created", nil)
	} else if err := d.instance.AdvanceTime(now); err != nil {
		d.fail(err)
		return
	}
	instance := d.instance
	budget.progress, budget.lastProgress, budget.monitoring = instance.Resources().Progress, time.Now(), true
	var ticks <-chan time.Time
	if limit := d.resources.MaxNoProgressDuration; limit > 0 {
		interval := min(limit, 10*time.Millisecond)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		ticks = ticker.C
	}
	checkBudget := func() {
		if err := budget.check(instance); err != nil {
			d.failTask(err)
		}
		if taskContext.Err() != nil {
			d.failTask(errors.New("isolate did not suspend before the workflow task deadline"))
		}
	}
	checkBudget()
	if !newInstance && !resuming {
		// An unrelated history event does not need to resume the instance.
		return
	}
	for _, r := range d.pending {
		r.command.Reply(r.payload, r.err)
	}
	d.pending = nil
	d.deliverWaitingSignals()
	for _, r := range d.pending {
		r.command.Reply(r.payload, r.err)
	}
	d.pending = nil
	// History replies above are batched while the previous task is suspended.
	// Service commands while the runtime seeks idle; after suspension drain
	// pending sends, then resume their transport handshakes. Return only after
	// a pass reaches idle without producing another command.
	handleCommand := func(command *isolate.Command) bool {
		checkBudget()
		if command == nil {
			d.fail(errors.New("isolate command stream closed"))
			return false
		}
		err := d.handle(command)
		if err != nil {
			command.Reply(nil, err)
			d.fail(err)
			return false
		}
		return (!d.completed || d.retainedCompletion != nil) && !d.closed
	}
	for {
		checkBudget()
		resume := instance.Resume
		if d.completed {
			resume = instance.ResumeReadOnly
		}
		if err := resume(); err != nil {
			d.fail(err)
			return
		}
		suspended := make(chan error, 1)
		go func() { suspended <- instance.Suspend() }()
		handled := false
		waiting := true
		for waiting {
			select {
			case command := <-instance.Commands():
				if !handleCommand(command) {
					return
				}
				handled = true
			case err := <-suspended:
				if err != nil {
					if !d.completed {
						d.fail(err)
					}
					return
				}
				waiting = false
			case <-instance.Done():
				checkBudget()
				if !d.completed {
					err := instance.Wait()
					if err == nil {
						err = errors.New("missing workflow completion")
					}
					d.failTask(fmt.Errorf("isolate returned without workflow.Complete: %w", err))
				}
				return
			case <-ticks:
				checkBudget()
			case <-taskContext.Done():
				checkBudget()
				d.failTask(errors.New("isolate did not suspend before the workflow task deadline"))
				return
			}
		}
		// A blocked command sender itself makes the group idle. Receiving it
		// while dispatch is fenced queues its continuation, so the next pass
		// can park that continuation on its reply without missing the command.
	drain:
		for {
			select {
			case command := <-instance.Commands():
				if !handleCommand(command) {
					return
				}
				handled = true
			default:
				break drain
			}
		}
		// Replies for inputs and already available signals also wait for
		// the suspension fence. Host processing speed must not affect which
		// workflow goroutine becomes ready during a task.
		for _, r := range d.immediate {
			r.command.Reply(r.payload, r.err)
		}
		d.immediate = nil
		if d.completed {
			d.publishRetainedCompletion()
			return
		}
		if !handled {
			if instance.PendingCalls() == 0 {
				d.failTask(errors.New("isolate deadlocked without a pending host operation"))
			}
			return
		}
	}
}

func (d *definition) replyWhenSuspended(command *isolate.Command, payload []byte, err error) {
	d.finish(command, d.callsByCommand[command], payload, err, true)
}

func (d *definition) finish(command *isolate.Command, state *callState, payload []byte, err error, immediate bool) {
	if d.closed {
		return
	}
	if state != nil {
		if state.done {
			return
		}
		state.done = true
		delete(d.calls, state.id)
		delete(d.callsByCommand, command)
		state.command, state.cancel = nil, nil
	}
	r := reply{command: command, payload: payload, err: err}
	if immediate {
		d.immediate = append(d.immediate, r)
	} else {
		d.pending = append(d.pending, r)
	}
}

func (d *definition) completion(command *isolate.Command) func([]byte, error) {
	state := d.callsByCommand[command]
	if state == nil {
		state = &callState{command: command}
		if d.callsByCommand == nil {
			d.callsByCommand = make(map[*isolate.Command]*callState)
		}
		d.callsByCommand[command] = state
	}
	// The callback retains a retireable cell, never the original command. Close
	// clears the cell even if the SDK retains or subsequently invokes its callback.
	return func(payload []byte, err error) {
		if d.closed || state.done {
			return
		}
		d.finish(state.command, state, payload, err, false)
	}
}

func (d *definition) finishWorkflow(result *commonpb.Payloads, err error) {
	env := d.env
	instance := d.instance
	d.completed = true
	if len(d.queryHandlers) != 0 && instance != nil {
		d.retainedCompletion = &workflowCompletion{result: result, err: err}
		d.retireOperations()
		instance.FreezeWorkflow()
		return
	}
	d.Close()
	if d.closeErr != nil {
		panic(&WorkflowTaskError{Cause: d.closeErr})
	}
	if instance != nil {
		if outcome := instance.Wait(); outcome != nil && !errors.Is(outcome, isolate.ErrRevoked) {
			d.failTask(outcome)
		}
	}
	// Only publish an execution result after revoking children and draining all
	// native cleanup. Completion cannot admit further activity/timer commands.
	env.Complete(result, err)
}

func completionError(message string, canceled, failed bool) error {
	if canceled {
		return temporal.NewCanceledError()
	}
	if failed || message != "" {
		return errors.New(message)
	}
	return nil
}

// handle emits or answers one host command. Replies are held until suspension.
func (d *definition) handle(command *isolate.Command) error {
	if d.completed && d.instance != nil && command.Op != workflow.OpQuery {
		return nil
	}
	switch command.Op {
	case workflow.OpRegisterQuery, workflow.OpQuery:
		return d.handleQuery(command)
	case workflow.OpGetVersion, workflow.OpIsReplaying, workflow.OpResolveWorkflowName:
		return d.handleVersion(command)
	case workflow.OpScheduleActivity, workflow.OpAwaitActivity, workflow.OpCancelActivity:
		return d.handleActivity(command)
	case isolate.LogOp:
		record, err := isolate.DecodeLog(command.Payload)
		if err != nil {
			return err
		}
		d.writeLog(record)
		d.replyWhenSuspended(command, nil, nil)
		return nil
	case workflow.OpWorkflowCancel:
		if d.cancelWaiter != nil {
			return errors.New("duplicate workflow cancellation listener")
		}
		if d.canceled || d.completed {
			d.replyWhenSuspended(command, nil, nil)
		} else {
			d.cancelWaiter = command
		}
		return nil
	case workflow.OpCancellableCall:
		var request workflow.CallRequest
		if err := json.Unmarshal(command.Payload, &request); err != nil {
			return err
		}
		if request.ID == 0 || (request.Op != workflow.OpActivityPayloads && request.Op != workflow.OpSleep && request.Op != workflow.OpSignal) {
			return errors.New("invalid cancellable call")
		}
		if d.calls[request.ID] != nil {
			return errors.New("duplicate cancellable call ID")
		}
		if d.earlyCancel[request.ID] {
			delete(d.earlyCancel, request.ID)
			d.replyWhenSuspended(command, nil, context.Canceled)
			return nil
		}
		if d.calls == nil {
			d.calls = make(map[uint64]*callState)
		}
		if d.callsByCommand == nil {
			d.callsByCommand = make(map[*isolate.Command]*callState)
		}
		state := &callState{id: request.ID, command: command}
		d.calls[request.ID], d.callsByCommand[command] = state, state
		command.Op, command.Payload = request.Op, request.Payload
		return d.handle(command)
	case workflow.OpCancelCall:
		var id uint64
		if err := json.Unmarshal(command.Payload, &id); err != nil {
			return err
		}
		if id == 0 {
			return errors.New("invalid cancel call ID")
		}
		if state := d.calls[id]; state != nil {
			original, cancel := state.command, state.cancel
			// Finish first so synchronous or late SDK callbacks cannot reply twice.
			d.finish(state.command, state, nil, context.Canceled, true)
			for index, waiter := range d.wantSignals {
				if waiter.command == original {
					d.wantSignals = append(d.wantSignals[:index], d.wantSignals[index+1:]...)
					break
				}
			}
			if cancel != nil {
				cancel()
			}
		} else {
			// A cancel callback may send before the original Call's transport wait.
			// These small IDs are discarded when that call arrives or workflow closes.
			if d.earlyCancel == nil {
				d.earlyCancel = make(map[uint64]bool)
			}
			d.earlyCancel[id] = true
		}
		d.replyWhenSuspended(command, nil, nil)
		return nil
	case workflow.OpInput:
		input, err := d.inputBytes()
		if err != nil {
			return err
		}
		d.replyWhenSuspended(command, input, nil)
	case workflow.OpStart:
		if d.entryName == "" {
			return errors.New("workflow entry name is not configured")
		}
		input, err := d.inputBytes()
		if err != nil {
			return err
		}
		payload, err := json.Marshal(workflow.Start{Name: d.entryName, Input: input})
		if err != nil {
			return err
		}
		d.replyWhenSuspended(command, payload, nil)
	case workflow.OpStartPayloads:
		if d.entryName == "" {
			return errors.New("workflow entry name is not configured")
		}
		var input []byte
		if d.input != nil {
			var err error
			input, err = proto.MarshalOptions{Deterministic: true}.Marshal(d.input)
			if err != nil {
				return err
			}
		}
		payload, err := json.Marshal(workflow.PayloadStart{Name: d.entryName, Payloads: input, Canceled: d.canceled, TaskQueue: d.env.WorkflowInfo().TaskQueueName, Options: workflow.RunOptions{Namespace: d.env.WorkflowInfo().Namespace, TaskQueue: d.env.WorkflowInfo().TaskQueueName, WorkflowExecutionTimeout: d.env.WorkflowInfo().WorkflowExecutionTimeout, WorkflowRunTimeout: d.env.WorkflowInfo().WorkflowRunTimeout, WorkflowTaskTimeout: d.env.WorkflowInfo().WorkflowTaskTimeout}})
		if err != nil {
			return err
		}
		d.replyWhenSuspended(command, payload, nil)
	case workflow.OpActivity:
		var request workflow.ActivityRequest
		if err := json.Unmarshal(command.Payload, &request); err != nil {
			return err
		}
		if request.Name == "" || request.StartToCloseTimeout <= 0 {
			return errors.New("invalid activity request")
		}
		input, err := d.env.GetDataConverter().ToPayloads(request.Input)
		if err != nil {
			return err
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
		finish := d.completion(command)
		d.env.ExecuteActivity(params, func(result *commonpb.Payloads, cause error) {
			if d.closed {
				return
			}
			var payload []byte
			if cause == nil && result != nil {
				cause = d.env.GetDataConverter().FromPayloads(result, &payload)
			}
			finish(payload, cause)
		})
		return nil
	case workflow.OpActivityPayloads:
		var request workflow.ActivityPayloadRequest
		if err := json.Unmarshal(command.Payload, &request); err != nil {
			return err
		}
		if request.Name == "" || request.StartToCloseTimeout <= 0 {
			return errors.New("invalid activity request")
		}
		if request.Function && d.resolveActivity != nil {
			request.Name = d.resolveActivity(request.Name)
		}
		var input *commonpb.Payloads
		if len(request.Payloads) != 0 {
			input = new(commonpb.Payloads)
			if err := proto.Unmarshal(request.Payloads, input); err != nil {
				return fmt.Errorf("decode activity argument payloads: %w", err)
			}
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
		finish := d.completion(command)
		activityID := d.env.ExecuteActivity(params, func(result *commonpb.Payloads, cause error) {
			if d.closed {
				return
			}
			var payload []byte
			if cause == nil && result != nil {
				payload, cause = proto.MarshalOptions{Deterministic: true}.Marshal(result)
			}
			finish(payload, cause)
		})
		if state := d.callsByCommand[command]; state != nil {
			state.cancel = func() { d.env.RequestCancelActivity(activityID) }
		}
		return nil
	case workflow.OpSleep:
		var duration time.Duration
		if err := json.Unmarshal(command.Payload, &duration); err != nil {
			return err
		}
		if duration < 0 {
			return errors.New("negative timer duration")
		}
		if duration == 0 {
			d.replyWhenSuspended(command, nil, nil)
			return nil
		}
		finish := d.completion(command)
		timerID := d.env.NewTimer(duration, goWorkflow.TimerOptions{}, func(result *commonpb.Payloads, cause error) { finish(nil, cause) })
		if state := d.callsByCommand[command]; state != nil && timerID != nil {
			state.cancel = func() { d.env.RequestCancelTimer(*timerID) }
		}
		return nil
	case workflow.OpSignal:
		name := string(command.Payload)
		index := d.signalIndex(name)
		if index < 0 {
			d.wantSignals = append(d.wantSignals, signalWaiter{name: name, command: command})
			return nil
		}
		payload, _ := json.Marshal(d.signals[index])
		d.signals = append(d.signals[:index], d.signals[index+1:]...)
		d.replyWhenSuspended(command, payload, nil)
	case workflow.OpComplete:
		var completion workflow.Completion
		if err := json.Unmarshal(command.Payload, &completion); err != nil {
			return err
		}
		var result *commonpb.Payloads
		var err error
		if completion.ContinueAsNew != nil {
			return d.finishContinuation(completion.ContinueAsNew)
		}
		if len(completion.Failure) != 0 {
			var transportErr error
			err, transportErr = failurecodec.Decode(completion.Failure, d.env.GetDataConverter())
			if transportErr != nil {
				return fmt.Errorf("decode workflow failure: %w", transportErr)
			}
		} else if completion.Failed || completion.Error != "" {
			err = completionError(completion.Error, completion.Canceled, completion.Failed)
		} else {
			result, err = d.env.GetDataConverter().ToPayloads(completion.Result)
		}
		if err == nil || completion.Failed || completion.Error != "" {
			d.finishWorkflow(result, err)
			return nil
		}
		return err
	case workflow.OpCompletePayloads:
		var completion workflow.PayloadCompletion
		if err := json.Unmarshal(command.Payload, &completion); err != nil {
			return err
		}
		var result *commonpb.Payloads
		var err error
		if completion.ContinueAsNew != nil {
			return d.finishContinuation(completion.ContinueAsNew)
		}
		if len(completion.Failure) != 0 {
			var transportErr error
			err, transportErr = failurecodec.Decode(completion.Failure, d.env.GetDataConverter())
			if transportErr != nil {
				return fmt.Errorf("decode workflow failure: %w", transportErr)
			}
		} else if completion.Failed || completion.Error != "" {
			err = completionError(completion.Error, completion.Canceled, completion.Failed)
		} else if len(completion.Payloads) != 0 {
			result = new(commonpb.Payloads)
			if err := proto.Unmarshal(completion.Payloads, result); err != nil {
				return fmt.Errorf("decode workflow result payloads: %w", err)
			}
		}
		d.finishWorkflow(result, err)
		return nil
	default:
		return fmt.Errorf("unknown isolate operation %d", command.Op)
	}
	return nil
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
		d.finish(d.wantSignals[waiterIndex].command, d.callsByCommand[d.wantSignals[waiterIndex].command], payload, nil, false)
		d.wantSignals = append(d.wantSignals[:waiterIndex], d.wantSignals[waiterIndex+1:]...)
		d.signals = append(d.signals[:signalIndex], d.signals[signalIndex+1:]...)
	}
}

func (d *definition) fail(err error) {
	if d.completed {
		return
	}
	var effect *isolate.EffectError
	if errors.As(err, &effect) {
		d.failTask(effect)
	}
	var failure *isolate.PanicError
	var ownership *isolate.OwnershipError
	var exited *isolate.GoexitError
	var startup *isolate.InitializationError
	var exit *isolate.ExitError
	var pending *isolate.KillPendingError
	var resource *isolate.ResourceLimitError
	if errors.As(err, &resource) || errors.As(err, &failure) || errors.As(err, &ownership) || errors.As(err, &exited) || errors.As(err, &startup) || errors.As(err, &exit) || errors.As(err, &pending) || errors.Is(err, isolate.ErrRevoked) {
		d.failTask(err)
	}
	env := d.env
	d.completed = true
	d.Close()
	if d.closeErr != nil {
		panic(&WorkflowTaskError{Cause: d.closeErr})
	}
	env.Complete(nil, err)
}

// WorkflowTaskError reports a lifecycle failure on the host. It is never a
// workflow execution result. The supported worker uses BlockWorkflow to retry
// its task, just as for an ordinary workflow panic.
type WorkflowTaskError struct{ Cause error }

func (e *WorkflowTaskError) Error() string { return e.Cause.Error() }
func (e *WorkflowTaskError) Unwrap() error { return e.Cause }

func (d *definition) failTask(err error) {
	d.completed = true
	var effect *isolate.EffectError
	var failure *isolate.PanicError
	var ownership *isolate.OwnershipError
	var exited *isolate.GoexitError
	var startup *isolate.InitializationError
	var resource *isolate.ResourceLimitError
	if errors.As(err, &resource) {
		if d.instance == nil {
			d.lastResources = resource.Stats
		}
		d.failureStack = resource.Stack
		d.observeResources("limit", resource)
	} else if errors.As(err, &effect) {
		d.failureStack = effect.Stack
	} else if errors.As(err, &failure) {
		d.failureStack = failure.Stack
	} else if errors.As(err, &ownership) {
		d.failureStack = ownership.Stack
	} else if errors.As(err, &exited) {
		d.failureStack = exited.Stack
	} else if errors.As(err, &startup) {
		d.failureStack = startup.Pending.Stack
	}
	d.Close()
	if effect != nil {
		panic(effect) // Preserve the original effect diagnostic contract.
	}
	panic(&WorkflowTaskError{Cause: err})
}

func (d *definition) StackTrace() string {
	if d.closeErr != nil {
		var pending *isolate.KillPendingError
		if errors.As(d.closeErr, &pending) {
			return d.failureStack + "\n" + pending.Error() + "\n" + pending.Stack
		}
		return d.failureStack + "\n" + d.closeErr.Error()
	}
	if d.failureStack != "" {
		return d.failureStack
	}
	return "isolate workflow stack trace unavailable"
}

func (d *definition) Close() {
	if !d.closed {
		d.closed = true
		d.retireOperations()
		d.queryWaiter, d.queryHandlers, d.retainedCompletion = nil, nil, nil
		d.resolveLogHandler = nil
	}
	if d.instance == nil {
		d.env = nil
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	d.closeErr = d.instance.Kill(ctx)
	if d.closeErr == nil {
		d.observeResources("closed", nil)
	} else {
		d.observeResources("termination-pending", d.closeErr)
	}
	if d.closeErr == nil {
		d.instance, d.env = nil, nil
	} else if d.env != nil {
		if logger := d.env.GetLogger(); logger != nil {
			logger.Warn("isolate termination pending", "error", d.closeErr)
		}
	}
}

func (d *definition) retireOperations() {
	for _, state := range d.activities {
		state.waiter, state.cancel, state.payload = nil, nil, nil
		state.retired = true
	}
	d.activities = nil
	for _, state := range d.callsByCommand {
		state.done = true
		state.command, state.cancel = nil, nil
	}
	d.calls, d.callsByCommand, d.earlyCancel = nil, nil, nil
	d.pending, d.immediate, d.signals, d.wantSignals = nil, nil, nil, nil
	d.cancelWaiter, d.input = nil, nil
	d.program = isolate.Program{}
	d.resolveActivity, d.resolveWorkflow = nil, nil
}

// CloseError exposes pending termination to hosts using the low-level factory.
// Calling Close again can finish cleanup; it never revives the instance.
func (d *definition) CloseError() error { return d.closeErr }
