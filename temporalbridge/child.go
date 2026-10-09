package temporalbridge

import (
	"encoding/json"
	"errors"
	"fmt"
	"isolate"

	"github.com/mfateev/sdk-go-poc/internal/failurecodec"
	"github.com/mfateev/sdk-go-poc/workflow"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/sdk/converter"
	bindings "go.temporal.io/sdk/internalbindings"
	"go.temporal.io/sdk/temporal"
	"google.golang.org/protobuf/proto"
)

// SDK callbacks retain only this retireable cell. Closing a definition clears
// waiters/results without requesting cancellation of server-side children.
type childState struct {
	namespace                                                                   string
	resultWaiter, startWaiter                                                   *isolate.Command
	result, start                                                               []byte
	cancel                                                                      func()
	done, started, resultRead, startRead, retired, synchronous, cancelRequested bool
}

func (d *definition) operationOutcome(result *commonpb.Payloads, cause error) []byte {
	o := workflow.ActivityOutcome{}
	if cause == nil && result != nil {
		var err error
		o.Payloads, err = proto.MarshalOptions{Deterministic: true}.Marshal(result)
		if err != nil {
			panic(err)
		}
	}
	if cause != nil {
		o.Failed, o.Error, o.Canceled = true, cause.Error(), temporal.IsCanceledError(cause)
		// The SDK failure schema has no dedicated variants for these two
		// initiation sentinels. Keep their public errors.As identity explicitly.
		switch cause.(type) {
		case *temporal.ChildWorkflowExecutionAlreadyStartedError:
			o.ErrorKind = "child-already-started"
		case *temporal.NamespaceNotFoundError:
			o.ErrorKind = "namespace-not-found"
		}
		var err error
		o.Failure, err = failurecodec.Encode(cause, d.env.GetDataConverter())
		if err != nil {
			panic(fmt.Errorf("encode child operation failure: %w", err))
		}
	}
	raw, err := json.Marshal(o)
	if err != nil {
		panic(err)
	}
	return raw
}

func decodeOptionValues(values map[string][]byte) (map[string]any, error) {
	if values == nil {
		return nil, nil
	}
	result := make(map[string]any, len(values))
	for key, raw := range values {
		p := new(commonpb.Payloads)
		if err := proto.Unmarshal(raw, p); err != nil {
			return nil, err
		}
		if len(p.Payloads) != 1 {
			return nil, errors.New("child option requires one payload")
		}
		result[key] = converter.NewRawValue(p.Payloads[0])
	}
	return result, nil
}

func (d *definition) childParams(request workflow.ChildRequest) (bindings.ExecuteWorkflowParams, error) {
	o := request.Options
	if request.Function && d.resolveWorkflow != nil {
		request.Name = d.resolveWorkflow(request.Name)
	}
	input := new(commonpb.Payloads)
	if err := proto.Unmarshal(request.Payloads, input); err != nil {
		return bindings.ExecuteWorkflowParams{}, err
	}
	memo, err := decodeOptionValues(request.Memo)
	if err != nil {
		return bindings.ExecuteWorkflowParams{}, err
	}
	search, err := decodeOptionValues(request.SearchAttributes)
	if err != nil {
		return bindings.ExecuteWorkflowParams{}, err
	}
	// The SDK accepts existing Payloads for untyped visibility attributes.
	// Preserve both encoding and number precision without decoding to any.
	for key, value := range search {
		search[key] = value.(converter.RawValue).Payload()
	}
	activity := activityOptions(workflow.ActivityOptions{RetryPolicy: o.RetryPolicy, Priority: o.Priority}, "", 0)
	p := bindings.ExecuteWorkflowParams{WorkflowOptions: bindings.WorkflowOptions{
		Namespace: o.Namespace, WorkflowID: o.WorkflowID, TaskQueueName: o.TaskQueue,
		WorkflowExecutionTimeout: o.WorkflowExecutionTimeout, WorkflowRunTimeout: o.WorkflowRunTimeout, WorkflowTaskTimeout: o.WorkflowTaskTimeout,
		WaitForCancellation: o.WaitForCancellation, WorkflowIDReusePolicy: o.WorkflowIDReusePolicy,
		RetryPolicy: activity.RetryPolicy, Priority: activity.Priority, CronSchedule: o.CronSchedule,
		Memo: memo, SearchAttributes: search, ParentClosePolicy: o.ParentClosePolicy, VersioningIntent: o.VersioningIntent,
		StaticSummary: o.StaticSummary, StaticDetails: o.StaticDetails,
		DataConverter: d.env.GetDataConverter(), RootDataConverter: d.env.GetDataConverter(),
	}, WorkflowType: &bindings.WorkflowType{Name: request.Name}, Input: input}
	return p, nil
}

func (d *definition) retireChild(id uint64, s *childState) {
	if s.resultRead && s.startRead {
		s.cancel, s.result, s.start = nil, nil, nil
		s.retired = true
		delete(d.children, id)
	}
}

func (d *definition) childStarted(id uint64, s *childState, execution bindings.WorkflowExecution, cause error) {
	if d.closed || s.retired || s.started {
		return
	}
	s.started = true
	var result *commonpb.Payloads
	if cause == nil {
		var err error
		result, err = d.env.GetDataConverter().ToPayloads(execution)
		if err != nil {
			panic(err)
		}
		if !s.done {
			namespace, workflowID := s.namespace, execution.ID
			s.cancel = func() { d.env.RequestCancelChildWorkflow(namespace, workflowID) }
			if s.cancelRequested {
				s.cancel()
				s.cancel = nil
			}
		}
	}
	s.start = d.operationOutcome(result, cause)
	if s.startWaiter != nil {
		d.finish(s.startWaiter, nil, s.start, nil, s.synchronous)
		s.startWaiter, s.start, s.startRead = nil, nil, true
	}
	d.retireChild(id, s)
}

func (d *definition) handleChild(command *isolate.Command) error {
	if command.Op == workflow.OpSignalChild || command.Op == workflow.OpAwaitChildSignal {
		return d.handleChildSignal(command)
	}
	if command.Op == workflow.OpScheduleChild {
		var r workflow.ChildRequest
		if err := json.Unmarshal(command.Payload, &r); err != nil {
			return err
		}
		if r.ID == 0 || r.Name == "" || r.Options.WorkflowExecutionTimeout < 0 || r.Options.WorkflowRunTimeout < 0 || r.Options.WorkflowTaskTimeout < 0 {
			return errors.New("invalid child schedule")
		}
		if d.children == nil {
			d.children = make(map[uint64]*childState)
		}
		if d.children[r.ID] != nil {
			return errors.New("duplicate child schedule ID")
		}
		params, err := d.childParams(r)
		if err != nil {
			return err
		}
		s := &childState{synchronous: true, namespace: params.Namespace}
		d.children[r.ID] = s
		id := r.ID
		d.env.ExecuteChildWorkflow(params, func(result *commonpb.Payloads, cause error) {
			if d.closed || s.retired || s.done {
				return
			}
			s.done, s.cancel = true, nil
			// Older histories can omit the start-error callback. Always resolve
			// both futures when initiation fails, without inventing a start.
			if !s.started && cause != nil {
				d.childStarted(id, s, bindings.WorkflowExecution{}, cause)
			}
			s.result = d.operationOutcome(result, cause)
			if s.resultWaiter != nil {
				d.finish(s.resultWaiter, nil, s.result, nil, s.synchronous)
				s.resultWaiter, s.result, s.resultRead = nil, nil, true
			}
			d.retireChild(id, s)
		}, func(execution bindings.WorkflowExecution, cause error) {
			d.childStarted(id, s, execution, cause)
		})
		s.synchronous = false
		d.replyWhenSuspended(command, nil, nil)
		return nil
	}
	var id uint64
	if err := json.Unmarshal(command.Payload, &id); err != nil {
		return err
	}
	s := d.children[id]
	if command.Op == workflow.OpCancelChild {
		if s != nil && !s.done && !s.cancelRequested {
			s.cancelRequested = true
			if s.started && s.cancel != nil {
				s.synchronous = true
				s.cancel()
				s.cancel, s.synchronous = nil, false
			}
		}
		d.replyWhenSuspended(command, nil, nil)
		return nil
	}
	if s == nil {
		return errors.New("unknown child schedule ID")
	}
	if command.Op == workflow.OpAwaitChildExecution {
		if s.startWaiter != nil || s.startRead {
			return errors.New("duplicate child execution await")
		}
		if s.started {
			d.replyWhenSuspended(command, s.start, nil)
			s.start, s.startRead = nil, true
		} else {
			s.startWaiter = command
		}
	} else {
		if s.resultWaiter != nil || s.resultRead {
			return errors.New("duplicate child result await")
		}
		if s.done {
			d.replyWhenSuspended(command, s.result, nil)
			s.result, s.resultRead = nil, true
		} else {
			s.resultWaiter = command
		}
	}
	d.retireChild(id, s)
	return nil
}

func (d *definition) handleChildSignal(command *isolate.Command) error {
	if command.Op == workflow.OpSignalChild {
		var r workflow.ChildSignalRequest
		if err := json.Unmarshal(command.Payload, &r); err != nil {
			return err
		}
		if r.ID == 0 || r.WorkflowID == "" {
			return errors.New("invalid child signal")
		}
		if d.childSignals == nil {
			d.childSignals = make(map[uint64]*activityState)
		}
		if d.childSignals[r.ID] != nil {
			return errors.New("duplicate child signal ID")
		}
		input := new(commonpb.Payloads)
		if err := proto.Unmarshal(r.Payloads, input); err != nil {
			return err
		}
		s := &activityState{synchronous: true}
		d.childSignals[r.ID] = s
		id := r.ID
		d.env.SignalExternalWorkflow(r.Namespace, r.WorkflowID, "", r.Name, input, nil, nil, true, func(result *commonpb.Payloads, cause error) {
			if d.closed || s.retired || s.done {
				return
			}
			s.done, s.payload = true, d.operationOutcome(result, cause)
			if s.waiter != nil {
				d.finish(s.waiter, nil, s.payload, nil, s.synchronous)
				s.waiter, s.payload, s.retired = nil, nil, true
				delete(d.childSignals, id)
			}
		})
		s.synchronous = false
		d.replyWhenSuspended(command, nil, nil)
		return nil
	}
	var id uint64
	if err := json.Unmarshal(command.Payload, &id); err != nil {
		return err
	}
	s := d.childSignals[id]
	if s == nil || s.waiter != nil {
		return errors.New("unknown or duplicate child signal await")
	}
	if s.done {
		d.replyWhenSuspended(command, s.payload, nil)
		s.payload, s.retired = nil, true
		delete(d.childSignals, id)
	} else {
		s.waiter = command
	}
	return nil
}
