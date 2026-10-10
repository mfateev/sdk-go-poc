package temporalbridge

import (
	"encoding/json"
	"errors"
	"fmt"
	"isolate"

	"github.com/mfateev/sdk-go-poc/internal/headerwire"
	"github.com/mfateev/sdk-go-poc/workflow"
	commonpb "go.temporal.io/api/common/v1"
	bindings "go.temporal.io/sdk/internalbindings"
	"go.temporal.io/sdk/temporal"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
)

type activityState struct {
	waiter                                      *isolate.Command
	cancel                                      func()
	payload                                     []byte
	done, retired, cancelRequested, synchronous bool
}

func activityOptions(o workflow.ActivityOptions, taskQueue string, sequence int64) bindings.ExecuteActivityOptions {
	originalTaskQueue := taskQueue
	if o.TaskQueue != "" {
		taskQueue = o.TaskQueue
	}
	p := bindings.ExecuteActivityOptions{
		ActivityID: o.ActivityID, TaskQueueName: taskQueue, OriginalTaskQueueName: originalTaskQueue, ScheduleID: sequence,
		ScheduleToCloseTimeout: o.ScheduleToCloseTimeout, ScheduleToStartTimeout: o.ScheduleToStartTimeout,
		StartToCloseTimeout: o.StartToCloseTimeout, HeartbeatTimeout: o.HeartbeatTimeout,
		WaitForCancellation: o.WaitForCancellation, DisableEagerExecution: o.DisableEagerExecution,
		VersioningIntent: o.VersioningIntent, Summary: o.Summary,
	}
	if o.RetryPolicy != nil {
		r := o.RetryPolicy
		p.RetryPolicy = &commonpb.RetryPolicy{
			InitialInterval: durationpb.New(r.InitialInterval), BackoffCoefficient: r.BackoffCoefficient,
			MaximumInterval: durationpb.New(r.MaximumInterval), MaximumAttempts: r.MaximumAttempts,
			NonRetryableErrorTypes: r.NonRetryableErrorTypes,
		}
	}
	if o.Priority != (workflow.Priority{}) {
		p.Priority = &commonpb.Priority{PriorityKey: int32(o.Priority.PriorityKey), FairnessKey: o.Priority.FairnessKey, FairnessWeight: o.Priority.FairnessWeight}
	}
	return p
}

// Scheduling and awaiting are separate so ExecuteActivity schedules before it
// returns. The host SDK controls cancel completion and WaitForCancellation.
func (d *definition) handleActivity(command *isolate.Command) error {
	if command.Op == workflow.OpScheduleActivity {
		var request workflow.ActivityPayloadRequest
		if err := json.Unmarshal(command.Payload, &request); err != nil {
			return err
		}
		if request.ID == 0 || request.Name == "" || request.Options == nil {
			return errors.New("invalid activity schedule")
		}
		if d.activities == nil {
			d.activities = make(map[uint64]*activityState)
		}
		if d.activities[request.ID] != nil {
			return errors.New("duplicate activity schedule ID")
		}
		o := *request.Options
		if o.StartToCloseTimeout < 0 || o.ScheduleToCloseTimeout < 0 || o.ScheduleToStartTimeout < 0 || o.HeartbeatTimeout < 0 || (o.StartToCloseTimeout == 0 && o.ScheduleToCloseTimeout == 0) {
			return errors.New("invalid activity timeouts")
		}
		if request.Function && d.resolveActivity != nil {
			request.Name = d.resolveActivity(request.Name)
		}
		input := new(commonpb.Payloads)
		if err := proto.Unmarshal(request.Payloads, input); err != nil {
			return fmt.Errorf("decode activity arguments: %w", err)
		}
		options := activityOptions(o, d.env.WorkflowInfo().TaskQueueName, d.env.GenerateSequence())
		dc := d.activityDataConverter(request.Name, options.TaskQueueName)
		input, err := encodeTransport(input, dc)
		if err != nil {
			return err
		}
		header, err := headerwire.Decode(request.Header)
		if err != nil {
			return err
		}
		params := bindings.ExecuteActivityParams{ExecuteActivityOptions: options, ActivityType: bindings.ActivityType{Name: request.Name}, Input: input, DataConverter: dc, Header: header,
			FailureConverter: temporal.NewDefaultFailureConverter(temporal.DefaultFailureConverterOptions{DataConverter: dc})}
		state := &activityState{synchronous: true}
		d.activities[request.ID] = state
		callID := request.ID // Do not retain the request or its argument bytes in SDK callbacks.
		id := d.env.ExecuteActivity(params, func(result *commonpb.Payloads, cause error) {
			if d.closed || state.retired || state.done {
				return
			}
			outcome := workflow.ActivityOutcome{}
			if cause == nil && result != nil {
				outcome.Payloads, cause = inboundPayloadBytes(result, dc)
			}
			if cause != nil {
				outcome.Failed = true
				outcome.Error = cause.Error()
				outcome.Canceled = temporal.IsCanceledError(cause)
				var err error
				outcome.Failure, err = encodeInboundFailure(cause, dc)
				if err != nil {
					panic(fmt.Errorf("encode activity failure: %w", err))
				}
			}
			payload, err := json.Marshal(outcome)
			if err != nil {
				panic(err)
			}
			state.payload, state.done, state.cancel = payload, true, nil
			if state.waiter != nil {
				d.finish(state.waiter, nil, payload, nil, state.synchronous)
				state.waiter, state.payload = nil, nil
				state.retired = true
				delete(d.activities, callID)
			}
		})
		state.synchronous = false
		if !state.done {
			state.cancel = func() { d.env.RequestCancelActivity(id) }
		}
		d.replyWhenSuspended(command, nil, nil)
		return nil
	}
	var id uint64
	if err := json.Unmarshal(command.Payload, &id); err != nil {
		return err
	}
	state := d.activities[id]
	if command.Op == workflow.OpCancelActivity {
		// A completed future may already have retired its state.
		if state != nil && !state.done && !state.cancelRequested {
			state.cancelRequested = true
			if state.cancel != nil {
				state.synchronous = true
				state.cancel()
				state.synchronous = false
			}
		}
		d.replyWhenSuspended(command, nil, nil)
		return nil
	}
	if state == nil {
		return errors.New("unknown activity schedule ID")
	}
	if state.waiter != nil {
		return errors.New("duplicate activity await")
	}
	if state.done {
		d.replyWhenSuspended(command, state.payload, nil)
		state.payload, state.retired = nil, true
		delete(d.activities, id)
	} else {
		state.waiter = command
	}
	return nil
}
