package temporalbridge

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"isolate"
	"reflect"

	"github.com/mfateev/sdk-go-poc/internal/headerwire"
	"github.com/mfateev/sdk-go-poc/workflow"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/sdk/converter"
	bindings "go.temporal.io/sdk/internalbindings"
	"go.temporal.io/sdk/temporal"
	"google.golang.org/protobuf/proto"
)

// Local activities use actual host arguments. Decode into their registered
// function's exact types, preserving integer precision and custom converters.
func localArguments(fn any, p *commonpb.Payloads, dc converter.DataConverter) ([]any, error) {
	t := reflect.TypeOf(fn)
	if t == nil || t.Kind() != reflect.Func || t.NumIn() == 0 || t.In(0) != reflect.TypeFor[context.Context]() {
		return nil, errors.New("local activity must take context.Context first")
	}
	if len(p.Payloads) != t.NumIn()-1 {
		return nil, fmt.Errorf("local activity got %d payloads, want %d", len(p.Payloads), t.NumIn()-1)
	}
	args, pointers := make([]any, len(p.Payloads)), make([]any, len(p.Payloads))
	for i := range pointers {
		pointers[i] = reflect.New(t.In(i + 1)).Interface()
	}
	if err := dc.FromPayloads(p, pointers...); err != nil {
		return nil, err
	}
	for i := range args {
		args[i] = reflect.ValueOf(pointers[i]).Elem().Interface()
	}
	return args, nil
}

func (d *definition) handleLocal(c *isolate.Command) error {
	if c.Op == workflow.OpAwaitLocal {
		return d.awaitState(c, d.locals)
	}
	if c.Op == workflow.OpCancelLocal {
		var id uint64
		if err := json.Unmarshal(c.Payload, &id); err != nil {
			return err
		}
		if s := d.locals[id]; s != nil && !s.done && !s.cancelRequested {
			s.cancelRequested = true
			s.synchronous = true
			if s.cancel != nil {
				s.cancel()
			}
			s.synchronous = false
		}
		d.replyWhenSuspended(c, nil, nil)
		return nil
	}
	var r workflow.LocalActivityRequest
	if err := json.Unmarshal(c.Payload, &r); err != nil {
		return err
	}
	if r.ID == 0 || r.Name == "" || r.Attempt < 1 || r.Options.ScheduleToCloseTimeout <= 0 || r.Options.StartToCloseTimeout <= 0 {
		return errors.New("invalid local activity schedule")
	}
	if d.locals == nil {
		d.locals = make(map[uint64]*activityState)
	}
	if d.locals[r.ID] != nil {
		return errors.New("duplicate local activity ID")
	}
	if r.Function && d.resolveActivity != nil {
		r.Name = d.resolveActivity(r.Name)
	}
	var fn any
	if d.resolveLocalActivity != nil {
		fn = d.resolveLocalActivity(r.Name)
	} else if a, ok := d.env.GetRegistry().GetActivity(r.Name); ok {
		fn = a.GetFunction()
	}
	info := d.env.WorkflowInfo()
	dc := converter.WithDataConverterSerializationContext(d.rootDataConverter(), converter.ActivitySerializationContext{Namespace: info.Namespace, WorkflowID: info.WorkflowExecution.ID, WorkflowType: info.WorkflowType.Name, ActivityType: r.Name, TaskQueue: info.TaskQueueName, IsLocal: true})
	var args []any
	if fn == nil {
		if !d.replayOnly {
			return d.localScheduleError(c, r.ID, fmt.Errorf("local activity %s is not registered", r.Name), dc)
		}
		fn = func(context.Context) error { panic("replay must never execute local activity") }
	} else {
		p := new(commonpb.Payloads)
		if err := proto.Unmarshal(r.Payloads, p); err != nil {
			return err
		}
		p, err := encodeTransport(p, dc)
		if err != nil {
			return err
		}
		args, err = localArguments(fn, p, dc)
		if err != nil {
			return d.localScheduleError(c, r.ID, err, dc)
		}
	}
	s := &activityState{synchronous: true}
	d.locals[r.ID] = s
	id := r.ID
	header, err := headerwire.Decode(r.Header)
	if err != nil {
		return err
	}
	params := bindings.ExecuteLocalActivityParams{ExecuteLocalActivityOptions: bindings.ExecuteLocalActivityOptions{ScheduleToCloseTimeout: r.Options.ScheduleToCloseTimeout, StartToCloseTimeout: r.Options.StartToCloseTimeout, RetryPolicy: r.Options.RetryPolicy, Summary: r.Options.Summary}, ActivityFn: fn, ActivityType: r.Name, InputArgs: args, WorkflowInfo: info, DataConverter: dc, FailureConverter: temporal.NewDefaultFailureConverter(temporal.DefaultFailureConverterOptions{DataConverter: dc}), Attempt: r.Attempt, ScheduledTime: r.ScheduledTime, Header: header}
	activityID := d.env.ExecuteLocalActivity(params, func(result *bindings.LocalActivityResultWrapper) {
		if d.closed || s.retired || s.done {
			return
		}
		if result == nil {
			panic("nil local activity result")
		}
		var outcome workflow.LocalActivityOutcome
		if err := json.Unmarshal(d.operationOutcome(result.Result, result.Err, dc), &outcome.ActivityOutcome); err != nil {
			panic(err)
		}
		if result.Err != nil && !temporal.IsCanceledError(result.Err) {
			outcome.Backoff, outcome.Attempt = result.Backoff, result.Attempt
		}
		p, err := json.Marshal(outcome)
		if err != nil {
			panic(err)
		}
		d.completeState(d.locals, id, s, p)
	})
	s.synchronous = false
	if !s.done {
		s.cancel = func() { d.env.RequestCancelLocalActivity(activityID) }
	}
	d.replyWhenSuspended(c, nil, nil)
	return nil
}

func (d *definition) localScheduleError(c *isolate.Command, id uint64, err error, dc converter.DataConverter) error {
	d.locals[id] = &activityState{done: true, payload: d.operationOutcome(nil, err, dc)}
	d.replyWhenSuspended(c, nil, nil)
	return nil
}
