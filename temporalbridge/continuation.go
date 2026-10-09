package temporalbridge

import (
	"encoding/json"
	"errors"
	"fmt"
	"isolate"
	"time"

	"github.com/mfateev/sdk-go-poc/workflow"
	commonpb "go.temporal.io/api/common/v1"
	bindings "go.temporal.io/sdk/internalbindings"
	"google.golang.org/protobuf/proto"
)

func (d *definition) handleVersion(command *isolate.Command) error {
	switch command.Op {
	case workflow.OpResolveWorkflowName:
		name := string(command.Payload)
		if d.resolveWorkflow != nil {
			name = d.resolveWorkflow(name)
		}
		d.replyWhenSuspended(command, []byte(name), nil)
	case workflow.OpIsReplaying:
		raw, err := json.Marshal(d.env.IsReplaying())
		if err != nil {
			return err
		}
		d.replyWhenSuspended(command, raw, nil)
	case workflow.OpGetVersion:
		var request workflow.VersionRequest
		if err := json.Unmarshal(command.Payload, &request); err != nil {
			return err
		}
		version := d.env.GetVersion(request.ChangeID, request.Min, request.Max)
		raw, err := json.Marshal(version)
		if err != nil {
			return err
		}
		d.replyWhenSuspended(command, raw, nil)
	}
	return nil
}

func (d *definition) finishContinuation(r *workflow.ContinueAsNewRequest) error {
	if r.Name == "" {
		return errors.New("continue-as-new: empty workflow type")
	}
	input := new(commonpb.Payloads)
	if err := proto.Unmarshal(r.Payloads, input); err != nil {
		return fmt.Errorf("decode continuation input: %w", err)
	}
	input, err := encodeTransport(input, d.env.GetDataConverter())
	if err != nil {
		return err
	}
	e := &bindings.ContinueAsNewError{WorkflowType: &bindings.WorkflowType{Name: r.Name}, Input: input,
		TaskQueueName: r.Options.TaskQueue, WorkflowRunTimeout: r.Options.WorkflowRunTimeout,
		WorkflowTaskTimeout: r.Options.WorkflowTaskTimeout, WorkflowExecutionTimeout: r.Options.WorkflowExecutionTimeout,
		VersioningIntent: r.Options.VersioningIntent, RetryPolicy: r.RetryPolicy,
		BackoffStartInterval: time.Duration(r.BackoffStartInterval), InitialVersioningBehavior: r.InitialVersioningBehavior}
	if r.Header != nil {
		e.Header = &commonpb.Header{Fields: make(map[string]*commonpb.Payload, len(r.Header))}
		for key, raw := range r.Header {
			p := new(commonpb.Payloads)
			if err := proto.Unmarshal(raw, p); err != nil {
				return err
			}
			if len(p.Payloads) != 1 {
				return fmt.Errorf("continuation header %q: expected one payload", key)
			}
			e.Header.Fields[key] = p.Payloads[0]
		}
	}
	d.finishWorkflow(nil, e)
	return nil
}
