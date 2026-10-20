package temporalbridge

import (
	"errors"
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
		raw, err := workflow.MarshalTransport(d.env.IsReplaying())
		if err != nil {
			return err
		}
		d.replyWhenSuspended(command, raw, nil)
	case workflow.OpGetVersion:
		var request workflow.VersionRequest
		if err := workflow.UnmarshalTransport(command.Payload, &request); err != nil {
			return err
		}
		version := d.env.GetVersion(request.ChangeID, request.Min, request.Max)
		raw, err := workflow.MarshalTransport(version)
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
	input := r.Payloads
	if input == nil {
		input = new(commonpb.Payloads)
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
		e.Header = proto.Clone(r.Header).(*commonpb.Header)
	}

	d.finishWorkflow(nil, e)
	return nil
}
