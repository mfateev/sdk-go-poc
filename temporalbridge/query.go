package temporalbridge

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"isolate"
	"slices"
	"time"

	"github.com/mfateev/sdk-go-poc/internal/headerwire"
	"github.com/mfateev/sdk-go-poc/workflow"
	commonpb "go.temporal.io/api/common/v1"
	"google.golang.org/protobuf/proto"
)

type workflowCompletion struct {
	result *commonpb.Payloads
	err    error
}

func (d *definition) publishRetainedCompletion() {
	if c := d.retainedCompletion; c != nil {
		d.retainedCompletion = nil
		d.observeResources("completed-retained", nil)
		d.env.Complete(c.result, c.err)
	}
}

func (d *definition) handleQuery(command *isolate.Command) error {
	if command.Op == workflow.OpQuery {
		if len(command.Payload) != 0 {
			return errors.New("unsolicited query response")
		}
		if d.queryWaiter != nil {
			return errors.New("duplicate query service")
		}
		d.queryWaiter = command
		return nil
	}
	var r workflow.QueryRegistration
	if err := json.Unmarshal(command.Payload, &r); err != nil {
		return err
	}
	if d.queryHandlers == nil {
		d.queryHandlers = make(map[string]workflow.QueryHandlerOptions)
	}
	d.queryHandlers[r.Name] = r.Options
	d.replyWhenSuspended(command, nil, nil)
	return nil
}

// The SDK serializes query handling with workflow task execution. Replies to
// ordinary activities, signals and timers remain queued while only the query
// service has a token. No history timestamp or completion is changed here.
func (d *definition) query(name string, input *commonpb.Payloads, header *commonpb.Header) (*commonpb.Payloads, error) {
	if !d.closed && name == "__temporal_workflow_metadata" {
		return d.workflowMetadata()
	}
	if d.closed || d.instance == nil {
		return nil, errors.New("workflow query state was evicted")
	}
	if _, ok := d.queryHandlers[name]; !ok {
		keys := make([]string, 0, len(d.queryHandlers))
		for key := range d.queryHandlers {
			keys = append(keys, key)
		}
		slices.Sort(keys)
		return nil, fmt.Errorf("unknown queryType %v. KnownQueryTypes=%v", name, keys)
	}
	raw, err := inboundPayloadBytes(input, d.env.GetDataConverter())
	if err != nil {
		return nil, err
	}
	fields, err := headerwire.Encode(header)
	if err != nil {
		return nil, err
	}
	response, err := d.runReadOnly(workflow.QueryRequest{Name: name, Payloads: raw, Header: fields})
	if err != nil {
		return nil, err
	}
	if response.Failed {
		return nil, errors.New(response.Error)
	}
	result := new(commonpb.Payloads)
	if err := proto.Unmarshal(response.Payloads, result); err != nil {
		return nil, err
	}
	return encodeTransport(result, d.env.GetDataConverter())
}

// runReadOnly admits the shared query/validation service at an exact fence.
func (d *definition) runReadOnly(input workflow.QueryRequest) (*workflow.QueryResponse, error) {
	if d.queryWaiter == nil {
		return nil, errors.New("workflow read-only service is not suspended")
	}
	d.querySequence++
	input.ID = d.querySequence
	var observation [16]byte
	if _, err := rand.Read(observation[:]); err != nil {
		return nil, err
	}
	input.ObservationID = hex.EncodeToString(observation[:])
	request, err := json.Marshal(input)
	if err != nil {
		return nil, err
	}
	d.queryWaiter.Reply(request, nil)
	d.queryWaiter = nil
	deadline := time.Second
	if d.resources.MaxTaskDuration > 0 {
		deadline = d.resources.MaxTaskDuration
	}
	ctx, cancel := context.WithTimeout(context.Background(), deadline)
	defer cancel()
	instance := d.instance
	defer d.drainWrites(instance)
	var response *workflow.QueryResponse
	for {
		if err := instance.ResumeReadOnly(); err != nil {
			d.failTask(err)
		}
		suspended := make(chan error, 1)
		go func() { suspended <- instance.Suspend() }()
		handled := false
		handle := func(command *isolate.Command) {
			handled = true
			if command == nil {
				d.failTask(errors.New("query command stream closed"))
			}
			if command.Op == workflow.OpQuery {
				var outcome workflow.QueryResponse
				if err := json.Unmarshal(command.Payload, &outcome); err != nil {
					d.failTask(err)
				}
				if response != nil || outcome.ID != d.querySequence {
					d.failTask(errors.New("invalid query response ID"))
				}
				response, d.queryWaiter = &outcome, command
				return
			}
			// Do not send any workflow command to the SDK from a read-only
			// handler, including calls made directly through isolate.Call/time.
			if command.Op == workflow.OpInfo {
				raw, err := d.infoBytes()
				command.Reply(raw, err)
			} else if command.Op == workflow.OpGetTypedSearchAttributes || command.Op == workflow.OpHasLastCompletionResult || command.Op == workflow.OpLastCompletionResult || command.Op == workflow.OpLastError {
				raw, err := d.readMetadata(command.Op)
				command.Reply(raw, err)
			} else if command.Op == workflow.OpIsReplaying {
				raw, _ := json.Marshal(d.env.IsReplaying())
				command.Reply(raw, nil)
			} else {
				command.Reply(nil, errors.New("query handlers and update validators cannot schedule workflow operations"))
			}
		}
		waiting := true
		for waiting {
			select {
			case message := <-instance.Writes():
				d.handleWrite(message)
			case command := <-instance.Commands():
				handle(command)
			case err := <-suspended:
				if err != nil {
					d.failTask(err)
				}
				waiting = false
			case <-ctx.Done():
				d.failTask(errors.New("read-only handler did not finish before its deadline"))
			case <-instance.Done():
				d.failTask(instance.Wait())
			}
		}
	drain:
		for {
			select {
			case message := <-instance.Writes():
				d.handleWrite(message)
			case command := <-instance.Commands():
				handle(command)
			default:
				break drain
			}
		}
		if !handled {
			if response == nil {
				d.failTask(errors.New("read-only handler blocked without a result"))
			}
			return response, nil
		}
	}
}
