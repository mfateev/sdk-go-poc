package temporalbridge

import (
	"encoding/json"
	"errors"
	"isolate"

	"github.com/mfateev/sdk-go-poc/workflow"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/sdk/converter"
	bindings "go.temporal.io/sdk/internalbindings"
	"go.temporal.io/sdk/temporal"
	"google.golang.org/protobuf/proto"
)

type nexusState struct {
	childState
	seq    int64
	policy workflow.NexusOperationCancellationType
}

func (d *definition) retireNexus(id uint64, s *nexusState) {
	if s.resultRead && s.startRead {
		s.result, s.start, s.cancel = nil, nil, nil
		s.retired = true
		delete(d.nexus, id)
	}
}
func (d *definition) nexusStarted(id uint64, s *nexusState, token string, err error) {
	if d.closed || s.retired || s.started {
		return
	}
	s.started = true
	var p *commonpb.Payloads
	if err == nil {
		p, err = converter.GetDefaultDataConverter().ToPayloads(workflow.NexusOperationExecution{OperationToken: token})
	}
	s.start = d.operationOutcome(p, err, converter.GetDefaultDataConverter())
	if s.startWaiter != nil {
		d.finish(s.startWaiter, nil, s.start, nil, s.synchronous)
		s.startWaiter, s.start, s.startRead = nil, nil, true
	}
	d.retireNexus(id, s)
}
func (d *definition) nexusCompleted(id uint64, s *nexusState, p *commonpb.Payloads, err error) {
	if d.closed || s.retired || s.done {
		return
	}
	if !s.started {
		d.nexusStarted(id, s, "", err)
	}
	s.done, s.cancel = true, nil
	s.result = d.operationOutcome(p, err, s.dc)
	if s.resultWaiter != nil {
		d.finish(s.resultWaiter, nil, s.result, nil, s.synchronous)
		s.resultWaiter, s.result, s.resultRead = nil, nil, true
	}
	d.retireNexus(id, s)
}
func (d *definition) handleNexus(c *isolate.Command) error {
	if c.Op == workflow.OpScheduleNexus {
		var r workflow.NexusRequest
		if err := json.Unmarshal(c.Payload, &r); err != nil {
			return err
		}
		if r.ID == 0 || r.Endpoint == "" || r.Service == "" || r.Operation == "" || r.Options.CancellationType < workflow.NexusOperationCancellationTypeAbandon || r.Options.CancellationType > workflow.NexusOperationCancellationTypeWaitCompleted || r.Options.ScheduleToCloseTimeout < 0 || r.Options.ScheduleToStartTimeout < 0 || r.Options.StartToCloseTimeout < 0 {
			return errors.New("invalid Nexus schedule")
		}
		if d.nexus == nil {
			d.nexus = make(map[uint64]*nexusState)
		}
		if d.nexus[r.ID] != nil {
			return errors.New("duplicate Nexus request ID")
		}
		p := new(commonpb.Payloads)
		if err := proto.Unmarshal(r.Payloads, p); err != nil {
			return err
		}
		if len(p.Payloads) != 1 {
			return errors.New("Nexus input must contain one payload")
		}
		dc := converter.WithDataConverterSerializationContext(d.rootDataConverter(), converter.NexusSerializationContext{Endpoint: r.Endpoint, Service: r.Service, Operation: r.Operation})
		p, err := encodeTransport(p, dc)
		if err != nil {
			return err
		}
		s := &nexusState{childState: childState{synchronous: true, dc: dc}, policy: r.Options.CancellationType}
		d.nexus[r.ID] = s
		id := r.ID
		params := bindings.NewExecuteNexusOperationParams(bindings.NewNexusClient(r.Endpoint, r.Service), r.Operation, p.Payloads[0], r.Options, nil)
		s.seq = d.env.ExecuteNexusOperation(params, func(result *commonpb.Payload, err error) {
			var payloads *commonpb.Payloads
			if result != nil {
				payloads = &commonpb.Payloads{Payloads: []*commonpb.Payload{result}}
			}
			d.nexusCompleted(id, s, payloads, err)
		}, func(token string, err error) { d.nexusStarted(id, s, token, err) })
		s.synchronous = false
		d.replyWhenSuspended(c, nil, nil)
		return nil
	}
	var id uint64
	if err := json.Unmarshal(c.Payload, &id); err != nil {
		return err
	}
	s := d.nexus[id]
	if c.Op == workflow.OpCancelNexus {
		if s != nil && !s.done && !s.cancelRequested {
			s.cancelRequested, s.synchronous = true, true
			if s.policy == workflow.NexusOperationCancellationTypeAbandon {
				d.env.AbandonNexusOperation(s.seq)
				d.nexusCompleted(id, s, nil, temporal.NewCanceledError())
			} else {
				d.env.RequestCancelNexusOperation(s.seq)
			}
			s.synchronous = false
		}
		d.replyWhenSuspended(c, nil, nil)
		return nil
	}
	if s == nil {
		return errors.New("unknown Nexus operation")
	}
	if c.Op == workflow.OpAwaitNexusExecution {
		if s.startWaiter != nil || s.startRead {
			return errors.New("duplicate Nexus execution await")
		}
		if s.started {
			d.replyWhenSuspended(c, s.start, nil)
			s.start, s.startRead = nil, true
			d.retireNexus(id, s)
		} else {
			s.startWaiter = c
		}
	} else {
		if s.resultWaiter != nil || s.resultRead {
			return errors.New("duplicate Nexus result await")
		}
		if s.done {
			d.replyWhenSuspended(c, s.result, nil)
			s.result, s.resultRead = nil, true
			d.retireNexus(id, s)
		} else {
			s.resultWaiter = c
		}
	}
	return nil
}
