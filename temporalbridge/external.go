package temporalbridge

import (
	"encoding/json"
	"errors"
	"isolate"

	"github.com/mfateev/sdk-go-poc/internal/headerwire"
	"github.com/mfateev/sdk-go-poc/workflow"
	commonpb "go.temporal.io/api/common/v1"
	"google.golang.org/protobuf/proto"
)

func (d *definition) handleExternal(c *isolate.Command) error {
	if c.Op == workflow.OpAwaitExternal {
		return d.awaitState(c, d.external)
	}
	var r workflow.ExternalRequest
	if err := json.Unmarshal(c.Payload, &r); err != nil {
		return err
	}
	if r.ID == 0 || r.WorkflowID == "" || (!r.Cancel && r.SignalName == "") {
		return errors.New("invalid external workflow request")
	}
	if d.external == nil {
		d.external = make(map[uint64]*activityState)
	}
	if d.external[r.ID] != nil {
		return errors.New("duplicate external request ID")
	}
	namespace := r.Namespace
	if namespace == "" {
		namespace = d.env.WorkflowInfo().Namespace
	}
	dc := d.workflowDataConverter(namespace, r.WorkflowID)
	s := &activityState{synchronous: true}
	d.external[r.ID] = s
	id := r.ID
	callback := func(p *commonpb.Payloads, err error) {
		if d.closed || s.retired || s.done {
			return
		}
		d.completeState(d.external, id, s, d.operationOutcome(p, err, dc))
	}
	if r.Cancel {
		d.env.RequestCancelExternalWorkflow(namespace, r.WorkflowID, r.RunID, callback)
	} else {
		p := new(commonpb.Payloads)
		if err := proto.Unmarshal(r.Payloads, p); err != nil {
			return err
		}
		p, err := encodeTransport(p, dc)
		if err != nil {
			return err
		}
		header, err := headerwire.Decode(r.Header)
		if err != nil {
			return err
		}
		d.env.SignalExternalWorkflow(namespace, r.WorkflowID, r.RunID, r.SignalName, p, nil, header, false, callback)
	}
	s.synchronous = false
	d.replyWhenSuspended(c, nil, nil)
	return nil
}

// State cells can outlive the definition in SDK callbacks. Retire them before
// detaching the host environment; late callbacks must not retain or revive work.
func (d *definition) completeState(states map[uint64]*activityState, id uint64, s *activityState, payload []byte) {
	s.done, s.payload, s.cancel = true, payload, nil
	if s.waiter != nil {
		d.finish(s.waiter, nil, payload, nil, s.synchronous)
		s.waiter, s.payload, s.retired = nil, nil, true
		delete(states, id)
	}
}
func (d *definition) awaitState(c *isolate.Command, states map[uint64]*activityState) error {
	var id uint64
	if err := json.Unmarshal(c.Payload, &id); err != nil {
		return err
	}
	s := states[id]
	if s == nil || s.waiter != nil {
		return errors.New("unknown or duplicate operation await")
	}
	if s.done {
		d.replyWhenSuspended(c, s.payload, nil)
		s.payload, s.retired = nil, true
		delete(states, id)
	} else {
		s.waiter = c
	}
	return nil
}
func retireStates(states map[uint64]*activityState) {
	for _, s := range states {
		s.waiter, s.payload, s.cancel = nil, nil, nil
		s.retired = true
	}
}
