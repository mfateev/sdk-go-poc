package temporalbridge

import (
	"encoding/json"
	"errors"
	"fmt"
	"isolate"
	"slices"

	"github.com/mfateev/sdk-go-poc/internal/headerwire"
	"github.com/mfateev/sdk-go-poc/workflow"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/sdk/converter"
	bindings "go.temporal.io/sdk/internalbindings"
	"go.temporal.io/sdk/temporal"
	"google.golang.org/protobuf/proto"
)

type updateState struct {
	id, name  string
	payload   []byte
	header    map[string][]byte
	callbacks bindings.UpdateCallbacks
	accepted  bool
}

// History callbacks queue input only. Validation and acceptance take place at
// a suspension fence, never while workflow goroutines are executing.
func (d *definition) queueUpdate(name, id string, input *commonpb.Payloads, header *commonpb.Header, callbacks bindings.UpdateCallbacks) {
	if d.completed || d.closed {
		callbacks.Reject(errors.New("workflow has completed"))
		return
	}
	if d.updates == nil {
		d.updates = make(map[string]*updateState)
	}
	if d.updates[id] != nil {
		callbacks.Reject(errors.New("duplicate update ID"))
		return
	}
	raw, err := inboundPayloadBytes(input, d.env.GetDataConverter())
	if err != nil {
		callbacks.Reject(err)
		return
	}
	fields, err := headerwire.Encode(header)
	if err != nil {
		callbacks.Reject(err)
		return
	}
	state := &updateState{id: id, name: name, payload: raw, header: fields, callbacks: callbacks}
	d.updates[id] = state
	d.queuedUpdates = append(d.queuedUpdates, state)
}

func (d *definition) handleUpdate(command *isolate.Command) error {
	switch command.Op {
	case workflow.OpRegisterUpdate:
		var r workflow.UpdateRegistration
		if err := json.Unmarshal(command.Payload, &r); err != nil {
			return err
		}
		if d.updateHandlers == nil {
			d.updateHandlers = make(map[string]workflow.UpdateRegistration)
		}
		d.updateHandlers[r.Name] = r
		// SDK registration yields to updates awaiting this handler. Keep the
		// registering goroutine parked until admitted handlers reach idle.
		d.updateRegistrations = append(d.updateRegistrations, command)
	case workflow.OpNextUpdate:
		if d.updateWaiter != nil {
			return errors.New("duplicate update delivery service")
		}
		d.updateWaiter = command
	case workflow.OpCompleteUpdate:
		var r workflow.UpdateCompletion
		if err := json.Unmarshal(command.Payload, &r); err != nil {
			return err
		}
		state := d.updates[r.ID]
		if state == nil || !state.accepted {
			return errors.New("completion for an unaccepted update")
		}
		var result any
		var cause error
		if len(r.Failure) != 0 {
			var err error
			cause, err = decodeOutboundFailure(r.Failure, d.env.GetDataConverter())
			if err != nil {
				return err
			}
		} else {
			payloads := new(commonpb.Payloads)
			if err := proto.Unmarshal(r.Payloads, payloads); err != nil {
				return err
			}
			if len(payloads.Payloads) != 1 {
				return errors.New("update result must contain exactly one payload")
			}
			// Forward the already encoded result, preserving JSON integer values.
			result = converter.NewRawValue(payloads.Payloads[0])
		}
		callbacks := state.callbacks
		state.callbacks, state.payload = nil, nil
		delete(d.updates, r.ID)
		callbacks.Complete(result, cause)
		d.replyWhenSuspended(command, nil, nil)
	}
	return nil
}

// Admit one update, then let normal goroutines run before validating the next.
// A later validator must observe any state changes made by earlier handlers.
func (d *definition) admitUpdate() bool {
	if len(d.queuedUpdates) == 0 || d.updateWaiter == nil || d.queryWaiter == nil {
		return false
	}
	index := -1
	for i, state := range d.queuedUpdates {
		if _, ok := d.updateHandlers[state.name]; ok {
			index = i
			break
		}
	}
	if index < 0 {
		return false
	}
	state := d.queuedUpdates[index]
	d.queuedUpdates = append(d.queuedUpdates[:index], d.queuedUpdates[index+1:]...)
	response, err := d.runReadOnly(workflow.QueryRequest{Kind: "validate", Name: state.name, UpdateID: state.id, Payloads: state.payload, Header: state.header, Canceled: d.canceled, SkipValidator: d.env.IsReplaying()})
	if err != nil {
		d.failTask(err)
	}
	if response.Failed || len(response.Failure) != 0 {
		cause := error(errors.New(response.Error))
		if len(response.Failure) != 0 {
			var err error
			cause, err = decodeOutboundFailure(response.Failure, d.env.GetDataConverter())
			if err != nil {
				d.failTask(err)
			}
		}
		state.callbacks.Reject(cause)
		state.callbacks, state.payload = nil, nil
		delete(d.updates, state.id)
		return true
	}
	raw, err := json.Marshal(workflow.UpdateRequest{ID: state.id, Name: state.name, Payloads: state.payload, Header: state.header})
	if err != nil {
		d.failTask(err)
	}
	state.accepted = true
	state.callbacks.Accept()
	state.payload = nil
	d.updateWaiter.Reply(raw, nil)
	d.updateWaiter = nil
	return true
}

func (d *definition) rejectUnhandledUpdates() {
	keys := make([]string, 0, len(d.updateHandlers))
	for key := range d.updateHandlers {
		keys = append(keys, key)
	}
	slices.Sort(keys)
	for _, state := range d.queuedUpdates {
		state.callbacks.Reject(fmt.Errorf("unknown update %v. KnownUpdates=%v", state.name, keys))
		state.callbacks, state.payload = nil, nil
		delete(d.updates, state.id)
	}
	d.queuedUpdates = nil
}

func (d *definition) warnUnfinishedUpdates(cause error) {
	if cause != nil && !temporal.IsCanceledError(cause) && !workflow.IsContinueAsNewError(cause) {
		return
	}
	var unfinished []workflow.UpdateInfo
	for _, state := range d.updates {
		if state.accepted && d.updateHandlers[state.name].UnfinishedPolicy == workflow.HandlerUnfinishedPolicyWarnAndAbandon {
			unfinished = append(unfinished, workflow.UpdateInfo{ID: state.id, Name: state.name})
		}
	}
	if len(unfinished) != 0 {
		slices.SortFunc(unfinished, func(a, b workflow.UpdateInfo) int {
			if a.ID < b.ID {
				return -1
			}
			if a.ID > b.ID {
				return 1
			}
			return 0
		})
		if logger := d.env.GetLogger(); logger != nil {
			logger.Warn("Workflow finished with unfinished update handlers", "Updates", unfinished)
		}
	}
}
