package temporalbridge

import (
	"encoding/json"
	"fmt"
	"github.com/mfateev/sdk-go-poc/workflow"
	"isolate"
)

func (d *definition) interceptSignal(command *isolate.Command) error {
	if d.interceptors.Factory.Name() == "" {
		return fmt.Errorf("signal interception not configured")
	}
	if len(command.Payload) != 0 {
		var response workflow.InterceptedSignals
		if err := json.Unmarshal(command.Payload, &response); err != nil {
			return err
		}
		if response.Error != "" {
			d.failTask(fmt.Errorf("signal interceptor: %s", response.Error))
		}
		d.signals = append(d.signals, response.Signals...)
		d.deliverProcessedSignals(true)
	}
	if d.signalHookWaiter != nil {
		return fmt.Errorf("duplicate signal interceptor service")
	}
	if len(d.incomingSignals) == 0 {
		d.signalHookWaiter = command
		d.completeSignalFlush()
		return nil
	}
	raw, err := json.Marshal(d.incomingSignals[0])
	if err != nil {
		return err
	}
	d.incomingSignals = d.incomingSignals[1:]
	d.replyWhenSuspended(command, raw, nil)
	return nil
}

// Hold the workflow at startup/completion until every admitted history signal
// has traversed its inbound hooks, including signals without channel receivers.
func (d *definition) flushSignals(command *isolate.Command) error {
	if d.interceptors.Factory.Name() == "" {
		return fmt.Errorf("signal interception not configured")
	}
	if d.signalFlushWaiter != nil {
		return fmt.Errorf("duplicate signal flush")
	}
	d.signalFlushWaiter = command
	d.completeSignalFlush()
	return nil
}
func (d *definition) completeSignalFlush() {
	if d.signalFlushWaiter != nil && d.signalHookWaiter != nil && len(d.incomingSignals) == 0 {
		d.replyWhenSuspended(d.signalFlushWaiter, nil, nil)
		d.signalFlushWaiter = nil
	}
}
