package temporalbridge

import "isolate"

// SinkEvent contains host-owned bytes and the execution metadata at delivery.
// Payload may be retained by the handler without retaining the private heap.
type SinkEvent struct {
	Operation                       uint32
	Name                            string
	Payload                         []byte
	WorkflowID, RunID, WorkflowType string
	Replay                          bool
}

// SinkHandler runs on the SDK host thread. Remote backends should enqueue to
// their own bounded exporter. Panics produce host diagnostics, not workflow
// failures; there is no response or error path back into the isolate.
type SinkHandler func(SinkEvent)

type SinkOptions struct {
	// Name is an optional diagnostic label, not a routing key.
	Name string
	// EnableReplay allows observations emitted while rebuilding workflow state.
	// The default suppresses them to avoid duplicate external effects.
	EnableReplay bool
}

func (d *definition) writeSink(message *isolate.Message) {
	defer func() {
		if fault := recover(); fault != nil {
			d.logWarning("isolate sink handler panicked", "operation", message.Op, "panic", fault)
		}
	}()
	var handler SinkHandler
	var options SinkOptions
	if d.resolveSink != nil {
		handler, options = d.resolveSink(message.Op)
	}
	if handler == nil {
		d.logWarning("unregistered isolate sink", "operation", message.Op)
		return
	}
	replay := d.env.IsReplaying()
	if replay && !options.EnableReplay {
		return
	}
	info := d.env.WorkflowInfo()
	handler(SinkEvent{Operation: message.Op, Name: options.Name, Payload: message.Payload,
		WorkflowID: info.WorkflowExecution.ID, RunID: info.WorkflowExecution.RunID,
		WorkflowType: info.WorkflowType.Name, Replay: replay})
}
