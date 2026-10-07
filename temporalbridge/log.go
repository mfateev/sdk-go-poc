package temporalbridge

import "isolate"

// LogEvent contains a copied workflow log record and host execution metadata.
// Replay is true when the worker is rebuilding state from history.
type LogEvent struct {
	isolate.LogRecord
	WorkflowID, RunID, WorkflowType string
	Replay                          bool
}

// LogHandler runs on the host. Sink errors must not become workflow inputs.
// Custom handlers choose whether to emit records whose Replay field is true.
type LogHandler func(LogEvent)

func (d *definition) writeLog(record isolate.LogRecord) {
	var handler LogHandler
	if d.resolveLogHandler != nil {
		handler = d.resolveLogHandler()
	}
	if handler == nil {
		// The SDK logger honors EnableLoggingInReplay and adds workflow metadata.
		d.env.GetLogger().Info(record.Message, "isolateLogSource", record.Source)
		return
	}
	info := d.env.WorkflowInfo()
	handler(LogEvent{LogRecord: record, WorkflowID: info.WorkflowExecution.ID,
		RunID: info.WorkflowExecution.RunID, WorkflowType: info.WorkflowType.Name,
		Replay: d.env.IsReplaying()})
}
