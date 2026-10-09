package temporalbridge

import "isolate"

// LogEvent contains a copied workflow log record and host execution metadata.
// Replay is true when the worker is rebuilding state from history.
type LogEvent struct {
	isolate.LogRecord
	WorkflowID, RunID, WorkflowType string
	Replay                          bool
}

// LogHandler runs on the SDK host thread. Remote backends should enqueue to an
// exporter; sink errors must not become workflow inputs. Panics are reported as
// host diagnostics without failing a Workflow Task.
// Custom handlers choose whether to emit records whose Replay field is true.
type LogHandler func(LogEvent)

func (d *definition) writeLog(record isolate.LogRecord) {
	defer func() {
		if fault := recover(); fault != nil {
			d.logWarning("isolate log handler panicked", "panic", fault)
		}
	}()
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

// handleWrite consumes observations without producing a reply or making
// workflow goroutines runnable. SDK replay metadata is read on its host thread,
// just as it is for the ordinary SDK's replay-aware workflow logger.
func (d *definition) handleWrite(message *isolate.Message) {
	if message == nil || d.env == nil {
		return
	}
	if message.Op != isolate.LogOp {
		d.logWarning("unsupported isolate write operation", "operation", message.Op)
		return
	}
	record, err := isolate.DecodeLog(message.Payload)
	if err != nil {
		d.logWarning("invalid isolate log record", "error", err)
		return
	}
	d.writeLog(record)
}

func (d *definition) logWarning(message string, fields ...any) {
	// Diagnostic logging may itself be backed by a failing host sink. Neither
	// that failure nor a custom handler panic is a workflow input.
	defer func() { _ = recover() }()
	if d.env != nil {
		if logger := d.env.GetLogger(); logger != nil {
			logger.Warn(message, fields...)
		}
	}
}

// drainWrites runs at the task/query fence and after revocation, before the
// environment and configured handler are released. Writes do not require an
// extra dispatch pass, and a completed execution can still have final output.
func (d *definition) drainWrites(instance *isolate.Isolate) {
	for {
		select {
		case message := <-instance.Writes():
			d.handleWrite(message)
		default:
			return
		}
	}
}
