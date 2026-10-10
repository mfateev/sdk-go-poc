package temporalbridge

import (
	"encoding/json"
	"fmt"
	"isolate"
	"math"
	"time"

	"github.com/mfateev/sdk-go-poc/workflow"
)

func (d *definition) handleObservation(message *isolate.Message) {
	defer func() {
		if fault := recover(); fault != nil {
			d.logWarning("isolate observation handler panicked", "operation", message.Op, "panic", fault)
		}
	}()
	switch message.Op {
	case workflow.OpLogWrite:
		var record workflow.LoggerWrite
		if err := json.Unmarshal(message.Payload, &record); err != nil {
			d.logWarning("invalid isolate logger record", "error", err)
			return
		}
		switch record.Level {
		case "debug", "info", "warn", "error":
		default:
			d.logWarning("invalid isolate logger level", "level", record.Level)
			return
		}
		if d.resolveLogHandler != nil {
			if handler := d.resolveLogHandler(); handler != nil {
				info := d.env.WorkflowInfo()
				handler(LogEvent{LogRecord: isolate.LogRecord{Source: "workflow", Message: record.Message},
					Level: record.Level, Fields: record.Fields, WorkflowID: info.WorkflowExecution.ID,
					RunID: info.WorkflowExecution.RunID, WorkflowType: info.WorkflowType.Name, Replay: d.env.IsReplaying()})
				return
			}
		}
		fields := make([]any, len(record.Fields))
		for i, field := range record.Fields {
			fields[i] = field
		}
		logger := d.env.GetLogger()
		switch record.Level {
		case "debug":
			logger.Debug(record.Message, fields...)
		case "info":
			logger.Info(record.Message, fields...)
		case "warn":
			logger.Warn(record.Message, fields...)
		case "error":
			logger.Error(record.Message, fields...)
		}
	case workflow.OpMetricWrite:
		if d.env.IsReplaying() {
			return
		}
		var record workflow.MetricWrite
		if err := json.Unmarshal(message.Payload, &record); err != nil {
			d.logWarning("invalid isolate metric record", "error", err)
			return
		}
		handler := d.env.GetMetricsHandler().WithTags(record.Tags)
		switch record.Kind {
		case "counter":
			handler.Counter(record.Name).Inc(record.Int)
		case "gauge":
			handler.Gauge(record.Name).Update(math.Float64frombits(record.FloatBits))
		case "timer":
			handler.Timer(record.Name).Record(time.Duration(record.Int))
		default:
			d.logWarning("invalid isolate metric kind", "error", fmt.Errorf("unknown kind %q", record.Kind))
		}
	}
}
