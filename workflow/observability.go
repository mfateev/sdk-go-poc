package workflow

import (
	"context"
	"encoding/json"
	"fmt"
	"isolate"
	"maps"
	"math"
	"time"

	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/log"
)

const (
	OpLogWrite    uint32 = 0xffff0002
	OpMetricWrite uint32 = 0xffff0003
)

// LoggerWrite and MetricWrite are dedicated observation envelopes. They do not
// use DataConverter or host objects. Arbitrary log values format in the isolate.
type LoggerWrite struct {
	Level, Message string
	Fields         []string
}
type MetricWrite struct {
	Kind, Name string
	Tags       map[string]string
	Int        int64
	FloatBits  uint64
}

func GetLogger(ctx context.Context) log.Logger {
	if out := currentOutbound(ctx); out != nil {
		return out.GetLogger(ctx)
	}
	return getLogger(ctx)
}
func getLogger(context.Context) log.Logger { return isolateLogger{} }

type isolateLogger struct{}

func (isolateLogger) Debug(message string, fields ...any) { emitLog("debug", message, fields) }
func (isolateLogger) Info(message string, fields ...any)  { emitLog("info", message, fields) }
func (isolateLogger) Warn(message string, fields ...any)  { emitLog("warn", message, fields) }
func (isolateLogger) Error(message string, fields ...any) { emitLog("error", message, fields) }
func emitLog(level, message string, fields []any) {
	formatted := make([]string, len(fields))
	for i, field := range fields {
		formatted[i] = fmt.Sprint(field)
	}
	raw, err := json.Marshal(LoggerWrite{Level: level, Message: message, Fields: formatted})
	if err == nil {
		isolate.Write(OpLogWrite, raw)
	}
}

func GetMetricsHandler(ctx context.Context) client.MetricsHandler {
	if out := currentOutbound(ctx); out != nil {
		return out.GetMetricsHandler(ctx)
	}
	return getMetricsHandler(ctx)
}
func getMetricsHandler(context.Context) client.MetricsHandler { return isolateMetrics{} }

type isolateMetrics struct{ tags map[string]string }

func (m isolateMetrics) WithTags(tags map[string]string) client.MetricsHandler {
	combined := maps.Clone(m.tags)
	if combined == nil {
		combined = make(map[string]string, len(tags))
	}
	for key, value := range tags {
		combined[key] = value
	}
	return isolateMetrics{tags: combined}
}
func (m isolateMetrics) Counter(name string) client.MetricsCounter {
	return isolateMetric{name: name, tags: m.tags, kind: "counter"}
}
func (m isolateMetrics) Gauge(name string) client.MetricsGauge {
	return isolateMetric{name: name, tags: m.tags, kind: "gauge"}
}
func (m isolateMetrics) Timer(name string) client.MetricsTimer {
	return isolateMetric{name: name, tags: m.tags, kind: "timer"}
}

type isolateMetric struct {
	name, kind string
	tags       map[string]string
}

func (m isolateMetric) Inc(delta int64)               { m.emit(delta, 0) }
func (m isolateMetric) Update(value float64)          { m.emit(0, math.Float64bits(value)) }
func (m isolateMetric) Record(duration time.Duration) { m.emit(int64(duration), 0) }
func (m isolateMetric) emit(integer int64, floatBits uint64) {
	raw, err := json.Marshal(MetricWrite{Kind: m.kind, Name: m.name, Tags: m.tags, Int: integer, FloatBits: floatBits})
	if err == nil {
		isolate.Write(OpMetricWrite, raw)
	}
}
