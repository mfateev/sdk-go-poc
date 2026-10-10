// Package ddtracewire preserves Datadog propagation metadata in W3C trace state.
// It contains no tracer objects or backend state and is safe in either owner.
package ddtracewire

import (
	"go.opentelemetry.io/otel/trace"
	"slices"
	"strconv"
	"strings"
)

type Metadata struct {
	Priority int
	Origin   string
	Tags     map[string]string
}

func FromContext(sc trace.SpanContext) Metadata {
	m := Metadata{Tags: make(map[string]string)}
	if sc.IsSampled() {
		m.Priority = 1
	}
	for _, member := range strings.Split(sc.TraceState().Get("dd"), ";") {
		key, value, ok := strings.Cut(member, ":")
		if !ok {
			continue
		}
		switch {
		case key == "s":
			if p, err := strconv.Atoi(value); err == nil && p >= -1 && p <= 2 && (p > 0) == sc.IsSampled() {
				m.Priority = p
			}
		case key == "o":
			m.Origin = strings.ReplaceAll(value, "~", "=")
		case strings.HasPrefix(key, "t."):
			m.Tags["_dd.p."+strings.TrimPrefix(key, "t.")] = strings.ReplaceAll(value, "~", "=")
		}
	}
	return m
}

// TraceState translates valid legacy metadata into a bounded Datadog member.
// Tags are sorted so header bytes do not depend on Go's map traversal order.
func (m Metadata) TraceState(base trace.TraceState) trace.TraceState {
	value := "s:" + strconv.Itoa(m.Priority)
	if m.Origin != "" {
		value += ";o:" + clean(m.Origin)
	}
	keys := make([]string, 0, len(m.Tags))
	for key := range m.Tags {
		if strings.HasPrefix(key, "_dd.p.") {
			keys = append(keys, key)
		}
	}
	slices.Sort(keys)
	for _, key := range keys {
		member := ";t." + clean(strings.TrimPrefix(key, "_dd.p.")) + ":" + clean(m.Tags[key])
		if len(value)+len(member) <= 253 {
			value += member
		}
	}
	if next, err := base.Insert("dd", value); err == nil {
		return next
	}
	return base
}

func clean(value string) string {
	var out strings.Builder
	for _, c := range value {
		switch {
		case c == '=':
			out.WriteByte('~')
		case c < 0x20 || c > 0x7e || c == ',' || c == ';' || c == ':':
			out.WriteByte('_')
		default:
			out.WriteByte(byte(c))
		}
	}
	return out.String()
}
