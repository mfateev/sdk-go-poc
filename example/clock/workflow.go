package clock

import (
	"time"
)

// ClockWorkflow makes every clock read observable to history replay.
//
//go:isolate
func ClockWorkflow() ([]byte, error) {
	started := time.Now().UTC()
	timer := time.NewTimer(time.Second)
	fired := (<-timer.C).UTC()
	completed := time.Now().UTC()
	result := started.Format(time.RFC3339Nano) + "|" + fired.Format(time.RFC3339Nano) + "|" + completed.Format(time.RFC3339Nano)
	return []byte(result), nil
}
