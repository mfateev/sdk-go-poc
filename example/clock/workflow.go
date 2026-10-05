package clock

import (
	"context"
	"time"
)

// ClockWorkflow makes every clock read observable to history replay.
//
//go:isolate
func ClockWorkflow(ctx context.Context) ([]byte, error) {
	started := time.Now().UTC()
	timer := time.NewTimer(time.Second)
	defer timer.Stop()
	var fired time.Time
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case fired = <-timer.C:
		fired = fired.UTC()
	}
	completed := time.Now().UTC()
	result := started.Format(time.RFC3339Nano) + "|" + fired.Format(time.RFC3339Nano) + "|" + completed.Format(time.RFC3339Nano)
	return []byte(result), nil
}
