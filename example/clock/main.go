package main

import (
	"time"

	"github.com/mfateev/sdk-go-poc/workflow"
)

func init() { workflow.Register("IsolateClock", ClockWorkflow) }

func main() {
	if err := workflow.Run(); err != nil {
		panic(err)
	}
}

// ClockWorkflow makes every clock read observable to history replay.
func ClockWorkflow(_ []byte) ([]byte, error) {
	started := time.Now().UTC()
	timer := time.NewTimer(time.Second)
	fired := (<-timer.C).UTC()
	completed := time.Now().UTC()
	result := started.Format(time.RFC3339Nano) + "|" + fired.Format(time.RFC3339Nano) + "|" + completed.Format(time.RFC3339Nano)
	return []byte(result), nil
}
