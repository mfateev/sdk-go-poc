package main

import (
	"time"

	"github.com/mfateev/sdk-go-poc/workflow"
)

// The result makes every clock read observable to history replay.
func main() {
	started := time.Now().UTC()
	timer := time.NewTimer(time.Second)
	fired := (<-timer.C).UTC()
	completed := time.Now().UTC()
	result := started.Format(time.RFC3339Nano) + "|" + fired.Format(time.RFC3339Nano) + "|" + completed.Format(time.RFC3339Nano)
	_ = workflow.Complete([]byte(result), nil)
}
