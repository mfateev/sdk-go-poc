// Package concurrent exercises native activity result channels and timers.
package concurrent

import (
	"runtime"
	"strings"
	"time"

	"github.com/mfateev/sdk-go-poc/workflow"
)

//go:isolate
func ConcurrentWorkflow() ([]byte, error) {
	first := workflow.ExecuteActivityAsync[[]byte]("echo", time.Minute, []byte("one"))
	second := workflow.ExecuteActivityAsync[[]byte]("echo", time.Minute, []byte("two"))
	timer := time.After(time.Second)
	var events []string
	for first != nil || second != nil || timer != nil {
		select {
		case result := <-first:
			if result.Err != nil {
				return nil, result.Err
			}
			events = append(events, string(result.Result))
			first = nil
		case result := <-second:
			if result.Err != nil {
				return nil, result.Err
			}
			events = append(events, string(result.Result))
			second = nil
		case <-timer:
			events = append(events, "timer")
			timer = nil
		}
	}
	return []byte(strings.Join(events, "|")), nil
}

//go:isolate
func DeadlockWorkflow() ([]byte, error) {
	var never chan struct{}
	<-never
	return nil, nil
}

//go:isolate
func YieldForeverWorkflow() ([]byte, error) {
	for {
		runtime.Gosched()
	}
}
