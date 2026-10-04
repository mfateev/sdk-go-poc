// Package workflow exposes host-mediated Temporal operations to a statically
// linked isolate program. Its byte-oriented API keeps the Temporal Go SDK on
// the host side of the isolate boundary.
package workflow

import (
	"encoding/json"
	"errors"
	"isolate"
	"time"
)

// Operation numbers are a wire contract. Never renumber an existing operation.
const (
	OpInput    uint32 = 1
	OpActivity uint32 = 2
	OpSleep    uint32 = 3
	OpSignal   uint32 = 4
	OpComplete uint32 = 5
)

// ActivityRequest is the wire representation of a host activity call.
type ActivityRequest struct {
	Name                string        `json:"name"`
	Input               []byte        `json:"input"`
	StartToCloseTimeout time.Duration `json:"start_to_close_timeout"`
}

// Signal is one incoming workflow signal.
type Signal struct {
	Name  string `json:"name"`
	Input []byte `json:"input"`
}

// SignalResult carries one signal or an error from its host call.
type SignalResult struct {
	Signal Signal
	Err    error
}

// ActivityResult carries the completed activity result or its error.
type ActivityResult struct {
	Result []byte
	Err    error
}

// Completion is the wire representation of a workflow result.
type Completion struct {
	Result []byte `json:"result"`
	Error  string `json:"error,omitempty"`
}

// Input returns the workflow's first byte-slice argument.
func Input() ([]byte, error) { return isolate.Call(OpInput, nil) }

// ExecuteActivity schedules a host activity and waits for its result.
func ExecuteActivity(name string, input []byte, timeout time.Duration) ([]byte, error) {
	if name == "" || timeout <= 0 {
		return nil, errors.New("workflow: activity name and timeout are required")
	}
	request, err := json.Marshal(ActivityRequest{Name: name, Input: input, StartToCloseTimeout: timeout})
	if err != nil {
		return nil, err
	}
	return isolate.Call(OpActivity, request)
}

// ExecuteActivityAsync starts an activity call in an isolate-owned goroutine.
// The returned channel receives one result and then closes. It is buffered so
// the call can finish if the workflow selects another event first.
func ExecuteActivityAsync(name string, input []byte, timeout time.Duration) <-chan ActivityResult {
	results := make(chan ActivityResult, 1)
	go func() {
		defer close(results)
		result, err := ExecuteActivity(name, input, timeout)
		if err != nil {
			result = nil
		}
		results <- ActivityResult{Result: result, Err: err}
	}()
	return results
}

// Sleep waits on a durable Temporal timer. Do not use time.Sleep for this POC.
func Sleep(duration time.Duration) error {
	if duration < 0 {
		return errors.New("workflow: negative sleep duration")
	}
	payload, err := json.Marshal(duration)
	if err != nil {
		return err
	}
	_, err = isolate.Call(OpSleep, payload)
	return err
}

// NextSignal waits for the next incoming signal, regardless of its name.
func NextSignal() (Signal, error) { return nextSignal("") }

func nextSignal(name string) (Signal, error) {
	var signal Signal
	payload, err := isolate.Call(OpSignal, []byte(name))
	if err != nil {
		return signal, err
	}
	err = json.Unmarshal(payload, &signal)
	return signal, err
}

// GetSignalChannel receives signals with the given name. An empty name receives
// all signals. Each call starts an isolate-owned goroutine that waits through
// Call; its channel is buffered so a completed call can finish if the workflow
// has selected another case. A host error is sent once, then the channel closes.
func GetSignalChannel(name string) <-chan SignalResult {
	results := make(chan SignalResult, 1)
	go func() {
		defer close(results)
		for {
			signal, err := nextSignal(name)
			results <- SignalResult{Signal: signal, Err: err}
			if err != nil {
				return
			}
		}
	}()
	return results
}

// Complete finishes the workflow. The program should return from main next.
func Complete(result []byte, cause error) error {
	completion := Completion{Result: result}
	if cause != nil {
		completion.Error = cause.Error()
	}
	payload, err := json.Marshal(completion)
	if err != nil {
		return err
	}
	_, err = isolate.Call(OpComplete, payload)
	return err
}
