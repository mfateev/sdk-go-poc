package workflow

import (
	"isolate"

	"github.com/mfateev/sdk-go-poc/internal/sinkop"
)

// Sink emits copied observations to a worker-registered host handler. Its
// operation code must match worker.RegisterSink and remain stable across builds.
// A Sink holds no host state and can be used by concurrent workflow goroutines,
// queries and update validators.
type Sink struct{ op uint32 }

// NewSink constructs a sink with an application operation code in the inclusive
// range 0x00010000 through 0xfffeffff. Other codes are reserved and panic.
// Construction does not contact the host or check its registrations.
func NewSink(op uint32) Sink {
	if !sinkop.Valid(op) {
		panic("workflow: sink operation must be in 0x00010000..0xfffeffff")
	}
	return Sink{op: op}
}

// Emit snapshots payload using one-way isolate.Write without a receipt or
// acknowledgment. The interceptor owns serialization; Temporal's DataConverter
// and codecs are not used. Missing handlers, backend failures, full queues and
// payloads exceeding 64 KiB never produce workflow inputs. The runtime queue
// holds at most 64 messages, shared with printing and other observations.
// Delivery is suppressed during replay unless the host registration opts in.
// Emit requires an active isolate; it cannot run during package initialization.
func (s Sink) Emit(payload []byte) {
	if !sinkop.Valid(s.op) {
		panic("workflow: uninitialized sink")
	}
	isolate.Write(s.op, payload)
}
