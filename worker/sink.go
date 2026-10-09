package worker

import (
	"fmt"
	"sync"

	"github.com/mfateev/sdk-go-poc/internal/sinkop"
	"github.com/mfateev/sdk-go-poc/temporalbridge"
)

type SinkEvent = temporalbridge.SinkEvent
type SinkHandler = temporalbridge.SinkHandler
type SinkOptions = temporalbridge.SinkOptions

// RegisterSink routes an application operation code to a host handler on an
// isolate worker or replayer. Codes must be in 0x00010000..0xfffeffff, matching
// workflow.NewSink. Zero, SDK Calls and runtime operations are reserved.
// Registration may precede or follow workflow registration; duplicate codes
// and nil handlers are rejected. At most one options value is accepted.
// Delivery is best effort and suppressed during replay unless EnableReplay is
// set. Handlers run on the SDK host thread; remote IO should use a bounded host
// exporter. Serialization belongs to the interceptor and its host handler.
func RegisterSink(w any, op uint32, handler SinkHandler, options ...SinkOptions) error {
	if !sinkop.Valid(op) {
		return fmt.Errorf("worker: sink operation %#x is outside 0x00010000..0xfffeffff", op)
	}
	if handler == nil {
		return fmt.Errorf("worker: sink handler is nil")
	}
	if len(options) > 1 {
		return fmt.Errorf("worker: at most one SinkOptions value is supported")
	}
	var opts SinkOptions
	if len(options) == 1 {
		opts = options[0]
	}
	var config *sinkConfiguration
	switch w := w.(type) {
	case *isolateWorker:
		config = &w.sinks
	case *isolateReplayer:
		config = &w.sinks
	default:
		return fmt.Errorf("worker: %T is not an isolate worker or replayer", w)
	}
	config.mu.Lock()
	defer config.mu.Unlock()
	if _, exists := config.entries[op]; exists {
		return fmt.Errorf("worker: sink operation %#x is already registered", op)
	}
	if config.entries == nil {
		config.entries = make(map[uint32]sinkRegistration)
	}
	config.entries[op] = sinkRegistration{handler: handler, options: opts}
	return nil
}

type sinkRegistration struct {
	handler SinkHandler
	options SinkOptions
}

type sinkConfiguration struct {
	mu      sync.RWMutex
	entries map[uint32]sinkRegistration
}

func (c *sinkConfiguration) resolve(op uint32) (SinkHandler, SinkOptions) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	entry := c.entries[op]
	return entry.handler, entry.options
}
