package worker

import (
	"fmt"
	"sync"

	"github.com/mfateev/sdk-go-poc/temporalbridge"
)

type LogEvent = temporalbridge.LogEvent
type LogHandler = temporalbridge.LogHandler

// SetIsolateLogHandler configures printing and standard logging for an isolate
// worker or replayer. It may be called before or after registration. A nil handler
// restores the SDK logger, including its EnableLoggingInReplay setting.
// A custom handler receives replay records too; inspect LogEvent.Replay to filter.
// Delivery uses best-effort one-way Write without acknowledgment. Handlers run
// on the SDK host thread; remote backends should enqueue to an exporter.
// Handler panics are diagnosed on the host and do not fail workflow tasks.
func SetIsolateLogHandler(w any, handler LogHandler) error {
	var config *logConfiguration
	switch w := w.(type) {
	case *isolateWorker:
		config = &w.logs
	case *isolateReplayer:
		config = &w.logs
	default:
		return fmt.Errorf("worker: %T is not an isolate worker or replayer", w)
	}
	config.mu.Lock()
	config.handler = handler
	config.mu.Unlock()
	return nil
}

type logConfiguration struct {
	mu      sync.RWMutex
	handler LogHandler
}

func (c *logConfiguration) resolve() LogHandler {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.handler
}
