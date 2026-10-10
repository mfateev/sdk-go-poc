package worker

import (
	"bytes"
	"fmt"
	"isolate"
	"sync"

	"github.com/mfateev/sdk-go-poc/interceptor"
	"github.com/mfateev/sdk-go-poc/temporalbridge"
)

// InterceptorOptions constructs a fresh SDK-style workflow interceptor chain
// inside each isolate. Factory must be marked //go:isolate and create all its
// state locally, using Config for settings and byte sinks for external output.
// The first returned interceptor is the outermost. Host WorkerOptions.Interceptors
// continue to apply to ordinary workflows and activities.
type InterceptorOptions struct {
	Factory func([]byte) ([]interceptor.WorkerInterceptor, error)
	Config  []byte
}

// SetIsolateInterceptors configures a worker or replayer. New executions snapshot
// the factory and copied configuration; cached executions retain their chain.
// Queries and validators construct a fresh scratch-owned chain from this same
// snapshot. A nil factory and empty configuration disable workflow interception.
func SetIsolateInterceptors(w any, options InterceptorOptions) error {
	var handle isolate.Handle
	if options.Factory != nil {
		var ok bool
		handle, ok = isolate.LookupFunction(options.Factory)
		if !ok {
			return fmt.Errorf("worker: interceptor factory must be a //go:isolate function")
		}
	} else if len(options.Config) != 0 {
		return fmt.Errorf("worker: interceptor configuration requires a factory")
	}
	var config *interceptorConfiguration
	switch w := w.(type) {
	case *isolateWorker:
		config = &w.interceptors
	case *isolateReplayer:
		config = &w.interceptors
	default:
		return fmt.Errorf("worker: %T is not an isolate worker or replayer", w)
	}
	config.mu.Lock()
	config.options = temporalbridge.InterceptorOptions{Factory: handle, Config: bytes.Clone(options.Config)}
	config.mu.Unlock()
	return nil
}

type interceptorConfiguration struct {
	mu      sync.RWMutex
	options temporalbridge.InterceptorOptions
}

func (c *interceptorConfiguration) resolve() temporalbridge.InterceptorOptions {
	c.mu.RLock()
	defer c.mu.RUnlock()
	out := c.options
	out.Config = bytes.Clone(out.Config)
	return out
}
