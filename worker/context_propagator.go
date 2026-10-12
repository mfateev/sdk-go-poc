package worker

import (
	"bytes"
	"fmt"
	"isolate"
	"sync"

	"github.com/mfateev/sdk-go-poc/temporalbridge"
	"github.com/mfateev/sdk-go-poc/workflow"
)

// ContextPropagatorOptions creates an ordered set of native propagators inside
// each workflow isolate. Factory must be marked //go:isolate, construct its own
// state, and use only the copied Config. Host contexts never cross the boundary.
// Configure matching host SDK propagators on client.Options.ContextPropagators
// for clients and activities. Queries/validators construct fresh scratch instances.
type ContextPropagatorOptions struct {
	Factory func([]byte) ([]workflow.ContextPropagator, error)
	Config  []byte
}

// SetIsolateContextPropagators configures a worker or replayer. New executions
// snapshot the setting; cached executions retain their propagators. A nil
// Factory and empty Config disable native propagation.
func SetIsolateContextPropagators(w any, options ContextPropagatorOptions) error {
	var handle isolate.Handle
	if options.Factory != nil {
		var ok bool
		handle, ok = isolate.LookupFunction(options.Factory)
		if !ok {
			return fmt.Errorf("worker: context propagator factory must be a //go:isolate function")
		}
	} else if len(options.Config) != 0 {
		return fmt.Errorf("worker: context propagator configuration requires a factory")
	}
	var config *propagatorConfiguration
	switch w := w.(type) {
	case *isolateWorker:
		config = &w.propagators
	case *isolateReplayer:
		config = &w.propagators
	default:
		return fmt.Errorf("worker: %T is not an isolate worker or replayer", w)
	}
	config.mu.Lock()
	config.options = temporalbridge.ContextPropagatorOptions{Factory: handle, Config: bytes.Clone(options.Config)}
	config.mu.Unlock()
	return nil
}

type propagatorConfiguration struct {
	mu      sync.RWMutex
	options temporalbridge.ContextPropagatorOptions
}

func (c *propagatorConfiguration) resolve() temporalbridge.ContextPropagatorOptions {
	c.mu.RLock()
	defer c.mu.RUnlock()
	out := c.options
	out.Config = bytes.Clone(out.Config)
	return out
}
