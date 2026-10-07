package worker

import (
	"fmt"
	"github.com/mfateev/sdk-go-poc/temporalbridge"
	"sync"
)

type ResourceOptions = temporalbridge.ResourceOptions
type ResourceEvent = temporalbridge.ResourceEvent

// SetIsolateResourceOptions configures admission and active-task watchdogs on
// an isolate worker or replayer. Registration may precede configuration. Each
// new execution snapshots the options; existing cached executions retain their
// original policy. Ordinary workflows keep their Temporal SDK behavior.
func SetIsolateResourceOptions(w any, options ResourceOptions) error {
	if err := options.Validate(); err != nil {
		return err
	}
	var config *resourceConfiguration
	switch w := w.(type) {
	case *isolateWorker:
		config = &w.resources
	case *isolateReplayer:
		config = &w.resources
	default:
		return fmt.Errorf("worker: %T is not an isolate worker or replayer", w)
	}
	config.mu.Lock()
	config.options = options
	config.mu.Unlock()
	return nil
}

type resourceConfiguration struct {
	mu      sync.RWMutex
	options ResourceOptions
}

func (c *resourceConfiguration) resolve() ResourceOptions {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.options
}

func resourceFactory(fn any, config *resourceConfiguration) any {
	if factory, ok := fn.(temporalbridge.Factory); ok {
		factory.ResolveResourceOptions = config.resolve
		return factory
	}
	return fn
}
