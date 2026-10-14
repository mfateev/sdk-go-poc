package worker

import (
	"bytes"
	"fmt"
	"isolate"
	"sync"

	"github.com/mfateev/sdk-go-poc/temporalbridge"
	"go.temporal.io/sdk/converter"
)

// FailureConverterOptions constructs a native failure converter with the data
// converter selected for the operation. Factory must be a top-level //temporal:workflow
// function. Each conversion gets fresh private state, including read-only calls.
// Configure a compatible host converter through client.Options.FailureConverter
// for host activity serialization, retry decisions and client error decoding.
type FailureConverterOptions struct {
	Factory func([]byte, converter.DataConverter) (converter.FailureConverter, error)
	Config  []byte
}

// SetIsolateFailureConverter configures workers and replayers, including after
// registration. New executions snapshot the factory/configuration. Empty options
// restore default failure conversion for new executions.
func SetIsolateFailureConverter(w any, options FailureConverterOptions) error {
	var handle isolate.Handle
	if options.Factory != nil {
		var ok bool
		handle, ok = isolate.LookupFunction(options.Factory)
		if !ok {
			return fmt.Errorf("worker: failure converter factory must be a //temporal:workflow function")
		}
	} else if len(options.Config) != 0 {
		return fmt.Errorf("worker: failure converter configuration requires a factory")
	}
	var config *failureConverterConfiguration
	switch w := w.(type) {
	case *isolateWorker:
		config = &w.failures
	case *isolateReplayer:
		config = &w.failures
	default:
		return fmt.Errorf("worker: %T is not an isolate worker or replayer", w)
	}
	config.mu.Lock()
	config.options = temporalbridge.FailureConverterOptions{Factory: handle, Config: bytes.Clone(options.Config)}
	config.mu.Unlock()
	return nil
}

type failureConverterConfiguration struct {
	mu      sync.RWMutex
	options temporalbridge.FailureConverterOptions
}

func (c *failureConverterConfiguration) resolve() temporalbridge.FailureConverterOptions {
	c.mu.RLock()
	defer c.mu.RUnlock()
	result := c.options
	result.Config = bytes.Clone(result.Config)
	return result
}
