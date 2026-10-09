package worker

import (
	"bytes"
	"fmt"
	"isolate"
	"sync"

	"github.com/mfateev/sdk-go-poc/temporalbridge"
	"go.temporal.io/sdk/converter"
)

// DataConverterOptions configures deterministic value serialization inside an
// isolate. Factory must be a top-level //go:isolate function. It receives its
// own copy of Config and must construct a new converter without external I/O.
// Use client.Options.DataConverter for host codecs, encryption and remote I/O;
// its underlying value serializer must use the same wire format as Factory.
// A nil Factory and empty Config restore the default Temporal serializer.
type DataConverterOptions struct {
	Factory func([]byte) (converter.DataConverter, error)
	Config  []byte
}

// SetIsolateDataConverter configures an isolate worker or replayer. New
// executions snapshot the setting, even when registration precedes this call.
// Cached executions keep their converter. Ordinary workflows are unaffected.
func SetIsolateDataConverter(w any, options DataConverterOptions) error {
	var handle isolate.Handle
	if options.Factory != nil {
		var ok bool
		handle, ok = isolate.LookupFunction(options.Factory)
		if !ok {
			return fmt.Errorf("worker: data converter factory must be a //go:isolate function")
		}
	} else if len(options.Config) != 0 {
		return fmt.Errorf("worker: data converter configuration requires a factory")
	}
	var config *converterConfiguration
	switch w := w.(type) {
	case *isolateWorker:
		config = &w.converters
	case *isolateReplayer:
		config = &w.converters
	default:
		return fmt.Errorf("worker: %T is not an isolate worker or replayer", w)
	}
	config.mu.Lock()
	config.options = temporalbridge.DataConverterOptions{Factory: handle, Config: bytes.Clone(options.Config)}
	config.mu.Unlock()
	return nil
}

type converterConfiguration struct {
	mu      sync.RWMutex
	options temporalbridge.DataConverterOptions
}

func (c *converterConfiguration) resolve() temporalbridge.DataConverterOptions {
	c.mu.RLock()
	defer c.mu.RUnlock()
	result := c.options
	result.Config = bytes.Clone(result.Config)
	return result
}

func converterFactory(fn any, config *converterConfiguration) any {
	if factory, ok := fn.(temporalbridge.Factory); ok {
		factory.ResolveDataConverter = config.resolve
		return factory
	}
	return fn
}
