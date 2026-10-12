package workflow

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"isolate"
	"reflect"
	"sort"

	"github.com/mfateev/sdk-go-poc/internal/headerwire"
	commonpb "go.temporal.io/api/common/v1"
	goWorkflow "go.temporal.io/sdk/workflow"
)

type HeaderReader = goWorkflow.HeaderReader
type HeaderWriter = goWorkflow.HeaderWriter

// ContextPropagator copies selected native context values through Temporal
// headers. Native workflows use context.Context for both methods. Configure
// matching host SDK propagators on the client/worker for host activity contexts.
// Instances belong to one isolate. Methods must be deterministic and must not
// perform I/O. Extract must return a derived context without mutating its parent;
// queries and update validators call fresh instances under a scratch owner.
type ContextPropagator interface {
	Inject(context.Context, HeaderWriter) error
	Extract(context.Context, HeaderReader) (context.Context, error)
}

var instancePropagatorFactory isolate.Handle
var instancePropagatorConfig []byte
var instancePropagators []ContextPropagator

func configureContextPropagators(factory isolate.Handle, config []byte) error {
	instancePropagatorFactory = factory
	instancePropagatorConfig = bytes.Clone(config)
	var err error
	instancePropagators, err = makeContextPropagators()
	return err
}

func makeContextPropagators() ([]ContextPropagator, error) {
	if instancePropagatorFactory.Name() == "" {
		return nil, nil
	}
	var result []ContextPropagator
	err := instancePropagatorFactory.Invoke(func(args ...isolate.Value) error {
		if len(args) != 1 {
			return errors.New("workflow: invalid context propagator factory arguments")
		}
		slot, ok := args[0].Pointer.(*[]byte)
		if !ok {
			return errors.New("workflow: context propagator factory must accept []byte")
		}
		*slot = bytes.Clone(instancePropagatorConfig)
		return nil
	}, func(values ...isolate.Value) error {
		if len(values) != 1 {
			return errors.New("workflow: context propagator factory must return []workflow.ContextPropagator")
		}
		var ok bool
		result, ok = values[0].Value.([]ContextPropagator)
		if !ok {
			return errors.New("workflow: invalid context propagator factory result")
		}
		for i, p := range result {
			if p == nil || reflect.ValueOf(p).Kind() == reflect.Pointer && reflect.ValueOf(p).IsNil() {
				return fmt.Errorf("workflow: nil context propagator at index %d", i)
			}
		}
		return nil
	})
	return result, err
}

func currentContextPropagators() ([]ContextPropagator, error) {
	if isolate.IsReadOnly() {
		return makeContextPropagators()
	}
	return instancePropagators, nil
}

// A private header map keeps injection and extraction from changing a captured
// interceptor header. Iteration is ordered independently of map scheduling.
type propagationHeader map[string]*commonpb.Payload

func (h propagationHeader) Set(key string, p *commonpb.Payload)      { h[key] = p }
func (h propagationHeader) Get(key string) (*commonpb.Payload, bool) { p, ok := h[key]; return p, ok }
func (h propagationHeader) ForEachKey(fn func(string, *commonpb.Payload) error) error {
	keys := make([]string, 0, len(h))
	for key := range h {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		if err := fn(key, h[key]); err != nil {
			return err
		}
	}
	return nil
}

func withIncomingHeader(ctx context.Context, fields map[string][]byte) (context.Context, error) {
	ctx, err := withInterceptorHeader(ctx, fields)
	if err != nil {
		return nil, err
	}
	propagators, err := currentContextPropagators()
	if err != nil {
		return nil, err
	}
	header := propagationHeader(InterceptorHeader(ctx))
	for i, p := range propagators {
		ctx, err = p.Extract(ctx, header)
		if err != nil {
			return nil, fmt.Errorf("workflow: extract context propagator %d: %w", i, err)
		}
		if ctx == nil {
			return nil, fmt.Errorf("workflow: context propagator %d returned nil context", i)
		}
	}
	return ctx, nil
}

func propagatedHeader(ctx context.Context) (map[string][]byte, error) {
	fields, err := headerwire.Encode(&commonpb.Header{Fields: InterceptorHeader(ctx)})
	if err != nil {
		return nil, err
	}
	copied, err := headerwire.Decode(fields)
	if err != nil {
		return nil, err
	}
	if copied == nil {
		copied = &commonpb.Header{Fields: make(map[string]*commonpb.Payload)}
	}
	header := propagationHeader(copied.Fields)
	propagators, err := currentContextPropagators()
	if err != nil {
		return nil, err
	}
	for i, p := range propagators {
		if err := p.Inject(ctx, header); err != nil {
			return nil, fmt.Errorf("workflow: inject context propagator %d: %w", i, err)
		}
	}
	if len(header) == 0 {
		return nil, nil
	}
	return headerwire.Encode(copied)
}
