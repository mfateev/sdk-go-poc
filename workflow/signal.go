package workflow

import (
	"context"
	"errors"
	"fmt"
	"isolate"
	"reflect"
	"sync"
	"time"
)

// Match the SDK signal-channel limit; a full buffer fails the workflow task.
const signalChannelCapacity = 100000

var typedSignalChannels = make(map[string]any)
var typedSignalChannelsMu sync.Mutex

// GetSignalChannel returns the workflow-owned channel of decoded signal values.
// Admitted signals are buffered before application goroutines resume, including
// signals queued before registration. A nonblocking select can drain that batch.
// Each name has one type and channel for the workflow's lifetime. Registering
// the same name with another type panics. Workflow cancellation closes the channel
// unless its first registration uses a context without cancellation;
// other host errors and invalid payloads panic rather than delivering a zero value.
func GetSignalChannel[T any](ctx context.Context, name string) <-chan T {
	return registerSignalChannel[T](ctx, name, SignalChannelOptions{}, false)
}

// GetSignalChannelWithOptions registers signal metadata and returns decoded values.
// Repeated calls reuse the same channel and retain the first description.
func GetSignalChannelWithOptions[T any](ctx context.Context, name string, options SignalChannelOptions) <-chan T {
	return registerSignalChannel[T](ctx, name, options, true)
}

var signalRegistrationLocks = make(map[string]*sync.Mutex)

func registerSignalChannel[T any](ctx context.Context, name string, options SignalChannelOptions, withOptions bool) <-chan T {
	assertWritable()
	if ctx == nil {
		panic("workflow: nil context")
	}
	// Serialize registration for each name across the metadata handshake. In
	// particular, repeated calls must not start unused interceptor adapters that
	// could consume signals from the shared raw channel.
	typedSignalChannelsMu.Lock()
	registration := signalRegistrationLocks[name]
	if registration == nil {
		registration = new(sync.Mutex)
		signalRegistrationLocks[name] = registration
	}
	typedSignalChannelsMu.Unlock()
	registration.Lock()
	defer registration.Unlock()
	typedSignalChannelsMu.Lock()
	previous, exists := typedSignalChannels[name]
	typedSignalChannelsMu.Unlock()
	if exists {
		return signalChannelType[T](name, previous)
	}
	ctx = signalChannelContext(ctx)
	var raw <-chan SignalResult
	if out := currentOutbound(ctx); out != nil {
		isolate.RunInbound(func() {
			if withOptions {
				raw = out.GetSignalChannelWithOptions(ctx, name, options)
			} else {
				raw = out.GetSignalChannel(ctx, name)
			}
		})
	} else {
		raw = getSignalChannelWithOptions(ctx, name, options)
	}
	ch := typedSignalChannel[T](ctx, name, raw)
	// Late registration waits for the complete admitted batch to reach its native
	// buffer before any application continuation can observe channel readiness.
	if _, err := isolate.Call(OpFlushSignalChannels, nil); err != nil {
		panic(err)
	}
	return ch
}

func signalChannelType[T any](name string, previous any) <-chan T {
	ch, ok := previous.(<-chan T)
	if !ok {
		panic(fmt.Sprintf("workflow: signal %q already registered with channel type %T; requested %v", name, previous, reflect.TypeFor[T]()))
	}
	return ch
}

func typedSignalChannel[T any](ctx context.Context, name string, raw <-chan SignalResult) <-chan T {
	typedSignalChannelsMu.Lock()
	defer typedSignalChannelsMu.Unlock()
	if previous, ok := typedSignalChannels[name]; ok {
		return signalChannelType[T](name, previous)
	}
	ctx = signalChannelContext(ctx)
	ch := make(chan T, signalChannelCapacity)
	typedSignalChannels[name] = (<-chan T)(ch)
	isolate.GoInbound(func() {
		defer close(ch)
		for {
			var result SignalResult
			select {
			case <-ctx.Done():
				return
			case next, ok := <-raw:
				if !ok {
					return
				}
				result = next
			}
			if result.Err != nil {
				if errors.Is(result.Err, context.Canceled) || errors.Is(result.Err, context.DeadlineExceeded) {
					return
				}
				panic(fmt.Sprintf("workflow: receive signal %q: %v", name, result.Err))
			}
			value, err := decodeSignal[T](result.Signal)
			if err != nil {
				panic(fmt.Sprintf("workflow: decode signal %q as %v: %v", name, reflect.TypeFor[T](), err))
			}
			select {
			case ch <- value:
			case <-ctx.Done():
				return
			default:
				panic(fmt.Sprintf("workflow: exceeded channel buffer size for signal: %s", name))
			}
		}
	})
	return ch
}

// Preserve caller values for outbound hooks while binding channel cancellation
// to the root workflow. A child activity context must not close shared channels.
type signalLifetimeContext struct {
	context.Context
	lifetime context.Context
}

func (ctx signalLifetimeContext) Deadline() (time.Time, bool) { return ctx.lifetime.Deadline() }
func (ctx signalLifetimeContext) Done() <-chan struct{}       { return ctx.lifetime.Done() }
func (ctx signalLifetimeContext) Err() error                  { return ctx.lifetime.Err() }

func signalChannelContext(ctx context.Context) context.Context {
	if rootContext == nil {
		return ctx
	}
	if _, ok := ctx.(signalLifetimeContext); ok {
		return ctx
	}
	lifetime := rootContext
	if ctx.Done() == nil {
		lifetime = context.WithoutCancel(rootContext)
	}
	return signalLifetimeContext{Context: ctx, lifetime: lifetime}
}

func decodeSignal[T any](signal Signal) (T, error) {
	var value T
	if isProtoValue(value) || isProtoValue(&value) {
		return value, errors.New("workflow: protobuf values are outside the isolate POC subset")
	}
	if len(signal.Payloads) == 0 {
		// Legacy byte-input hosts may omit the serialized envelope.
		if input, ok := any(signal.Input).(T); ok {
			return input, nil
		}
		return value, errors.New("signal has no serialized payload")
	}
	payloads, err := decodePayloads(signal.Payloads)
	if err == nil {
		err = checkArgumentCount(payloads, 1)
	}
	if err == nil {
		err = currentDataConverter().FromPayloads(payloads, &value)
	}
	if err != nil {
		var zero T
		return zero, err
	}
	return value, nil
}
