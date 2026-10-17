// Package hostintegration contains SDK-owned adapters for shared Go libraries.
// The Go runtime sees only ordinary typed host operations and allocation scopes.
package hostintegration

import (
	"errors"
	"fmt"
	"isolate"
	"reflect"
	"strings"
	"sync"
)

// Operation describes a source adapter registered during process initialization.
// HostOnly is reserved for shared caches and registries; their results remain
// opaque handles. Other operations allocate new values in the caller's domain.
type Operation struct {
	Function func(Context, []any) []any
	Copy     func([]any) []any
	HostOnly bool
	Reject   bool
}

var registry = struct {
	sync.RWMutex
	operations      map[string]Operation
	generated       map[reflect.Type]bool
	defaultResolver any
}{
	operations: make(map[string]Operation), generated: make(map[reflect.Type]bool),
}

// Register installs an adapter before isolate execution. Replayed package
// initializers leave the process registry alone.
func Register(name string, operation Operation) {
	if isolate.IsActive() {
		return
	}
	registry.Lock()
	defer registry.Unlock()
	if _, exists := registry.operations[name]; exists {
		panic("hostintegration: duplicate operation " + name)
	}
	registry.operations[name] = operation
}

// RegisterGenerated identifies generated message types without runtime layout
// privileges. It runs only during host initialization.
func RegisterGenerated(value any) {
	if isolate.IsActive() {
		return
	}
	registry.Lock()
	registry.generated[reflect.TypeOf(value)] = true
	registry.Unlock()
}

func supportedGenerated(value any) bool {
	t := reflect.TypeOf(value)
	if t == nil {
		return false
	}
	for t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	if t.Kind() != reflect.Struct {
		return false
	}
	t = reflect.PointerTo(t)
	registry.RLock()
	ok := registry.generated[t]
	registry.RUnlock()
	if !ok || t.Elem().Kind() != reflect.Struct {
		return false
	}
	for i := 0; i < t.Elem().NumField(); i++ {
		f := t.Elem().Field(i)
		if f.PkgPath != "" && f.Name != "state" && f.Name != "sizeCache" && f.Name != "unknownFields" {
			return false
		}
	}
	return true
}

var generated = isolate.NewHostFunc(func(_ isolate.HostContext, value any) (bool, error) { return supportedGenerated(value), nil })

// IsGenerated reports whether value is a supported generated message.
func IsGenerated(value any) bool {
	if !isolate.IsActive() {
		return supportedGenerated(value)
	}
	result, err := generated(value)
	if err != nil {
		panic(err)
	}
	return result
}

// IsActive is used by generated wrappers; host implementation calls do not
// recursively cross the application boundary.
func IsActive() bool { return isolate.IsActive() }

// Host scopes shared cache construction even when called inside a private
// constructor. Its callback is an SDK/library implementation, not user code.
func Host[T any](build func() T) T {
	result, err := isolate.Allocate(isolate.HostAllocator(), build)
	if err != nil {
		panic(err)
	}
	return result
}

type request struct {
	Name      string
	Arguments []any
}

type response struct {
	Values          []any
	ReadOnlyFailure string
}

var invoke = isolate.NewHostFunc(func(ctx isolate.HostContext, r request) (result response, err error) {
	defer func() {
		if failure := recover(); failure != nil {
			message, ok := failure.(string)
			if isolate.IsReadOnly() && ok && (strings.HasPrefix(message, "isolate: read-only handler") || strings.HasPrefix(message, "isolate: forbidden operation in read-only handler")) {
				result, err = isolate.Allocate(ctx.Allocator(), func() response { return response{ReadOnlyFailure: strings.Clone(message)} })
				return
			}
			panic(failure)
		}
	}()

	registry.RLock()
	op, ok := registry.operations[r.Name]
	registry.RUnlock()
	if !ok {
		panic("hostintegration: unregistered operation " + r.Name)
	}
	if op.Reject {
		if isolate.IsReadOnly() {
			panic("isolate: forbidden operation in read-only handler: unsupported metadata operation " + r.Name)
		}
		panic("hostintegration: unsupported metadata operation " + r.Name)
	}
	fn := op.Function
	build := func() []any {
		var values []any
		if op.HostOnly {
			values = Host(func() []any { return fn(ctx, r.Arguments) })
		} else {
			values = fn(ctx, r.Arguments)
		}
		if op.Copy != nil {
			values = op.Copy(values)
		}
		result := make([]any, len(values))
		for i, value := range values {
			if value == nil {
				continue
			}
			v := reflect.ValueOf(value)
			switch {
			case op.HostOnly && v.Kind() == reflect.String:
				copied := reflect.New(v.Type()).Elem()
				copied.SetString(strings.Clone(v.String()))
				result[i] = copied.Interface()
			case op.HostOnly && v.Kind() == reflect.Slice:
				if v.IsNil() {
					result[i] = v.Interface()
				} else {
					copied := reflect.MakeSlice(v.Type(), v.Len(), v.Len())
					reflect.Copy(copied, v)
					result[i] = copied.Interface()
				}
			default:
				copied := reflect.New(v.Type()).Elem()
				copied.Set(v)
				result[i] = copied.Interface()

			}
		}
		return result
	}
	values, err := isolate.Allocate(ctx.Allocator(), build)
	return response{Values: values}, err
})

// Call is emitted by SDK source adapters. Its result container belongs to the
// original caller; opaque host handles grant no ordinary access to host memory.
func Call(name string, arguments ...any) []any {
	result, err := invoke(request{name, arguments})
	if err != nil {
		panic(fmt.Sprintf("hostintegration: %s: %v", name, err))
	}
	if result.ReadOnlyFailure != "" {
		panic(result.ReadOnlyFailure)
	}
	return result.Values
}

// Argument preserves typed nil and nil interface arguments across an adapter.
func Argument[T any](value any) T {
	if value == nil {
		var zero T
		return zero
	}
	return value.(T)
}

// CopyString copies descriptor scalar data into the current construction owner.
func CopyString(value string) string { return strings.Clone(value) }

// Context is the active host boundary, passed only to registered implementations.
type Context = isolate.HostContext

// Bind wraps an explicit function argument with synchronous application reentry.
// Libraries must not retain it beyond the operation.
func Bind[T any](ctx Context, value any) T {
	fn := Argument[T](value)
	reflected := reflect.ValueOf(fn)
	if !reflected.IsValid() || reflected.Kind() != reflect.Func || reflected.IsNil() {
		return fn
	}
	wrapped := Host(func() reflect.Value {
		return reflect.MakeFunc(reflected.Type(), func(args []reflect.Value) (result []reflect.Value) {
			copied, err := isolate.Allocate(ctx.Allocator(), func() []reflect.Value { return append([]reflect.Value(nil), args...) })
			if err != nil {
				panic(err)
			}
			ctx.Reenter(func() { result = reflected.Call(copied) })
			return result
		})
	})
	return wrapped.Interface().(T)
}

// CheckResolver keeps the supported resolver policy in the SDK adapter.
func CheckResolver(options any) {
	value := reflect.ValueOf(options)
	if value.Kind() != reflect.Struct {
		return
	}
	resolver := value.FieldByName("Resolver")
	if resolver.IsValid() && !resolver.IsNil() {
		registry.RLock()
		defaultResolver := registry.defaultResolver
		registry.RUnlock()
		candidate := resolver.Interface()
		if reflect.TypeOf(candidate).Comparable() && candidate == defaultResolver {
			return
		}
		panic("isolate: forbidden operation custom protobuf resolver")
	}
}

// CheckMessages rejects custom application methods before library code could
// call them under host globals. Explicit function callbacks use Bind/Reenter.
func CheckMessages(arguments ...any) {
	for _, argument := range arguments {
		t := reflect.TypeOf(argument)
		if t == nil {
			continue
		}
		method, ok := t.MethodByName("ProtoReflect")
		if !ok || method.Type.NumIn() != 1 || method.Type.NumOut() != 1 {
			continue
		}
		supported := supportedGenerated(argument)
		if !supported {
			panic("hostintegration: unsupported protobuf implementation " + t.String())
		}
	}
}

// CopyError delivers library diagnostics without exposing a host error object.
func CopyError(err error) error {
	if err == nil {
		return nil
	}
	return errors.New(err.Error())
}

// MetadataCopy transfers scalar metadata without relabeling the shared graph.
// It runs after Host has restored the enclosing construction allocator.
func MetadataCopy[T any](value T) T {
	reflected := reflect.ValueOf(value)
	switch reflected.Kind() {
	case reflect.String:
		copy := reflect.New(reflected.Type()).Elem()
		copy.SetString(strings.Clone(reflected.String()))
		return copy.Interface().(T)
	case reflect.Slice:
		if reflected.IsNil() {
			return value
		}
		copy := reflect.MakeSlice(reflected.Type(), reflected.Len(), reflected.Len())
		reflect.Copy(copy, reflected)
		return copy.Interface().(T)
	}
	return value
}

// RegisterDefaultResolver records the library's process registry at startup.
func RegisterDefaultResolver(value any) {
	if isolate.IsActive() {
		return
	}
	registry.Lock()
	registry.defaultResolver = value
	registry.Unlock()
}
