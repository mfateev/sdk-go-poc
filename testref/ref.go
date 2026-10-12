// Package testref provides typed host-backed references for isolate tests.
// Handles contain only numeric identities. Loads return JSON snapshots; writes
// are explicit host operations, never shared Go memory. Reference reads are
// test inputs outside Temporal history and must not be used in production.
package testref

import (
	"encoding/json"
	"errors"
	"fmt"
	"isolate"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/mfateev/sdk-go-poc/internal/testrefwire"
)

type handle struct {
	Environment uint64 `json:"environment"`
	Object      uint64 `json:"object"`
}

// Ref is a copyable handle owned by one test environment. Children in that
// environment may use it. Closing the environment invalidates its handles.
type Ref[T any] struct{ handle handle }

func (r Ref[T]) MarshalJSON() ([]byte, error)     { return json.Marshal(r.handle) }
func (r *Ref[T]) UnmarshalJSON(data []byte) error { return json.Unmarshal(data, &r.handle) }

type request struct {
	Handle handle          `json:"handle"`
	Type   string          `json:"type"`
	Action string          `json:"action"`
	Value  json.RawMessage `json:"value,omitempty"`
	Old    json.RawMessage `json:"old,omitempty"`
}

type response struct {
	Value   json.RawMessage `json:"value"`
	Swapped bool            `json:"swapped"`
}

// Load returns an independent snapshot, including for pointer, map and slice T.
func (r Ref[T]) Load() (T, error) {
	out, err := r.call("load", nil, nil)
	var value T
	if err == nil {
		err = json.Unmarshal(out.Value, &value)
	}
	return value, err
}

func (r Ref[T]) Store(value T) error { _, err := r.call("store", value, nil); return err }

// Swap atomically stores value and returns the previous snapshot.
func (r Ref[T]) Swap(value T) (T, error) {
	out, err := r.call("swap", value, nil)
	var previous T
	if err == nil {
		err = json.Unmarshal(out.Value, &previous)
	}
	return previous, err
}

// CompareAndSwap compares canonical JSON snapshots, rather than pointer
// identity. Comparison and replacement execute atomically on the host.
func (r Ref[T]) CompareAndSwap(old, value T) (bool, error) {
	out, err := r.call("compare-and-swap", value, old)
	return out.Swapped, err
}

func (r Ref[T]) call(action string, value, old any) (response, error) {
	var out response
	if r.handle.Environment == 0 || r.handle.Object == 0 {
		return out, errors.New("testref: uninitialized reference")
	}
	if isolate.IsReadOnly() && action != "load" {
		return out, errors.New("testref: read-only handlers cannot change host references")
	}
	req := request{Handle: r.handle, Type: typeKey(reflect.TypeFor[T]()), Action: action}
	var err error
	if action != "load" {
		req.Value, err = json.Marshal(value)
		if err != nil {
			return out, err
		}
	}
	if action == "compare-and-swap" {
		req.Old, err = json.Marshal(old)
		if err != nil {
			return out, err
		}
	}
	raw, err := json.Marshal(req)
	if err != nil {
		return out, err
	}
	var result []byte
	if isolate.IsActive() {
		call := isolate.Call
		if isolate.IsReadOnly() {
			call = isolate.ReadOnlyCall
		}
		result, err = call(testrefwire.Operation, raw)
	} else {
		registry, ok := registries.Load(r.handle.Environment)
		if !ok {
			return out, errors.New("testref: environment is closed")
		}
		result, err = registry.(*Registry).Handle(raw, false)
	}
	if err == nil {
		err = json.Unmarshal(result, &out)
	}
	return out, err
}

var nextEnvironment atomic.Uint64
var registries sync.Map

type cell struct {
	typ       string
	value     json.RawMessage
	normalize func([]byte) (json.RawMessage, error)
}

// Registry is host-only storage. Each testing environment owns one registry.
type Registry struct {
	mu     sync.Mutex
	id     uint64
	next   uint64
	closed bool
	cells  map[uint64]cell
}

func NewRegistry() *Registry {
	if isolate.IsActive() {
		panic("testref: registries must be created on the host")
	}
	r := &Registry{id: nextEnvironment.Add(1), cells: make(map[uint64]cell)}
	if r.id == 0 {
		panic("testref: environment identity exhausted")
	}
	registries.Store(r.id, r)
	return r
}

func New[T any](registry *Registry, initial T) (Ref[T], error) {
	var ref Ref[T]
	if isolate.IsActive() {
		return ref, errors.New("testref: references must be created on the host")
	}
	if registry == nil {
		return ref, errors.New("testref: nil registry")
	}
	value, err := json.Marshal(initial)
	if err != nil {
		return ref, err
	}
	registry.mu.Lock()
	defer registry.mu.Unlock()
	if registry.closed {
		return ref, errors.New("testref: environment is closed")
	}
	registry.next++
	if registry.next == 0 {
		return ref, errors.New("testref: object identity exhausted")
	}
	ref.handle = handle{registry.id, registry.next}
	registry.cells[registry.next] = cell{typ: typeKey(reflect.TypeFor[T]()), value: value, normalize: func(raw []byte) (json.RawMessage, error) {
		var copy T
		if err := json.Unmarshal(raw, &copy); err != nil {
			return nil, err
		}
		return json.Marshal(copy)
	}}
	return ref, nil
}

func (r *Registry) Close() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.closed = true
	r.cells = nil
	registries.Delete(r.id)
}

// Handle validates the environment, object and Go type before any access.
// Raw isolate.Call cannot bypass the restriction on read-only writes.
func (r *Registry) Handle(data []byte, readOnly bool) ([]byte, error) {
	if isolate.IsActive() {
		return nil, errors.New("testref: registry access is a host operation")
	}
	var req request
	if err := json.Unmarshal(data, &req); err != nil {
		return nil, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return nil, errors.New("testref: environment is closed")
	}
	if req.Handle.Environment != r.id {
		return nil, errors.New("testref: reference belongs to another test environment")
	}
	c, ok := r.cells[req.Handle.Object]
	if !ok {
		return nil, errors.New("testref: unknown reference")
	}
	if req.Type != c.typ {
		return nil, errors.New("testref: reference type mismatch")
	}
	if readOnly && req.Action != "load" {
		return nil, errors.New("testref: read-only handlers cannot change host references")
	}
	out := response{Value: c.value}
	switch req.Action {
	case "load":
	case "store", "swap", "compare-and-swap":
		value, err := c.normalize(req.Value)
		if err != nil {
			return nil, fmt.Errorf("testref: invalid value: %w", err)
		}
		var old json.RawMessage
		if req.Action == "compare-and-swap" {
			old, err = c.normalize(req.Old)
			if err != nil {
				return nil, fmt.Errorf("testref: invalid comparison: %w", err)
			}
		}
		if req.Action != "compare-and-swap" || string(c.value) == string(old) {
			c.value = value
			r.cells[req.Handle.Object] = c
			out.Swapped = true
		}
	default:
		return nil, fmt.Errorf("testref: unknown operation %q", req.Action)
	}
	return json.Marshal(out)
}

// Qualify names and composite structure so serialized handles cannot confuse
// identically named types from different packages.
func typeKey(t reflect.Type) string {
	var out strings.Builder
	seen := make(map[reflect.Type]int)
	var write func(reflect.Type)
	write = func(t reflect.Type) {
		if id, ok := seen[t]; ok {
			out.WriteString("@" + strconv.Itoa(id))
			return
		}
		seen[t] = len(seen)
		out.WriteString(t.PkgPath() + "/" + t.Name() + ":" + t.Kind().String() + "{")
		switch t.Kind() {
		case reflect.Pointer, reflect.Slice:
			write(t.Elem())
		case reflect.Array:
			out.WriteString(strconv.Itoa(t.Len()))
			write(t.Elem())
		case reflect.Map:
			write(t.Key())
			write(t.Elem())
		case reflect.Struct:
			for i := 0; i < t.NumField(); i++ {
				f := t.Field(i)
				out.WriteString(f.PkgPath + "/" + f.Name + ":" + strconv.Quote(string(f.Tag)) + ":" + strconv.FormatBool(f.Anonymous) + ";")
				write(f.Type)
			}
		case reflect.Interface:
			for i := 0; i < t.NumMethod(); i++ {
				m := t.Method(i)
				out.WriteString(m.PkgPath + "/" + m.Name)
				write(m.Type)
			}
		case reflect.Func:
			out.WriteString(strconv.FormatBool(t.IsVariadic()))
			for i := 0; i < t.NumIn(); i++ {
				write(t.In(i))
			}
			out.WriteString("->")
			for i := 0; i < t.NumOut(); i++ {
				write(t.Out(i))
			}
		}
		out.WriteString("}")
	}
	write(t)
	return out.String()
}
