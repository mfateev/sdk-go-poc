package testref

import (
	"encoding/json"
	"reflect"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestReferenceSnapshotsAndAtomicOperations(t *testing.T) {
	registry := NewRegistry()
	defer registry.Close()
	initial := map[string][]int{"values": {1, 2}}
	ref, err := New(registry, initial)
	require.NoError(t, err)
	initial["values"][0] = 99
	value, err := ref.Load()
	require.NoError(t, err)
	require.Equal(t, 1, value["values"][0])
	value["values"][0] = 88
	copy, err := ref.Load()
	require.NoError(t, err)
	require.Equal(t, 1, copy["values"][0])
	previous, err := ref.Swap(map[string][]int{"values": {3}})
	require.NoError(t, err)
	require.Equal(t, []int{1, 2}, previous["values"])
	swapped, err := ref.CompareAndSwap(previous, initial)
	require.NoError(t, err)
	require.False(t, swapped)
	swapped, err = ref.CompareAndSwap(map[string][]int{"values": {3}}, map[string][]int{"values": {4}})
	require.NoError(t, err)
	require.True(t, swapped)
}

func TestReferenceConcurrentCompareAndSwap(t *testing.T) {
	registry := NewRegistry()
	defer registry.Close()
	ref, err := New(registry, 0)
	require.NoError(t, err)
	errors := make(chan error, 8)
	var workers sync.WaitGroup
	for range 8 {
		workers.Go(func() {
			for range 50 {
				for {
					value, err := ref.Load()
					if err != nil {
						errors <- err
						return
					}
					ok, err := ref.CompareAndSwap(value, value+1)
					if err != nil {
						errors <- err
						return
					}
					if ok {
						break
					}
				}
			}
		})
	}
	workers.Wait()
	close(errors)
	for err := range errors {
		require.NoError(t, err)
	}
	value, err := ref.Load()
	require.NoError(t, err)
	require.Equal(t, 400, value)
}

func TestReferenceNamespaceTypeValidationAndCleanup(t *testing.T) {
	first, second := NewRegistry(), NewRegistry()
	defer second.Close()
	ref, err := New(first, 7)
	require.NoError(t, err)
	raw, err := json.Marshal(ref)
	require.NoError(t, err)
	var wrong Ref[string]
	require.NoError(t, json.Unmarshal(raw, &wrong))
	_, err = wrong.Load()
	require.ErrorContains(t, err, "type mismatch")
	// Embedded and explicitly named fields are different Go types even when
	// the field names and element types match.
	type Value struct{ N int }
	embedded, err := New(first, struct{ Value }{Value{N: 7}})
	require.NoError(t, err)
	raw, err = json.Marshal(embedded)
	require.NoError(t, err)
	var named Ref[struct{ Value Value }]
	require.NoError(t, json.Unmarshal(raw, &named))
	_, err = named.Load()
	require.ErrorContains(t, err, "type mismatch")
	packet, err := json.Marshal(request{Handle: ref.handle, Type: typeKey(reflect.TypeFor[int]()), Action: "load"})
	require.NoError(t, err)
	_, err = second.Handle(packet, false)
	require.ErrorContains(t, err, "another test environment")
	first.Close()
	first.Close()
	_, err = ref.Load()
	require.ErrorContains(t, err, "closed")
	_, err = New(first, 8)
	require.ErrorContains(t, err, "closed")
}

func TestReferenceRejectsReadOnlyAndMalformedWrites(t *testing.T) {
	registry := NewRegistry()
	defer registry.Close()
	ref, err := New(registry, 7)
	require.NoError(t, err)
	packet := request{Handle: ref.handle, Type: typeKey(reflect.TypeFor[int]()), Action: "store", Value: json.RawMessage(`"bad"`)}
	raw, err := json.Marshal(packet)
	require.NoError(t, err)
	_, err = registry.Handle(raw, false)
	require.ErrorContains(t, err, "invalid value")
	packet.Value = json.RawMessage(`8`)
	raw, err = json.Marshal(packet)
	require.NoError(t, err)
	_, err = registry.Handle(raw, true)
	require.ErrorContains(t, err, "read-only")
	value, err := ref.Load()
	require.NoError(t, err)
	require.Equal(t, 7, value)
	var empty Ref[int]
	_, err = empty.Load()
	require.ErrorContains(t, err, "uninitialized")
}
