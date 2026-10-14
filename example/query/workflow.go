package query

import (
	"context"
	"encoding/json"
	"errors"
	"isolate"
	"math/rand"
	randv2 "math/rand/v2"
	"os"
	"reflect"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/mfateev/sdk-go-poc/workflow"
)

type State struct {
	Count  int
	Bytes  []byte
	Values map[string]int
	Atomic atomic.Int64
}

// StackWorkflow deliberately registers no custom query handlers.
//
//temporal:workflow
func StackWorkflow(ctx context.Context) error {
	done := make(chan struct{})
	finished := make(chan struct{}, 2)
	go stackWait(done, finished)
	go stackWait(done, finished)
	time.Sleep(time.Minute)
	close(done)
	<-finished
	<-finished
	return nil
}

//go:noinline
func stackWait(done <-chan struct{}, finished chan<- struct{}) {
	<-done
	finished <- struct{}{}
}

// DispatchCloseWorkflow ports internal/internal_coroutines_test.go::TestDispatchClose.
// Native goroutine headers replace coroutine names and fixed three-line traces.
//
//temporal:workflow
func DispatchCloseWorkflow(ctx context.Context) ([]string, error) {
	var history []string
	if err := workflow.SetQueryHandler(ctx, "history", func() ([]string, error) {
		return append([]string(nil), history...), nil
	}); err != nil {
		return nil, err
	}
	blocked := make(chan struct{})
	for i := range 10 {
		go dispatchCloseChild(blocked, &history, i)
	}
	history = append(history, "root")
	<-blocked
	return history, nil
}

//go:noinline
func dispatchCloseChild(blocked <-chan struct{}, history *[]string, i int) {
	<-blocked
	*history = append(*history, "child-"+strconv.Itoa(i))
}

// StackDepthWorkflow exercises the native counterpart of the legacy stack
// depth-trimming helper with the original shallow/deep values (2 and 100).
//
//temporal:workflow
func StackDepthWorkflow(ctx context.Context, depth int) error {
	stackTraceDepth(depth)
	return nil
}

//go:noinline
func stackTraceDepth(depth int) {
	if depth == 0 {
		time.Sleep(time.Minute)
		return
	}
	stackTraceDepth(depth - 1)
}

var packageCount int

type callback struct{ state *State }

func (v callback) MarshalJSON() ([]byte, error) { v.state.Count = 999; return []byte(`"invalid"`), nil }

type callbackError struct{ state *State }

func (v callbackError) Error() string { v.state.Count = 999; return "invalid" }

//temporal:workflow
func QueryWorkflow(ctx context.Context, initial int) (int, error) {
	before := randv2.Uint64()
	randomState := []uint64{before}
	state := &State{Count: initial, Bytes: []byte{7}, Values: map[string]int{"count": initial}}
	state.Atomic.Store(int64(initial))
	packageCount = initial
	ready := workflow.ExecuteActivity(ctx, "") // A completed validation failure.
	if err := workflow.SetQueryHandler(ctx, "ready", func() (bool, error) { return ready.IsReady(), nil }); err != nil {
		return 0, err
	}
	if err := workflow.SetQueryHandlerWithOptions(ctx, "state", func(prefix string) (map[string]any, error) {
		return map[string]any{"prefix": prefix, "count": state.Count, "map": state.Values["count"], "byte": state.Bytes[0], "package": packageCount, "atomic": state.Atomic.Load(), "utc": time.Now().Location().String()}, nil
	}, workflow.QueryHandlerOptions{Description: "current state"}); err != nil {
		return 0, err
	}
	if err := workflow.SetQueryHandler(ctx, "state", func(prefix string) (map[string]any, error) {
		return map[string]any{"prefix": prefix, "count": state.Count, "map": state.Values["count"], "byte": state.Bytes[0], "package": packageCount, "atomic": state.Atomic.Load(), "utc": time.Now().Location().String()}, nil
	}); err != nil {
		return 0, err
	}
	if err := workflow.SetQueryHandler(ctx, "bad", func(mode string) (string, error) {
		return BadHandler(ctx, state, mode)
	}); err != nil {
		return 0, err
	}
	if err := workflow.SetQueryHandler(ctx, "random", func() ([]uint64, error) { return []uint64{randv2.Uint64(), uint64(rand.Int63())}, nil }); err != nil {
		return 0, err
	}
	if err := workflow.SetQueryHandler(ctx, "random-state", func() ([]uint64, error) {
		return append([]uint64(nil), randomState...), nil
	}); err != nil {
		return 0, err
	}
	timer := time.NewTimer(time.Minute)
	defer timer.Stop()
	signals := workflow.GetSignalChannel[[]byte](ctx, "finish")
	select {
	case <-timer.C:
	case <-signals:
	}
	after := randv2.Uint64()
	randomState = append(randomState, after)
	// Reading a query must not change ordinary workflow dispatch or memory.
	if state.Count != initial || state.Values["count"] != initial || state.Bytes[0] != 7 || packageCount != initial || state.Atomic.Load() != int64(initial) {
		return 0, errors.New("query mutated state")
	}
	state.Count++
	state.Values["count"]++
	state.Atomic.Add(1)
	packageCount++
	return state.Count, nil
}

// BadHandler exercises forbidden operations shared by query and validator probes.
func BadHandler(ctx context.Context, state *State, mode string) (string, error) {
	switch mode {
	case "exit":
		os.Exit(1)
	case "goexit":
		runtime.Goexit()
	case "environment":
		os.Getenv("HOME")
	case "mutex":
		var mu sync.Mutex
		mu.Lock()
		mu.Lock()
	case "waitgroup":
		var wg sync.WaitGroup
		wg.Add(1)
		wg.Wait()
	case "cond":
		cond := sync.NewCond(&sync.Mutex{})
		cond.L.Lock()
		cond.Wait()
	case "field":
		state.Count++
	case "map":
		state.Values["count"]++
	case "delete":
		delete(state.Values, "count")
	case "clear":
		clear(state.Values)
	case "slice":
		state.Bytes[0]++
	case "copy":
		copy(state.Bytes, []byte{9})
	case "package":
		packageCount++
	case "atomic":
		state.Atomic.Store(999)
	case "reflect":
		reflect.ValueOf(state).Elem().FieldByName("Count").SetInt(999)
	case "callback":
		_, err := json.Marshal(callback{state})
		return "", err
	case "error-callback":
		return "", callbackError{state}
	case "goroutine":
		go func() { state.Count++ }()
	case "channel":
		<-make(chan struct{})
	case "select":
		select {
		case <-make(chan struct{}):
		}
	case "activity":
		workflow.ExecuteActivity(ctx, "forbidden")
	case "child":
		workflow.ExecuteChildWorkflow(ctx, "forbidden")
	case "timer":
		time.Sleep(time.Second)
	case "raw":
		_, err := isolate.Call(workflow.OpSleep, []byte("1000000000"))
		return "", err
	case "panic":
		panic("bad query")
	case "error":
		return "", errors.New("")
	}
	return "unexpected success", nil
}
