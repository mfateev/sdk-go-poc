// Package determinism exercises the supported language and library operations
// together. Its activity input and completion are retained as a replay fixture.
package determinism

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"iter"
	"math"
	"math/rand"
	randv2 "math/rand/v2"
	"reflect"
	"runtime"
	"slices"
	"sync"
	"time"

	"github.com/mfateev/sdk-go-poc/workflow"
)

//go:isolate
func DeterminismWorkflow(ctx context.Context, workers int) ([]string, error) {
	if workers < 1 || workers > 32 {
		return nil, fmt.Errorf("workers must be between 1 and 32")
	}
	trace := []string{"start:" + time.Now().Format(time.RFC3339Nano)}
	var records sync.Mutex
	record := func(s string) { records.Lock(); trace = append(trace, s); records.Unlock() }
	var wg sync.WaitGroup
	for worker := 0; worker < workers; worker++ {
		wg.Go(func() {
			for k, v := range map[int]string{2: "two", -1: "minus", 0: "zero"} {
				record(fmt.Sprintf("map:%d:%d:%s", worker, k, v))
			}
			var m sync.Map
			for _, k := range []string{"z", "a", "m"} {
				m.Store(k, k)
			}
			m.Range(func(k, v any) bool { record(fmt.Sprintf("sync-map:%d:%s:%s", worker, k, v)); return true })
			next, stop := iter.Pull2(func(yield func(int, uint64) bool) {
				for i := 0; i < 4; i++ {
					record(fmt.Sprintf("produce:%d:%d", worker, i))
					runtime.Gosched()
					if !yield(i, randv2.Uint64()) {
						return
					}
				}
			})
			defer stop()
			for {
				i, value, ok := next()
				if !ok {
					break
				}
				record(fmt.Sprintf("consume:%d:%d:%x:%x", worker, i, value, rand.Uint64()))
			}
			closed := make(chan struct{})
			close(closed)
			for i := 0; i < 8; i++ {
				selected := -1
				select {
				case <-closed:
					selected = 0
				case <-closed:
					selected = 1
				case <-closed:
					selected = 2
				}
				r, _, _ := reflect.Select([]reflect.SelectCase{
					{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(closed)},
					{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(closed)},
				})
				record(fmt.Sprintf("select:%d:%d:%d", worker, selected, r))
			}
		})
	}
	wg.Wait()
	// Include distribution tails and floating-point bit patterns in the
	// portability corpus, without retaining thousands of values in history.
	var numbers []byte
	for i := 0; i < 4096; i++ {
		numbers = binary.LittleEndian.AppendUint64(numbers, math.Float64bits(rand.NormFloat64()))
		numbers = binary.LittleEndian.AppendUint64(numbers, math.Float64bits(randv2.ExpFloat64()))
	}
	record(fmt.Sprintf("distributions:%x", sha256.Sum256(numbers)))
	child, cancel := context.WithCancel(ctx)
	notifications := make(chan int, 16)
	for i := 0; i < 16; i++ {
		ctx, cancelChild := context.WithCancel(child)
		defer cancelChild()
		context.AfterFunc(ctx, func() { notifications <- i })
	}
	cancel()
	for i := 0; i < 16; i++ {
		record(fmt.Sprintf("cancel:%d", <-notifications))
	}
	immediate := time.NewTimer(0)
	if cap(immediate.C) != 0 {
		return nil, fmt.Errorf("timer channel exposed its buffer")
	}
	<-immediate.C
	delayed := time.NewTimer(time.Second)
	select {
	case <-ctx.Done():
		delayed.Stop()
		return nil, ctx.Err()
	case <-delayed.C:
	}
	record("finish:" + time.Now().Format(time.RFC3339Nano))
	// Retain the trace in history so replay can check freshly computed
	// observations against the recorded activity result and final completion.
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{StartToCloseTimeout: time.Minute})
	var recorded []string
	err := workflow.ExecuteActivity(ctx, RecordTrace, trace).Get(ctx, &recorded)
	if err != nil {
		return nil, err
	}
	// Replay supplies the old activity result. Compare it to our newly
	// computed trace: the SDK's command checks need not compare activity input.
	if !slices.Equal(trace, recorded) {
		for i := 0; i < min(len(trace), len(recorded)); i++ {
			if trace[i] != recorded[i] {
				return nil, fmt.Errorf("determinism trace changed at %d: got %q, recorded %q", i, trace[i], recorded[i])
			}
		}
		return nil, fmt.Errorf("determinism trace length changed: got %d, recorded %d", len(trace), len(recorded))
	}
	return trace, nil
}

// RecordTrace is a host activity. It retains the exact workflow observations.
func RecordTrace(_ context.Context, trace []string) ([]string, error) { return trace, nil }
