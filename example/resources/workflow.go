// Package resources exercises worker resource policy using compiled workflows.
package resources

import (
	"context"
	"iter"
	"runtime"
	"time"
)

type Input struct {
	Mode       string
	Iterations int
}

//go:isolate
func ResourceWorkflow(ctx context.Context, input Input) (uint64, error) {
	switch input.Mode {
	case "goroutines":
		go func() { select {} }()
		go func() { select {} }()
		select {}
	case "memory":
		data := make([]byte, 128<<20)
		runtime.KeepAlive(data)
	case "small":
		data := make([]*uint64, 50000)
		for index := range data {
			data[index] = new(uint64)
		}
		runtime.KeepAlive(data)
	case "stack":
		recurse(10000)
	case "iterator":
		next, stop := iter.Pull(func(yield func(int) bool) { yield(1) })
		defer stop()
		next()
	case "yield":
		for {
			runtime.Gosched()
		}
	case "busy":
		var result uint64
		for index := 0; index < input.Iterations; index++ {
			result = result*1664525 + uint64(index)
		}
		return result, nil
	case "cached":
		select {
		case <-time.After(time.Hour):
		case <-ctx.Done():
			return 0, ctx.Err()
		}
	}
	return 7, nil
}

//go:noinline
func recurse(depth int) {
	var frame [1024]byte
	frame[depth%len(frame)] = byte(depth)
	if depth != 0 {
		recurse(depth - 1)
	}
	runtime.KeepAlive(frame)
}
