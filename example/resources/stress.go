package resources

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"iter"
	"math/rand/v2"
	"runtime"
	"sync"
	"time"
)

// stressWorkflow combines allocation churn, CPU work, deterministic scheduling,
// random streams, map iteration, select, stack growth and iterator cleanup.
func stressWorkflow(ctx context.Context, rounds int) (uint64, error) {
	var wg sync.WaitGroup
	observations := make(chan uint64, 64)
	for worker := range 4 {
		wg.Go(func() {
			for round := range rounds {
				data := make([]byte, 256<<10)
				seed := rand.Uint64()
				for i := range data {
					data[i] = byte(seed + uint64(i)*uint64(worker+1))
				}
				digest := sha256.Sum256(data)
				value := binary.LittleEndian.Uint64(digest[:])
				for key, item := range map[int]uint64{3: seed, 1: value, 2: uint64(round)} {
					value = value*33 + uint64(key) + item
				}
				next, stop := iter.Pull(func(yield func(uint64) bool) {
					for i := range 4 {
						runtime.Gosched()
						if !yield(value + uint64(i)) {
							return
						}
					}
				})
				for {
					v, ok := next()
					if !ok {
						break
					}
					observations <- v
				}
				stop()
				recurse(16)
				runtime.KeepAlive(data)
			}
		})
	}
	go func() { wg.Wait(); close(observations) }()
	var result uint64
	ready := make(chan struct{})
	close(ready)
	for value := range observations {
		select {
		case <-ready:
			result = result*33 + value
		case <-ready:
			result = result*37 + value
		}
	}
	// Every run suspends with live state. The host either delivers the timer or
	// evicts the instance, including a blocked child and an unconsumed long timer.
	go func() { <-ctx.Done() }()
	longTimer := time.NewTimer(time.Hour)
	defer longTimer.Stop()
	<-time.After(time.Second)
	return result, nil
}
