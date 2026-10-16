package main

import (
	"flag"
	"fmt"
	"isolate"
	"os"
	"runtime"
	"runtime/debug"
	"slices"
	"sync"
	"time"

	"github.com/mfateev/sdk-go-poc/example/determinism"
	"github.com/mfateev/sdk-go-poc/example/resources"
	"github.com/mfateev/sdk-go-poc/temporalbridge"
	"github.com/mfateev/sdk-go-poc/worker"
	"github.com/shirou/gopsutil/v4/process"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
)

type stressSnapshot struct {
	heap, objects, stacks, resident uint64
	goroutines, threads, fds        int
	cpu                             float64
}

func (s stressSnapshot) String() string {
	return fmt.Sprintf("heap=%.2fMiB objects=%d stacks=%.2fMiB rss=%.2fMiB goroutines=%d threads=%d fds=%d cpu=%.3fs", float64(s.heap)/(1<<20), s.objects, float64(s.stacks)/(1<<20), float64(s.resident)/(1<<20), s.goroutines, s.threads, s.fds, s.cpu)
}

func stressSample(p *process.Process) stressSnapshot {
	// Collect twice so finalizers and objects released by the first collection
	// are included. Return memory to the OS before comparing resident memory.
	runtime.GC()
	debug.FreeOSMemory()
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	memory, err := p.MemoryInfo()
	mustStress(err)
	threads, err := p.NumThreads()
	mustStress(err)
	cpu, err := p.Times()
	mustStress(err)
	fds, err := p.NumFDs()
	mustStress(err)
	return stressSnapshot{m.HeapAlloc, m.HeapObjects, m.StackInuse, memory.RSS, runtime.NumGoroutine(), int(threads), int(fds), cpu.User + cpu.System}
}

func mustStress(err error) {
	if err != nil {
		panic(err)
	}
}

func stressGrowth(base, current stressSnapshot) error {
	// Fixed allowances absorb runtime pools and scheduler growth; they do not
	// scale with completed workflows, so sustained retention eventually fails.
	for _, check := range []struct {
		name                     string
		before, after, allowance int64
	}{
		{"heap bytes", int64(base.heap), int64(current.heap), 16 << 20},
		{"heap objects", int64(base.objects), int64(current.objects), 20000},
		{"stack bytes", int64(base.stacks), int64(current.stacks), 4 << 20},
		{"resident bytes", int64(base.resident), int64(current.resident), 64 << 20},
		{"goroutines", int64(base.goroutines), int64(current.goroutines), 8},
		{"threads", int64(base.threads), int64(current.threads), 8},
		{"file descriptors", int64(base.fds), int64(current.fds), 4},
	} {
		if check.after-check.before > check.allowance {
			return fmt.Errorf("possible %s leak: baseline=%d current=%d growth=%d allowance=%d", check.name, check.before, check.after, check.after-check.before, check.allowance)
		}
	}
	return nil
}

func runStress(args []string) {
	flags := flag.NewFlagSet("isolate-stress", flag.ExitOnError)
	duration := flags.Duration("duration", 10*time.Minute, "wall-clock load duration, excluding warmup and final cleanup")
	historyFile := flags.String("history", "../../determinism/testdata/history-shared-random.json", "saved determinism history")
	mustStress(flags.Parse(args))
	if *duration <= 0 {
		panic("stress duration must be positive")
	}
	p, err := process.NewProcess(int32(os.Getpid()))
	mustStress(err)
	f, err := os.Open(*historyFile)
	mustStress(err)
	history, err := client.HistoryFromJSON(f, client.HistoryJSONOptions{})
	mustStress(err)
	mustStress(f.Close())
	var expectedTrace []string
	mustStress(converter.GetDefaultDataConverter().FromPayloads(history.Events[len(history.Events)-1].GetWorkflowExecutionCompletedEventAttributes().Result, &expectedTrace))
	replay := func() {
		r := worker.NewWorkflowReplayer()
		r.RegisterWorkflow(determinism.DeterminismWorkflow)
		r.RegisterActivity(determinism.RecordTrace)
		mustStress(r.ReplayWorkflowHistory(resourceLogger{}, history))
		getter, ok := r.(interface{ GetWorkflowResult(string, any) error })
		if !ok {
			panic("missing replay result getter")
		}
		var actual []string
		mustStress(getter.GetWorkflowResult("", &actual))
		if !slices.Equal(actual, expectedTrace) {
			panic("saved-history determinism trace changed")
		}
	}
	var expected [4]uint64
	batch := func(warm bool) {
		var wg sync.WaitGroup
		for slot := range 4 {
			wg.Go(func() {
				for _, evict := range []bool{false, true} {
					t := &check{name: fmt.Sprintf("stress/slot-%d/evict-%v", slot, evict)}
					d, e := resourceDefinition(t, resources.Input{Mode: "stress", Iterations: 4 + slot}, temporalbridge.ResourceOptions{})
					// Keep cleanup active if a later assertion fails.
					func() {
						defer d.Close()
						if fault := runResourceTaskWithTimeout(d, 15*time.Second); fault != nil {
							t.Fatal(fault)
						}
						if e.completes != 0 || e.timer == nil {
							t.Fatal("workflow did not suspend at timer")
						}
						stats := d.(interface{ Resources() isolate.ResourceStats }).Resources()
						if stats.LiveGoroutines == 0 || stats.TotalAllocatedBytes < 4<<20 || stats.RunningNanoseconds == 0 {
							t.Fatalf("workload did not exercise resources: %+v", stats)
						}
						if !evict {
							e.timer(nil, nil)
							if fault := runResourceTaskWithTimeout(d, 15*time.Second); fault != nil {
								t.Fatal(fault)
							}
							if e.completes != 1 || e.err != nil {
								t.Fatalf("completion count=%d error=%v", e.completes, e.err)
							}
							var result uint64
							mustStress(e.GetDataConverter().FromPayloads(e.result, &result))
							if warm {
								expected[slot] = result
							} else if result != expected[slot] {
								t.Fatalf("determinism changed: got %x want %x", result, expected[slot])
							}
						}
					}()
					if err := d.(interface{ CloseError() error }).CloseError(); err != nil {
						t.Fatal(err)
					}
					stats := d.(interface{ Resources() isolate.ResourceStats }).Resources()
					if stats.LiveGoroutines != 0 || stats.StackBytes != 0 {
						t.Fatalf("close retained execution resources: %+v", stats)
					}
					// Retain and invoke stale callbacks after close: they must not revive work.
					e.timer(nil, nil)
					e.cancel()
					mustStress(e.signal("late", nil, nil))
					d.OnWorkflowTaskStarted(time.Second)
					d.Close()
					if e.cancels != 0 || e.completes != map[bool]int{true: 0, false: 1}[evict] {
						t.Fatal("closed workflow revived callbacks")
					}
				}
			})
		}
		wg.Wait()
		replay()
	}
	for i := range 3 {
		batch(i == 0)
	}
	baseline := stressSample(p)
	fmt.Printf("stress baseline: %s\n", baseline)
	start := time.Now()
	nextSample := start.Add(30 * time.Second)
	batches := 0
	for time.Since(start) < *duration {
		batch(false)
		batches++
		if time.Now().After(nextSample) {
			current := stressSample(p)
			fmt.Printf("stress elapsed=%s workflows=%d replays=%d: %s\n", time.Since(start).Round(time.Second), batches*8, batches, current)
			mustStress(stressGrowth(baseline, current))
			nextSample = time.Now().Add(30 * time.Second)
		}
	}
	final := stressSample(p)
	mustStress(stressGrowth(baseline, final))
	// No workflows remain runnable. A full busy goroutine would consume about
	// 250ms here; allow 100ms for background GC and process accounting noise.
	cpuBefore, err := p.Times()
	mustStress(err)
	time.Sleep(250 * time.Millisecond)
	cpuAfter, err := p.Times()
	mustStress(err)
	idleCPU := cpuAfter.User + cpuAfter.System - cpuBefore.User - cpuBefore.System
	if idleCPU > 0.1 {
		panic(fmt.Sprintf("possible CPU leak after cleanup: %.3fs CPU in 250ms idle", idleCPU))
	}
	fmt.Printf("stress passed: duration=%s workflows=%d replays=%d idleCPU=%.3fs\nbaseline: %s\nfinal:    %s\n", time.Since(start).Round(time.Millisecond), batches*8, batches, idleCPU, baseline, final)
}
