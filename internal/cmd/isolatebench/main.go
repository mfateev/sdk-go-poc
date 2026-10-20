// isolatebench measures initialized/retained protobuf workflow state and verifies
// resumption after GC. Run built binaries serially, outside compilation/tests.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"isolate"
	"os"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/mfateev/sdk-go-poc/example/protobufvalues/model"
	"github.com/mfateev/sdk-go-poc/internal/integrationcontract"
	"github.com/mfateev/sdk-go-poc/workflow"
	"go.temporal.io/sdk/converter"
)

//temporal:workflow
func Work(ctx context.Context, value *model.Record) (*model.Record, error) {
	if value.Sequence != 42 {
		panic("invalid proto input")
	}
	_, err := isolate.Call(10001, nil)
	return value, err
}
func check(err error) {
	if err != nil {
		panic(err)
	}
}
func rss() uint64 {
	data, err := os.ReadFile("/proc/self/statm")
	check(err)
	n, err := strconv.ParseUint(strings.Fields(string(data))[1], 10, 64)
	check(err)
	return n * uint64(os.Getpagesize())
}
func memory() (uint64, uint64) {
	runtime.GC()
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return rss(), m.HeapAlloc
}
func main() {
	n := flag.Int("instances", 1000, "")
	started := flag.Bool("started", false, "")
	flag.Parse()
	if *n <= 0 {
		panic("instances must be positive")
	}
	runtime.GOMAXPROCS(8)
	h, ok := isolate.LookupFunction(Work)
	if !ok {
		panic("missing entry")
	}
	program := h.ProgramWithHandle(func(h isolate.Handle) { check(workflow.RunFunction(h)) })
	now := time.Unix(1700000000, 0)
	create := func() *isolate.Isolate {
		i, e := isolate.NewContext(context.Background(), isolate.Config{Program: program, Deterministic: true, InitialTime: &now, TimerOp: workflow.OpSleep})
		check(e)
		return i
	}
	for range 5 {
		i := create()
		check(i.Kill(context.Background()))
	}
	baseRSS, baseHeap := memory()
	instances := make([]*isolate.Isolate, 0, *n)
	pending := make([]*isolate.Command, 0, *n)
	times := make([]float64, 0, *n)
	for range *n {
		s := time.Now()
		i := create()
		times = append(times, float64(time.Since(s).Nanoseconds())/1e6)
		instances = append(instances, i)
		if *started {
			check(i.Start())
			check(i.Resume())
			c := <-i.Commands()
			if c.Op != workflow.OpStartPayloads {
				panic(c.Op)
			}
			payloads, e := converter.GetDefaultDataConverter().ToPayloads(&model.Record{Sequence: 42})
			check(e)
			start, e := workflow.MarshalTransport(workflow.PayloadStart{Payloads: payloads})
			check(e)
			c.Reply(integrationcontract.Pack(start), nil)
			check(i.Resume())
			for {
				c = <-i.Commands()
				if c.Op == 10001 {
					pending = append(pending, c)
					break
				}
				if c.Op != workflow.OpWorkflowCancel {
					panic(c.Op)
				}
			}

		}
	}
	endRSS, endHeap := memory()
	sort.Float64s(times)
	var total float64
	for _, v := range times {
		total += v
	}
	out := map[string]any{"instances": *n, "started": *started, "rss_KiB": float64(endRSS-baseRSS) / float64(*n) / 1024, "live_KiB": float64(endHeap-baseHeap) / float64(*n) / 1024, "mean_ms": total / float64(*n), "p50_ms": times[*n/2], "p95_ms": times[*n*95/100], "p99_ms": times[*n*99/100]}
	resumed := 0
	if *started {
		stride := max(1, *n/100)
		for index := 0; index < *n; index += stride {
			i := instances[index]
			pending[index].Reply(nil, nil)
			check(i.Resume())
			completion := <-i.Commands()
			for completion.Op == workflow.OpWorkflowCancel {
				completion = <-i.Commands()
			}
			if completion.Op != workflow.OpCompletePayloads {
				panic(completion.Op)
			}
			var result workflow.PayloadCompletion
			check(workflow.UnmarshalTransport(completion.Payload, &result))
			var record model.Record
			check(converter.GetDefaultDataConverter().FromPayloads(result.Payloads, &record))
			if record.Sequence != 42 {
				panic("protobuf state changed after GC")
			}
			completion.Reply(nil, nil)
			check(i.Resume())
			check(i.Wait())
			resumed++
		}
	}
	out["resumed_after_GC"] = resumed
	raw, _ := json.Marshal(out)
	fmt.Println(string(raw))
	runtime.KeepAlive(instances)
	for _, i := range instances {
		check(i.Kill(context.Background()))
	}
}
