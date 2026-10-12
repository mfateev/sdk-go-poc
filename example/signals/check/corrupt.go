package main

import (
	"fmt"
	"strings"
	"time"

	"github.com/mfateev/sdk-go-poc/example/converter/custom"
	"github.com/mfateev/sdk-go-poc/example/signals"
	"github.com/mfateev/sdk-go-poc/worker"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
)

// Capture the actual host observations emitted by the compiled workflow.
type diagnostics struct {
	logs   []string
	fields [][]any
	count  int64
}

func (*diagnostics) Debug(string, ...any) {}
func (*diagnostics) Info(string, ...any)  {}
func (*diagnostics) Warn(string, ...any)  {}
func (d *diagnostics) Error(message string, fields ...any) {
	d.logs = append(d.logs, message)
	d.fields = append(d.fields, fields)
}
func (d *diagnostics) WithTags(map[string]string) client.MetricsHandler { return d }
func (d *diagnostics) Counter(name string) client.MetricsCounter {
	if name != "temporal_corrupted_signals" {
		panic("unexpected counter " + name)
	}
	return d
}
func (d *diagnostics) Inc(value int64)                { d.count += value }
func (*diagnostics) Gauge(string) client.MetricsGauge { panic("unexpected gauge") }
func (*diagnostics) Timer(string) client.MetricsTimer { panic("unexpected timer") }
func (d *diagnostics) check(want int, replay bool) {
	count := int64(want)
	if replay {
		count = 0
	}
	if len(d.logs) != want || d.count != count {
		panic(fmt.Sprintf("corruption diagnostics logs=%v counter=%d, want %d/%d", d.logs, d.count, want, count))
	}
	for i, message := range d.logs {
		if !strings.Contains(message, "Deserialization error. Corrupted signal received on channel ") || len(d.fields[i]) != 2 || d.fields[i][0] != "Error" || fmt.Sprint(d.fields[i][1]) == "" {
			panic(fmt.Sprintf("missing error details: %s %v", message, d.fields[i]))
		}
	}
}

func checkCorrupt(mode string, configured, intercepted, replay bool) {
	r := new(registration)
	w := worker.Wrap(r)
	w.RegisterWorkflow(signals.CorruptWorkflow)
	if intercepted {
		must(worker.SetIsolateInterceptors(w, worker.InterceptorOptions{Factory: signals.NewInterceptors}))
	}
	closed := strings.HasPrefix(mode, "closed-")
	if closed {
		must(worker.SetIsolateInterceptors(w, worker.InterceptorOptions{Factory: signals.NewClosedSignalInterceptors, Config: []byte(mode)}))
	}
	dc := converter.GetDefaultDataConverter()
	if configured {
		var err error
		dc, err = custom.New([]byte("corrupt-signals"))
		must(err)
		must(worker.SetIsolateDataConverter(w, worker.DataConverterOptions{Factory: custom.New, Config: []byte("corrupt-signals")}))
	}
	e := &environment{dc: dc, diagnostics: new(diagnostics), replay: replay}
	d := r.factory.NewWorkflowDefinition()
	defer d.Close()
	input, err := dc.ToPayloads(mode)
	must(err)
	d.Execute(e, nil, input)
	send := func(name string, value any) {
		payloads, err := dc.ToPayloads(value)
		must(err)
		must(e.signal(name, payloads, nil))
	}
	wantErrors := 1
	if !closed {
		if mode != "async" {
			d.OnWorkflowTaskStarted(5 * time.Second)
		}
		if e.completed {
			panic("corrupt workflow completed before receiving signals")
		}
		name := "message"
		if mode == "next" {
			name = "bytes"
		}
		send(name, "wrong")
		if mode == "receive" || mode == "select" || mode == "next" {
			// A corrupt-only task must suspend normally, without waking the reader.
			d.OnWorkflowTaskStarted(5 * time.Second)
			if e.completed {
				panic("corrupt signal completed workflow")
			}
			e.diagnostics.check(1, replay)
		}
		if mode == "async" {
			// A nonblocking reader sees no zero values from any corrupt encoding.
			bad := &commonpb.Payloads{Payloads: []*commonpb.Payload{{Metadata: map[string][]byte{"encoding": []byte("unknown")}, Data: []byte("bad")}}}
			must(e.signal(name, bad, nil))
			wantErrors++
			bad.Payloads[0].Metadata["encoding"] = []byte("json/plain")
			bad.Payloads[0].Data = []byte("{")
			must(e.signal(name, bad, nil))
			wantErrors++
			// Exercise envelope rejection before inbound hooks as well as
			// value decoding after hooks. Field 3 is outside payloadwire's subset.
			bad.Payloads[0].ProtoReflect().SetUnknown([]byte{0x18, 0x01})
			must(e.signal(name, bad, nil))
			wantErrors++
			send(name, signals.Message{Text: "valid"})
			send(name, "wrong again")
			wantErrors++
			// Wrong argument counts must also be discarded.
			payloads, err := dc.ToPayloads("one", "two")
			must(err)
			must(e.signal(name, payloads, nil))
			wantErrors++
			must(e.signal(name, &commonpb.Payloads{}, nil))
			wantErrors++
		} else if mode == "next" {
			send(name, []byte("valid"))
		} else {
			send(name, signals.Message{Text: "valid"})
		}
		if mode == "late" {
			send("start", true)
		}
	}
	d.OnWorkflowTaskStarted(5 * time.Second)
	if !e.completed || e.err != nil {
		panic(fmt.Sprintf("corrupt workflow completion: %t %v", e.completed, e.err))
	}
	var got []signals.Message
	must(dc.FromPayloads(e.result, &got))
	if closed {
		if len(got) != 0 {
			panic(fmt.Sprintf("closed corrupt channel delivered %v", got))
		}
	} else if len(got) != 1 || got[0].Text != "valid" {
		panic(fmt.Sprintf("corrupt values delivered: %v", got))
	}
	e.diagnostics.check(wantErrors, replay)
}
