package workflow

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"

	commonpb "go.temporal.io/api/common/v1"
)

func signalPayload(t *testing.T, encoding, data string) Signal {
	t.Helper()
	return Signal{Payloads: &commonpb.Payloads{Payloads: []*commonpb.Payload{{Metadata: map[string][]byte{"encoding": []byte(encoding)}, Data: []byte(data)}}}}
}

func TestDecodeTypedSignal(t *testing.T) {
	type message struct {
		ID   int64
		Tags []string
	}
	value, err := decodeSignal[message](signalPayload(t, "json/plain", `{"ID":9007199254740993,"Tags":["one","two"]}`))
	if err != nil || value.ID != 9007199254740993 || strings.Join(value.Tags, ",") != "one,two" {
		t.Fatalf("typed signal = %+v, %v", value, err)
	}
	text, err := decodeSignal[string](signalPayload(t, "json/plain", `"hello"`))
	if err != nil || text != "hello" {
		t.Fatalf("string signal = %q, %v", text, err)
	}
	binary, err := decodeSignal[[]byte](signalPayload(t, "binary/plain", "bytes"))
	if err != nil || !bytes.Equal(binary, []byte("bytes")) {
		t.Fatalf("binary signal = %q, %v", binary, err)
	}
	null, err := decodeSignal[*message](signalPayload(t, "binary/null", ""))
	if err != nil || null != nil {
		t.Fatalf("null signal = %+v, %v", null, err)
	}
}

func TestDecodeTypedSignalRejectsInvalidPayload(t *testing.T) {
	if _, err := decodeSignal[[]byte](Signal{Input: []byte("raw")}); err == nil {
		t.Fatal("signal without serialized payload accepted")
	}
	for _, signal := range []Signal{
		signalPayload(t, "json/plain", `{"ID":1,"Tags":false}`),
		signalPayload(t, "json/plain", `"wrong type"`),
		signalPayload(t, "unsupported", `0`),
		{Payloads: &commonpb.Payloads{Payloads: []*commonpb.Payload{nil}}},
		{Payloads: &commonpb.Payloads{Payloads: []*commonpb.Payload{{}, {}}}},
		{},
	} {
		value, err := decodeSignal[struct {
			ID   int64
			Tags []string
		}](signal)
		if err == nil || value.ID != 0 || value.Tags != nil {
			t.Fatalf("invalid signal returned partial value %+v, %v", value, err)
		}
	}
	if value, err := decodeSignal[*customTestProto](signalPayload(t, "binary/null", "")); err != nil || value != nil {
		t.Fatalf("nil protobuf signal: %v %v", value, err)
	}
}

func TestTypedSignalChannelIdentityOrderingAndCancellation(t *testing.T) {
	previousChannels, previousRoot := typedSignalChannels, rootContext
	typedSignalChannels, rootContext = make(map[string]any), nil
	t.Cleanup(func() { typedSignalChannels, rootContext = previousChannels, previousRoot })
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	raw := make(chan SignalResult, 3)
	for _, value := range []string{`"first"`, `"second"`, `"third"`} {
		raw <- SignalResult{Signal: signalPayload(t, "json/plain", value)}
	}
	ch := typedSignalChannel[string](ctx, "message", raw)
	if ch != typedSignalChannel[string](ctx, "message", raw) {
		t.Fatal("repeated registration returned a different channel")
	}
	for _, want := range []string{"first", "second", "third"} {
		select {
		case got := <-ch:
			if got != want {
				t.Fatalf("signal = %q, want %q", got, want)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("signal delivery timed out")
		}
	}
	cancel()
	select {
	case _, ok := <-ch:
		if ok {
			t.Fatal("cancellation delivered a zero signal")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("cancellation did not close the channel")
	}
	defer func() {
		p := recover()
		if p == nil || !strings.Contains(p.(string), "already registered") {
			t.Fatalf("conflicting channel type panic = %v", p)
		}
	}()
	typedSignalChannel[int](ctx, "message", raw)
}
