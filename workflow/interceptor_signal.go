package workflow

import (
	"context"
	"encoding/json"
	"github.com/mfateev/sdk-go-poc/internal/payloadwire"
	"isolate"
)

type interceptedSignalKey struct{}
type InterceptedSignals struct {
	Signals []Signal
	Error   string
}

// Deliver signals through the inbound chain when the history event is admitted,
// including signals whose native channel has no receiver yet. The terminal
// copies each Next.HandleSignal delivery; omitting Next filters the signal.
func serveInterceptedSignals() {
	var response []byte
	for {
		raw, err := isolate.Call(OpInterceptSignal, response)
		if err != nil {
			panic(err)
		}
		var signal Signal
		if err = json.Unmarshal(raw, &signal); err != nil {
			panic(err)
		}
		ctx, err := withInterceptorHeader(rootContext, signal.Header)
		if err != nil {
			panic(err)
		}
		payloads, err := decodePayloads(signal.Payloads)
		if err != nil {
			panic(err)
		}
		outcome := InterceptedSignals{}
		ctx = context.WithValue(ctx, interceptedSignalKey{}, func(in *HandleSignalInput) error {
			raw, err := payloadwire.Encode(in.Arg)
			if err != nil {
				return err
			}
			outcome.Signals = append(outcome.Signals, Signal{Name: in.SignalName, Payloads: raw})
			return nil
		})
		if err = activeInterceptors.inbound.HandleSignal(ctx, &HandleSignalInput{SignalName: signal.Name, Arg: payloads}); err != nil {
			outcome.Error = err.Error()
		}
		response, err = json.Marshal(outcome)
		if err != nil {
			panic(err)
		}
	}
}
