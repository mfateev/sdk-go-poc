package workflow

import (
	"context"
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
		if err = UnmarshalTransport(raw, &signal); err != nil {
			panic(err)
		}
		ctx, err := withIncomingHeader(rootContext, signal.Header)
		if err != nil {
			panic(err)
		}
		payloads, err := decodePayloads(signal.Payloads, GetDataConverter(ctx))
		if err != nil {
			reportCorruptSignal(ctx, signal.Name, err)
			// Acknowledge a dropped signal so admission can finish and the next
			// signal can traverse the interceptor chain.
			response, err = MarshalTransport(InterceptedSignals{})
			if err != nil {
				panic(err)
			}
			continue
		}
		outcome := InterceptedSignals{}
		ctx = context.WithValue(ctx, interceptedSignalKey{}, func(in *HandleSignalInput) error {

			outcome.Signals = append(outcome.Signals, Signal{Name: in.SignalName, Payloads: in.Arg})
			return nil
		})
		if err = activeInterceptors.inbound.HandleSignal(ctx, &HandleSignalInput{SignalName: signal.Name, Arg: payloads}); err != nil {
			outcome.Error = err.Error()
		}
		response, err = MarshalTransport(outcome)
		if err != nil {
			panic(err)
		}
	}
}
