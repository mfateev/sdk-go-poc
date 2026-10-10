package workflow

import (
	"context"
	"encoding/json"
	"fmt"
	"isolate"
	"strings"
	"sync"

	"github.com/mfateev/sdk-go-poc/internal/failurecodec"
	"github.com/mfateev/sdk-go-poc/internal/payloadwire"
	"github.com/mfateev/sdk-go-poc/internal/searchattrwire"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/temporal"
	goWorkflow "go.temporal.io/sdk/workflow"
)

// SignalChannelOptions configures signal metadata using the SDK's options.
type SignalChannelOptions = goWorkflow.SignalChannelOptions

// LastCompletionSnapshot distinguishes absent data from a present empty envelope.
type LastCompletionSnapshot struct {
	Present  bool
	Payloads []byte
}

type SignalRegistration struct {
	Name    string
	Options SignalChannelOptions
}

// metadataCall is available in queries and update validators. Only the host's
// explicitly read-only operations can be used through ReadOnlyCall.
func metadataCall(ctx context.Context, op uint32) ([]byte, error) {
	if ctx == nil {
		return nil, fmt.Errorf("workflow: nil context")
	}
	if isolate.IsReadOnly() {
		return isolate.ReadOnlyCall(op, nil)
	}
	return isolate.Call(op, nil)
}

// GetTypedSearchAttributes returns a private copy of the current typed attributes.
// It can also be called from queries and update validators.
func GetTypedSearchAttributes(ctx context.Context) temporal.SearchAttributes {
	if out := currentOutbound(ctx); out != nil {
		return out.GetTypedSearchAttributes(ctx)
	}
	return getTypedSearchAttributes(ctx)
}

// UpsertSearchAttributes records untyped search attributes using default
// serialization, independently of the worker's converter and payload codecs.
func UpsertSearchAttributes(ctx context.Context, attributes map[string]any) error {
	assertWritable()
	if out := currentOutbound(ctx); out != nil {
		return out.UpsertSearchAttributes(ctx, attributes)
	}
	return upsertSearchAttributes(ctx, attributes)
}

// UpsertTypedSearchAttributes records typed updates, including ValueUnset deletions.
func UpsertTypedSearchAttributes(ctx context.Context, attributes ...temporal.SearchAttributeUpdate) error {
	assertWritable()
	if out := currentOutbound(ctx); out != nil {
		return out.UpsertTypedSearchAttributes(ctx, attributes...)
	}
	return upsertTypedSearchAttributes(ctx, attributes...)
}

// UpsertMemo merges memo fields; nil values delete fields. The recorded SDK flag
// determines serializer selection on replay, with default-converter fallback.
func UpsertMemo(ctx context.Context, memo map[string]any) error {
	assertWritable()
	if out := currentOutbound(ctx); out != nil {
		return out.UpsertMemo(ctx, memo)
	}
	return upsertMemo(ctx, memo)
}

// HasLastCompletionResult reports whether previous-run result data is present,
// without decoding it. A present empty payload envelope also returns true.
func HasLastCompletionResult(ctx context.Context) bool {
	if out := currentOutbound(ctx); out != nil {
		return out.HasLastCompletionResult(ctx)
	}
	return hasLastCompletionResult(ctx)
}

// GetLastCompletionResult decodes previous-run results into the supplied pointers.
// It returns temporal.ErrNoData when no previous result is present.
func GetLastCompletionResult(ctx context.Context, values ...any) error {
	if out := currentOutbound(ctx); out != nil {
		return out.GetLastCompletionResult(ctx, values...)
	}
	return getLastCompletionResult(ctx, values...)
}

// GetLastError returns the previous-run failure, or nil when absent.
func GetLastError(ctx context.Context) error {
	if out := currentOutbound(ctx); out != nil {
		return out.GetLastError(ctx)
	}
	return getLastError(ctx)
}

func getTypedSearchAttributes(ctx context.Context) temporal.SearchAttributes {
	raw, err := metadataCall(ctx, OpGetTypedSearchAttributes)
	if err != nil {
		panic(err)
	}
	attributes, err := searchattrwire.Decode(raw)
	if err != nil {
		panic(err)
	}
	return attributes
}
func upsertSearchAttributes(ctx context.Context, attributes map[string]any) error {
	assertWritable()
	if _, reserved := attributes["TemporalChangeVersion"]; reserved {
		return fmt.Errorf("TemporalChangeVersion is a reserved key that cannot be set with UpsertSearchAttributes")
	}
	fields, err := encodeSearchAttributeValues(attributes)
	if err != nil {
		return err
	}
	return writeMetadata(ctx, OpUpsertSearchAttributes, fields)
}
func upsertTypedSearchAttributes(ctx context.Context, attributes ...temporal.SearchAttributeUpdate) error {
	assertWritable()
	fields, err := searchattrwire.EncodeFields(temporal.NewSearchAttributes(attributes...))
	if err != nil {
		return err
	}
	if _, reserved := fields["TemporalChangeVersion"]; reserved {
		return fmt.Errorf("TemporalChangeVersion is a reserved key that cannot be set with UpsertTypedSearchAttributes")
	}
	return writeMetadata(ctx, OpUpsertSearchAttributes, fields)
}
func writeMetadata(ctx context.Context, op uint32, fields map[string][]byte) error {
	if ctx == nil {
		return fmt.Errorf("workflow: nil context")
	}
	raw, err := json.Marshal(fields)
	if err != nil {
		return err
	}
	_, err = isolate.Call(op, raw)
	return err
}
func upsertMemo(ctx context.Context, memo map[string]any) error {
	assertWritable()
	if len(memo) == 0 {
		return fmt.Errorf("memo is empty")
	}
	raw, err := metadataCall(ctx, OpMemoEncodingPolicy)
	if err != nil {
		return err
	}
	var useUser bool
	if err := json.Unmarshal(raw, &useUser); err != nil {
		return err
	}
	fields, err := encodeMemoFields(memo, currentDataConverter(), useUser)
	if err != nil {
		return err
	}
	return writeMetadata(ctx, OpUpsertMemo, fields)
}
func encodeMemoFields(memo map[string]any, userDC converter.DataConverter, useUser bool) (map[string][]byte, error) {
	dc := newDefaultDataConverter()
	if useUser {
		dc = userDC
	}
	fields := make(map[string][]byte, len(memo))
	for key, value := range memo {
		p, err := dc.ToPayload(value)
		if err != nil && useUser {
			fallback, fallbackErr := newDefaultDataConverter().ToPayload(value)
			if fallbackErr == nil {
				p, err = fallback, nil
			}
		}
		if err != nil {
			return nil, fmt.Errorf("encode workflow memo error: %v", err)
		}
		fields[key], err = payloadwire.Encode(&commonpb.Payloads{Payloads: []*commonpb.Payload{p}})
		if err != nil {
			return nil, err
		}
	}
	return fields, nil
}

func lastCompletion(ctx context.Context) (LastCompletionSnapshot, error) {
	raw, err := metadataCall(ctx, OpLastCompletionResult)
	var snapshot LastCompletionSnapshot
	if err == nil {
		err = json.Unmarshal(raw, &snapshot)
	}
	return snapshot, err
}
func hasLastCompletionResult(ctx context.Context) bool {
	raw, err := metadataCall(ctx, OpHasLastCompletionResult)
	if err != nil {
		panic(err)
	}
	var present bool
	if err := json.Unmarshal(raw, &present); err != nil {
		panic(err)
	}
	return present
}

func getLastCompletionResult(ctx context.Context, values ...any) error {
	snapshot, err := lastCompletion(ctx)
	if err != nil {
		return err
	}
	if !snapshot.Present {
		return temporal.ErrNoData
	}
	p, err := payloadwire.Decode(snapshot.Payloads)
	if err != nil {
		return err
	}
	return currentDataConverter().FromPayloads(p, values...)
}
func getLastError(ctx context.Context) error {
	raw, err := metadataCall(ctx, OpLastError)
	if err != nil {
		panic(err)
	}
	if len(raw) == 0 {
		return nil
	}
	cause, err := failurecodec.Decode(raw, currentDataConverter())
	if err != nil {
		panic(err)
	}
	return cause
}

var signalChannels = make(map[string]<-chan SignalResult)
var signalChannelsMu sync.Mutex

// GetSignalChannelWithOptions returns the workflow-owned native signal channel.
// Repeated registrations for a name reuse the channel and its first description.
func GetSignalChannelWithOptions(ctx context.Context, name string, options SignalChannelOptions) <-chan SignalResult {
	assertWritable()
	if out := currentOutbound(ctx); out != nil {
		return out.GetSignalChannelWithOptions(ctx, name, options)
	}
	return getSignalChannelWithOptions(ctx, name, options)
}
func getSignalChannel(ctx context.Context, name string) <-chan SignalResult {
	return getSignalChannelWithOptions(ctx, name, SignalChannelOptions{})
}

// Repeated requests return the same native channel; as in the SDK, the first
// registration chooses its description. Host metadata contains copied options.
func getSignalChannelWithOptions(ctx context.Context, name string, options SignalChannelOptions) <-chan SignalResult {
	assertWritable()
	if strings.HasPrefix(name, "__temporal_") && !strings.HasPrefix(name, "__temporal_workflow_stream_") {
		panic("__temporal_ is a reserved prefix")
	}
	if ctx == nil {
		return newSignalChannel(ctx, name)
	}
	signalChannelsMu.Lock()
	defer signalChannelsMu.Unlock()
	if ch := signalChannels[name]; ch != nil {
		return ch
	}
	raw, err := json.Marshal(SignalRegistration{Name: name, Options: options})
	if err != nil {
		panic(err)
	}
	if _, err := isolate.Call(OpRegisterSignal, raw); err != nil {
		panic(err)
	}
	// Channel lifetime belongs to the workflow, not a caller's activity context.
	signalCtx := rootContext
	if signalCtx == nil {
		signalCtx = ctx
	}
	ch := newSignalChannel(signalCtx, name)
	signalChannels[name] = ch
	return ch
}
