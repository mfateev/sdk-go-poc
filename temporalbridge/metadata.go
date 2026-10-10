package temporalbridge

import (
	"encoding/json"
	"fmt"
	"isolate"
	"reflect"
	"slices"
	"unsafe"

	"github.com/mfateev/sdk-go-poc/internal/failurewire"
	"github.com/mfateev/sdk-go-poc/internal/searchattrwire"
	"github.com/mfateev/sdk-go-poc/workflow"
	commonpb "go.temporal.io/api/common/v1"
	failurepb "go.temporal.io/api/failure/v1"
	sdkpb "go.temporal.io/api/sdk/v1"
	"go.temporal.io/sdk/converter"
	bindings "go.temporal.io/sdk/internalbindings"
)

// memoUserDCFlag is SDKFlagMemoUserDCEncode in the pinned SDK v1.49.0.
// TryUse records/reads the SDK flag, preserving encoding on old histories.
const memoUserDCFlag = 7

func (d *definition) readMetadata(op uint32) ([]byte, error) {
	switch op {
	case workflow.OpGetTypedSearchAttributes:
		return searchattrwire.Encode(d.env.TypedSearchAttributes())
	case workflow.OpHasLastCompletionResult:
		return json.Marshal(bindings.GetLastCompletionResult(d.env) != nil)
	case workflow.OpLastCompletionResult:
		p := bindings.GetLastCompletionResult(d.env)
		snapshot := workflow.LastCompletionSnapshot{Present: p != nil}
		if p != nil {
			var err error
			snapshot.Payloads, err = inboundPayloadBytes(p, d.env.GetDataConverter())
			if err != nil {
				return nil, err
			}
		}
		return json.Marshal(snapshot)
	case workflow.OpLastError:
		return d.lastFailureBytes()
	default:
		return nil, fmt.Errorf("unsupported metadata read: %d", op)
	}
}

// The pinned internalbindings API exposes lastCompletionResult but not
// lastFailure. This host-only adapter reads the SDK field with a checked layout
// and immediately copies its protobuf bytes. No pointer crosses into an isolate.
// Remove this ABI dependency when upstream exposes a failure getter.
func (d *definition) lastFailureBytes() ([]byte, error) {
	field := reflect.ValueOf(d.env.WorkflowInfo()).Elem().FieldByName("lastFailure")
	if !field.IsValid() || field.Type() != reflect.TypeFor[*failurepb.Failure]() || !field.CanAddr() {
		return nil, fmt.Errorf("workflow last failure: incompatible SDK layout")
	}
	failure := reflect.NewAt(field.Type(), unsafe.Pointer(field.UnsafeAddr())).Elem().Interface().(*failurepb.Failure)
	if failure == nil {
		return nil, nil
	}
	raw, err := failurewire.Encode(failure)
	if err != nil {
		return nil, err
	}
	return mapFailurePayloads(raw, d.env.GetDataConverter(), false)
}

func (d *definition) handleMetadata(command *isolate.Command) error {
	switch command.Op {
	case workflow.OpGetTypedSearchAttributes, workflow.OpHasLastCompletionResult, workflow.OpLastCompletionResult, workflow.OpLastError:
		raw, err := d.readMetadata(command.Op)
		d.replyWhenSuspended(command, raw, err)
	case workflow.OpMemoEncodingPolicy:
		raw, err := json.Marshal(d.env.TryUse(memoUserDCFlag))
		d.replyWhenSuspended(command, raw, err)
	case workflow.OpRegisterSignal:
		var r workflow.SignalRegistration
		if err := json.Unmarshal(command.Payload, &r); err != nil {
			return err
		}
		if d.signalChannels == nil {
			d.signalChannels = make(map[string]workflow.SignalChannelOptions)
		}
		if _, registered := d.signalChannels[r.Name]; !registered {
			d.signalChannels[r.Name] = r.Options
		}
		d.replyWhenSuspended(command, nil, nil)
	case workflow.OpUpsertSearchAttributes, workflow.OpUpsertMemo:
		var fields map[string][]byte
		if err := json.Unmarshal(command.Payload, &fields); err != nil {
			return err
		}
		values, err := decodeOptionValues(fields)
		if err == nil {
			if command.Op == workflow.OpUpsertSearchAttributes {
				if _, reserved := values["TemporalChangeVersion"]; reserved {
					err = fmt.Errorf("TemporalChangeVersion is a reserved key that cannot be set, please use other key")
				} else {
					// The SDK accepts already-encoded payloads and never applies worker
					// serializers/codecs to search attributes.
					for key, value := range values {
						values[key] = value.(converter.RawValue).Payload()
					}
					err = d.env.UpsertSearchAttributes(values)
				}
			} else {
				// RawValue skips host serialization while retaining the SDK's memo
				// transport codecs, default fallback and recorded replay flag.
				err = d.env.UpsertMemo(values)
			}
		}
		d.replyWhenSuspended(command, nil, err)
	}
	return nil
}

func (d *definition) workflowMetadata() (*commonpb.Payloads, error) {
	result := &sdkpb.WorkflowMetadata{Definition: &sdkpb.WorkflowDefinition{Type: d.env.WorkflowInfo().WorkflowType.Name}}
	definition := result.Definition
	definition.QueryDefinitions = []*sdkpb.WorkflowInteractionDefinition{
		{Name: "__stack_trace", Description: "Current stack trace"},
		{Name: "__open_sessions", Description: "Open sessions on the workflow"},
		{Name: "__temporal_workflow_metadata", Description: "Metadata about the workflow"},
	}
	for name, options := range d.queryHandlers {
		definition.QueryDefinitions = append(definition.QueryDefinitions, &sdkpb.WorkflowInteractionDefinition{Name: name, Description: options.Description})
	}
	for name, options := range d.signalChannels {
		definition.SignalDefinitions = append(definition.SignalDefinitions, &sdkpb.WorkflowInteractionDefinition{Name: name, Description: options.Description})
	}
	for name, registration := range d.updateHandlers {
		definition.UpdateDefinitions = append(definition.UpdateDefinitions, &sdkpb.WorkflowInteractionDefinition{Name: name, Description: registration.Description})
	}
	for _, definitions := range [][]*sdkpb.WorkflowInteractionDefinition{definition.QueryDefinitions, definition.SignalDefinitions, definition.UpdateDefinitions} {
		slices.SortFunc(definitions, func(a, b *sdkpb.WorkflowInteractionDefinition) int {
			if a.Name < b.Name {
				return -1
			}
			if a.Name > b.Name {
				return 1
			}
			return 0
		})
	}
	// Upstream encodes this built-in with the default serializer followed by
	// the configured host transport codec, even with a custom worker serializer.
	p, err := converter.GetDefaultDataConverter().ToPayloads(result)
	if err != nil {
		return nil, err
	}
	return encodeTransport(p, d.env.GetDataConverter())
}
