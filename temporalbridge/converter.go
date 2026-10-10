package temporalbridge

import (
	"fmt"
	"isolate"

	"github.com/mfateev/sdk-go-poc/internal/failurecodec"
	"github.com/mfateev/sdk-go-poc/internal/failurewire"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/sdk/converter"
	bindings "go.temporal.io/sdk/internalbindings"
	"google.golang.org/protobuf/proto"
)

// DataConverterOptions is the host snapshot of a compiler-created serializer
// factory. Config is transported as copied bytes, never shared with the isolate.
type DataConverterOptions struct {
	Factory isolate.Handle
	Config  []byte
}

// RawValue bypasses host value serialization while retaining configured codecs.
// Use the single-payload contract to reject cardinality-changing codecs rather
// than silently truncating decoded arguments or failure details.
func encodeTransport(p *commonpb.Payloads, dc converter.DataConverter) (*commonpb.Payloads, error) {
	if p == nil {
		return nil, nil
	}
	out := &commonpb.Payloads{Payloads: make([]*commonpb.Payload, len(p.Payloads))}
	for i, payload := range p.Payloads {
		if payload == nil {
			return nil, fmt.Errorf("nil payload %d", i)
		}
		var err error
		out.Payloads[i], err = dc.ToPayload(converter.NewRawValue(payload))
		if err != nil {
			panic(&WorkflowTaskError{Cause: fmt.Errorf("encode isolate payload %d: %w", i, err)})
		}
	}
	return out, nil
}

func decodeTransport(p *commonpb.Payloads, dc converter.DataConverter) (*commonpb.Payloads, error) {
	if p == nil {
		return nil, nil
	}
	out := &commonpb.Payloads{Payloads: make([]*commonpb.Payload, len(p.Payloads))}
	for i, payload := range p.Payloads {
		if payload == nil {
			return nil, fmt.Errorf("nil payload %d", i)
		}
		var raw converter.RawValue
		if err := dc.FromPayload(payload, &raw); err != nil {
			panic(&WorkflowTaskError{Cause: fmt.Errorf("decode isolate payload %d: %w", i, err)})
		}
		if raw.Payload() == nil {
			return nil, fmt.Errorf("host data converter does not implement RawValue at payload %d", i)
		}
		out.Payloads[i] = raw.Payload()
	}
	return out, nil
}

func inboundPayloadBytes(p *commonpb.Payloads, dc converter.DataConverter) ([]byte, error) {
	plain, err := decodeTransport(p, dc)
	if err != nil || plain == nil {
		return nil, err
	}
	return proto.MarshalOptions{Deterministic: true}.Marshal(plain)
}

func (d *definition) rootDataConverter() converter.DataConverter {
	if root, ok := d.env.(interface {
		GetRootDataConverter() converter.DataConverter
	}); ok {
		return root.GetRootDataConverter()
	}
	return d.env.GetDataConverter()
}

func (d *definition) workflowDataConverter(namespace, id string) converter.DataConverter {
	return converter.WithDataConverterSerializationContext(d.rootDataConverter(), converter.WorkflowSerializationContext{Namespace: namespace, WorkflowID: id})
}

func (d *definition) activityDataConverter(name, taskQueue string) converter.DataConverter {
	info := d.env.WorkflowInfo()
	return converter.WithDataConverterSerializationContext(d.rootDataConverter(), converter.ActivitySerializationContext{
		Namespace: info.Namespace, WorkflowID: info.WorkflowExecution.ID, WorkflowType: info.WorkflowType.Name, ActivityType: name, TaskQueue: taskQueue,
	})
}

// Reserve the SDK's reset-aware child ID before context-dependent input
// serialization, then pass it explicitly when scheduling the child.
func (d *definition) prepareChildID(id string) string {
	if id != "" {
		return id
	}
	if _, contextAware := d.rootDataConverter().(converter.DataConverterWithSerializationContext); !contextAware {
		return id
	}
	return bindings.GenerateChildWorkflowID(d.env)
}

// Convert every payload in the failure tree, including encrypted common
// attributes, without decoding application values on the host. Failure holders
// can retain their original encrypted payloads, so do not blindly re-encode them.
func mapFailurePayloads(raw []byte, dc converter.DataConverter, encode bool) ([]byte, error) {
	f, err := failurewire.Decode(raw)
	if err != nil {
		return nil, err
	}
	transform := decodeTransport
	if encode {
		transform = encodeTransport
	}
	for current := f; current != nil; current = current.Cause {
		if current.EncodedAttributes != nil {
			p, err := transform(&commonpb.Payloads{Payloads: []*commonpb.Payload{current.EncodedAttributes}}, dc)
			if err != nil {
				return nil, err
			}
			current.EncodedAttributes = p.Payloads[0]
		}
		var p **commonpb.Payloads
		switch {
		case current.GetApplicationFailureInfo() != nil:
			p = &current.GetApplicationFailureInfo().Details
		case current.GetTimeoutFailureInfo() != nil:
			p = &current.GetTimeoutFailureInfo().LastHeartbeatDetails
		case current.GetCanceledFailureInfo() != nil:
			p = &current.GetCanceledFailureInfo().Details
		case current.GetResetWorkflowFailureInfo() != nil:
			p = &current.GetResetWorkflowFailureInfo().LastHeartbeatDetails
		}
		if p != nil {
			result, err := transform(*p, dc)
			if err != nil {
				return nil, err
			}
			*p = result
		}
	}
	return failurewire.Encode(f)
}

func encodeInboundFailure(cause error, dc converter.DataConverter) ([]byte, error) {
	raw, err := failurecodec.Encode(cause, dc)
	if err != nil {
		return nil, err
	}
	return mapFailurePayloads(raw, dc, false)
}

func decodeOutboundFailure(raw []byte, dc converter.DataConverter) (error, error) {
	encoded, err := mapFailurePayloads(raw, dc, true)
	if err != nil {
		return nil, err
	}
	return failurecodec.Decode(encoded, dc)
}
