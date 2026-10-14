package support

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/mfateev/sdk-go-poc/workflow"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/sdk/converter"
)

type Key string

const HeaderKey Key = "test-header"
const MaskKey Key = "mask"

//temporal:workflow
func Propagators(config []byte) ([]workflow.ContextPropagator, error) {
	if string(config) == "factory-error" {
		return nil, errors.New("propagator factory failure")
	}
	if string(config) == "nil-propagator" {
		return []workflow.ContextPropagator{(*propagator)(nil)}, nil
	}
	return []workflow.ContextPropagator{&propagator{mode: string(config)}}, nil
}

type propagator struct {
	mode      string
	extracted int
}

func (p *propagator) Inject(ctx context.Context, writer workflow.HeaderWriter) error {
	if p.mode == "inject-error" {
		return errors.New("propagator inject failure")
	}
	value, ok := ctx.Value(HeaderKey).(string)
	if !ok {
		return fmt.Errorf("missing propagated value")
	}
	payload, err := converter.NewCompositeDataConverter(converter.NewJSONPayloadConverter()).ToPayload(value)
	if err != nil {
		return err
	}
	writer.Set(string(HeaderKey), payload)
	return nil
}
func (p *propagator) Extract(ctx context.Context, reader workflow.HeaderReader) (context.Context, error) {
	p.extracted++ // Query/validator instances must be scratch-owned.
	if p.mode == "extract-error" {
		return nil, errors.New("propagator extract failure")
	}
	payload, ok := reader.Get(string(HeaderKey))
	if !ok {
		return ctx, nil
	}
	var value string
	if err := converter.NewCompositeDataConverter(converter.NewJSONPayloadConverter()).FromPayload(payload, &value); err != nil {
		return nil, err
	}
	return context.WithValue(ctx, HeaderKey, value), nil
}

const Encoding = "json/context-isolate"

type payloadConverter struct{}

func (*payloadConverter) Encoding() string { return Encoding }
func (*payloadConverter) ToPayload(value any) (*commonpb.Payload, error) {
	data, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	return &commonpb.Payload{Metadata: map[string][]byte{converter.MetadataEncoding: []byte(Encoding)}, Data: data}, nil
}
func (*payloadConverter) FromPayload(p *commonpb.Payload, ptr any) error {
	return json.Unmarshal(p.Data, ptr)
}
func (*payloadConverter) ToString(p *commonpb.Payload) string { return string(p.Data) }

// CompatibleConverter writes default JSON and accepts the context-specific
// serializer's wire format as well. Host activity mock values use default JSON.
func CompatibleConverter() converter.DataConverter {
	return converter.NewCompositeDataConverter(converter.NewNilPayloadConverter(), converter.NewByteSlicePayloadConverter(), converter.NewJSONPayloadConverter(), &payloadConverter{})
}
func PerContextConverter() converter.DataConverter {
	return converter.NewCompositeDataConverter(converter.NewNilPayloadConverter(), converter.NewByteSlicePayloadConverter(), &payloadConverter{}, converter.NewJSONPayloadConverter())
}

//temporal:workflow
func DataConverter([]byte) (converter.DataConverter, error) {
	return &contextAwareConverter{DataConverter: CompatibleConverter()}, nil
}

type contextAwareConverter struct{ converter.DataConverter }

func (dc *contextAwareConverter) WithContext(ctx context.Context) converter.DataConverter {
	return dc.WithWorkflowContext(ctx)
}
func (dc *contextAwareConverter) WithWorkflowContext(ctx context.Context) converter.DataConverter {
	mask, _ := ctx.Value(MaskKey).(string)
	value, _ := ctx.Value(HeaderKey).(string)
	return &boundConverter{DataConverter: dc.DataConverter, mask: mask, contextValue: value, cache: make(map[string]int)}
}

type boundConverter struct {
	converter.DataConverter
	mask         string
	contextValue string
	cache        map[string]int
}

func (dc *boundConverter) ToPayloads(values ...any) (*commonpb.Payloads, error) {
	dc.cache["encoded"]++
	p, err := dc.DataConverter.ToPayloads(values...)
	if err != nil || p == nil {
		return p, err
	}
	for _, payload := range p.Payloads {
		payload.Metadata["propagated-context"] = []byte(dc.contextValue)
	}
	if dc.mask != "" {
		for _, payload := range p.Payloads {
			payload.Data = bytes.ReplaceAll(payload.Data, []byte(dc.mask), []byte("?"))
		}
	}
	return p, nil
}
func (dc *boundConverter) FromPayloads(p *commonpb.Payloads, ptrs ...any) error {
	dc.cache["decoded"]++
	return dc.DataConverter.FromPayloads(p, ptrs...)
}
