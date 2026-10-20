package protobufvalues

import (
	"context"
	"fmt"
	"isolate"
	"time"

	"github.com/mfateev/sdk-go-poc/example/protobufvalues/model"
	"github.com/mfateev/sdk-go-poc/workflow"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/temporal"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/dynamicpb"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const Precise int64 = 9007199254740993

func Record() *model.Record {
	return &model.Record{Sequence: Precise, Settings: &model.Settings{}, Data: []byte{0, 1, 255}, Names: []string{"first", "second"}, Entries: map[string]*model.Record{"z": {Sequence: 2}, "a": {Sequence: 1}}, State: model.State_STATE_READY, At: &timestamppb.Timestamp{Seconds: 1700000000, Nanos: 123}, Choice: &model.Record_Child{Child: &model.Record{Sequence: 3}}}
}
func CheckRecord(value *model.Record) error {
	if !proto.Equal(value, Record()) {
		return fmt.Errorf("protobuf value lost: %v", value)
	}
	return nil
}
func DataConverter(mode string) converter.DataConverter {
	codecs := []converter.PayloadConverter{converter.NewNilPayloadConverter(), converter.NewByteSlicePayloadConverter()}
	if mode == "binary" {
		codecs = append(codecs, converter.NewProtoPayloadConverter(), converter.NewProtoJSONPayloadConverter())
	} else {
		codecs = append(codecs, converter.NewProtoJSONPayloadConverter(), converter.NewProtoPayloadConverter())
	}
	return converter.NewCompositeDataConverter(append(codecs, converter.NewJSONPayloadConverter())...)
}

//temporal:workflow
func ConverterFactory(config []byte) (converter.DataConverter, error) {
	return DataConverter(string(config)), nil
}

//temporal:workflow
func Child(ctx context.Context, value *model.Record) (*model.Record, error) {
	return value, CheckRecord(value)
}

//temporal:workflow
func Workflow(ctx context.Context, value *model.Record, mode string) (*model.Record, error) {
	if err := CheckRecord(value); err != nil {
		return nil, err
	}
	if err := checkConversions(ctx); err != nil {
		return nil, err
	}
	if value.State.String() != "STATE_READY" {
		return nil, fmt.Errorf("enum name lost")
	}
	if mode == "continued" {
		return value, nil
	}
	if mode == "continue" {
		return nil, workflow.NewContinueAsNewError(ctx, Workflow, value, "continued")
	}
	if mode == "failure" {
		return nil, temporal.NewNonRetryableApplicationError("protobuf details", "Record", nil, value)
	}
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{StartToCloseTimeout: time.Minute})
	ctx = workflow.WithLocalActivityOptions(ctx, workflow.LocalActivityOptions{StartToCloseTimeout: time.Minute})
	var result *model.Record
	future := workflow.ExecuteActivity(ctx, "Echo", value)
	if err := future.Get(ctx, &result); err != nil {
		return nil, err
	}
	if err := CheckRecord(result); err != nil {
		return nil, err
	}
	// Value inputs and value destinations exercise the SDK's pointer construction.
	var local model.Record
	if err := workflow.ExecuteLocalActivity(ctx, "Echo", *value).Get(ctx, &local); err != nil {
		return nil, err
	}
	if err := CheckRecord(&local); err != nil {
		return nil, err
	}
	var child *model.Record
	if err := workflow.ExecuteChildWorkflow(ctx, Child, value).Get(ctx, &child); err != nil {
		return nil, err
	}
	if err := CheckRecord(child); err != nil {
		return nil, err
	}
	if err := workflow.UpsertMemo(ctx, map[string]any{"record": value}); err != nil {
		return nil, err
	}
	if mode == "simple" {
		return value, nil
	}
	cold := Record()
	// Registration-context conversion is independently selected for these handlers.
	queryCtx := workflow.WithDataConverter(ctx, DataConverter("binary"))
	if err := workflow.SetQueryHandler(queryCtx, "value", func() (*model.Record, error) { return value, nil }); err != nil {
		return nil, err
	}
	if err := workflow.SetQueryHandler(queryCtx, "future", func() (*model.Record, error) {
		var out *model.Record
		err := future.Get(queryCtx, &out)
		return out, err
	}); err != nil {
		return nil, err
	}
	if err := workflow.SetQueryHandler(ctx, "echo", func(v *model.Record) (*model.Record, error) { return v, CheckRecord(v) }); err != nil {
		return nil, err
	}
	if err := workflow.SetQueryHandler(ctx, "mutate", func() (*model.Record, error) { value.Sequence++; return value, nil }); err != nil {
		return nil, err
	}
	if err := workflow.SetQueryHandler(ctx, "mutate-reflection", func() (*model.Record, error) {
		value.ProtoReflect().Set(value.ProtoReflect().Descriptor().Fields().ByNumber(1), protoreflect.ValueOfInt64(1))
		return value, nil
	}); err != nil {
		return nil, err
	}
	if err := workflow.SetUpdateHandlerWithOptions(queryCtx, "update", func(_ context.Context, v *model.Record) (*model.Record, error) { return v, CheckRecord(v) }, workflow.UpdateHandlerOptions{Validator: func(v *model.Record) error { return CheckRecord(v) }}); err != nil {
		return nil, err
	}
	// Both the update and query registrations must be visible before readiness.
	if err := workflow.SetQueryHandler(ctx, "conversions", func() (bool, error) {
		if err := CheckRecord(proto.Clone(cold).(*model.Record)); err != nil {
			return false, err
		}
		return true, checkConversions(ctx)
	}); err != nil {
		return nil, err
	}
	if err := workflow.SetQueryHandler(ctx, "ready", func() (bool, error) { return true, nil }); err != nil {
		return nil, err
	}
	received := <-workflow.GetSignalChannel[*model.Record](ctx, "record")
	if err := CheckRecord(received); err != nil {
		return nil, err
	}
	return value, nil
}

//temporal:workflow
func UpstreamPayloads(ctx context.Context) (commonpb.Payloads, error) {
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{StartToCloseTimeout: time.Minute})
	ctx = workflow.WithLocalActivityOptions(ctx, workflow.LocalActivityOptions{StartToCloseTimeout: time.Minute})
	first := &commonpb.Payloads{Payloads: []*commonpb.Payload{{Metadata: map[string][]byte{"encoding": []byte("someencoding")}, Data: []byte("input1")}}}
	second := &commonpb.Payloads{Payloads: []*commonpb.Payload{{Data: []byte("input2")}}}
	var out commonpb.Payloads
	err := workflow.ExecuteActivity(ctx, "Payloads", first, second).Get(ctx, &out)
	if err == nil && !proto.Equal(&out, &commonpb.Payloads{Payloads: []*commonpb.Payload{{Data: []byte("result")}}}) {
		err = fmt.Errorf("payload result lost")
	}
	return out, err
}

//temporal:workflow
func UpstreamRandom(ctx context.Context) (*commonpb.WorkflowType, error) {
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{StartToCloseTimeout: time.Minute})
	ctx = workflow.WithLocalActivityOptions(ctx, workflow.LocalActivityOptions{StartToCloseTimeout: time.Minute})
	var out *commonpb.WorkflowType
	err := workflow.ExecuteActivity(ctx, "Random", &commonpb.WorkflowType{Name: "input1"}, &commonpb.DataBlob{EncodingType: enumspb.ENCODING_TYPE_PROTO3}).Get(ctx, &out)
	if err == nil && !proto.Equal(out, &commonpb.WorkflowType{Name: "result"}) {
		err = fmt.Errorf("random protobuf result lost")
	}
	return out, err
}

func checkConversions(ctx context.Context) error {
	// Proto2 defaults contain process-owned descriptor strings and bytes. Coder
	// construction must copy those scalar defaults into the conversion owner.
	now := time.Now()
	if got := timestamppb.Now().AsTime(); !got.Equal(now) {
		return fmt.Errorf("protobuf constructor changed workflow time: %v != %v", got, now)
	}
	registered, err := protoregistry.GlobalTypes.FindMessageByName("native.protobuf.Record")
	if err != nil {
		return err
	}
	fresh := registered.New().Interface().(*model.Record)
	fresh.Sequence = 42
	if fresh.Sequence != 42 {
		return fmt.Errorf("registry constructor lost private ownership")
	}
	if _, err := (protojson.MarshalOptions{Resolver: protoregistry.GlobalTypes}).Marshal(Record()); err != nil {
		return err
	}
	settings := &model.Settings{}
	for _, mode := range []string{"json", "binary"} {
		dc := workflow.GetDataConverter(workflow.WithDataConverter(ctx, DataConverter(mode)))
		payload, err := dc.ToPayload(settings)
		if err != nil {
			return err
		}
		var decoded *model.Settings
		if err := dc.FromPayload(payload, &decoded); err != nil {
			return err
		}
		if decoded.GetLabel() != "native" || decoded.GetSequence() != Precise || len(decoded.GetData()) != 2 || decoded.GetData()[1] != 255 {
			return fmt.Errorf("proto2 defaults lost")
		}
		wrapped, err := anypb.New(Record())
		if err != nil {
			return err
		}
		payload, err = dc.ToPayload(wrapped)
		if err != nil {
			return err
		}
		var anyValue *anypb.Any
		if err := dc.FromPayload(payload, &anyValue); err != nil {
			return err
		}
		var record model.Record
		if err := anyValue.UnmarshalTo(&record); err != nil {
			return err
		}
		if err := CheckRecord(&record); err != nil {
			return err
		}
		var empty *model.Record
		payload, err = dc.ToPayload(empty)
		if err != nil {
			return err
		}
		var nilRecord *model.Record
		if err := dc.FromPayload(payload, &nilRecord); err != nil {
			return err
		}
		if nilRecord != nil {
			return fmt.Errorf("typed nil protobuf lost")
		}
	}
	unknown := protowire.AppendVarint(protowire.AppendTag(nil, 100, protowire.VarintType), 123)
	record := Record()
	record.ProtoReflect().SetUnknown(unknown)
	dc := workflow.GetDataConverter(workflow.WithDataConverter(ctx, DataConverter("binary")))
	payload, err := dc.ToPayload(record)
	if err != nil {
		return err
	}
	var roundtrip *model.Record
	if err := dc.FromPayload(payload, &roundtrip); err != nil {
		return err
	}
	if string(roundtrip.ProtoReflect().GetUnknown()) != string(unknown) {
		return fmt.Errorf("unknown protobuf fields lost")
	}
	dynamic := dynamicpb.NewMessage(Record().ProtoReflect().Descriptor())
	if payload, err := dc.ToPayload(dynamic); err != nil || payload == nil {
		return fmt.Errorf("dynamic message conversion: %v", err)
	}
	if _, err := dc.ToPayload(&legacyMessage{}); err == nil {
		return fmt.Errorf("gogo-style message accepted")
	}
	return nil
}

type legacyMessage struct{}

func (*legacyMessage) Reset()         {}
func (*legacyMessage) String() string { return "legacy" }
func (*legacyMessage) ProtoMessage()  {}

//temporal:workflow
func ResolverWorkflow(ctx context.Context) error {
	_, err := (protojson.MarshalOptions{Resolver: &protoregistry.Types{}}).Marshal(Record())
	return err
}

type customReflectMessage struct{ model.Record }

func (*customReflectMessage) Reset()         {}
func (*customReflectMessage) String() string { return "custom" }
func (*customReflectMessage) ProtoMessage()  {}
func (m *customReflectMessage) ProtoReflect() protoreflect.Message {
	if !isolate.IsActive() {
		panic("application ProtoReflect executed in host")
	}
	return m.Record.ProtoReflect()
}

//temporal:workflow
func CustomMessageWorkflow(context.Context) error {
	_, err := proto.Marshal(&customReflectMessage{})
	return err
}
