// Package metadata exercises trusted type services through the real SDK
// dispatcher. The driver runs it without a Temporal server.
package metadata

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"time"

	"github.com/mfateev/sdk-go-poc/internal/failurecodec"
	"github.com/mfateev/sdk-go-poc/internal/failurewire"
	"github.com/mfateev/sdk-go-poc/workflow"
	enumspb "go.temporal.io/api/enums/v1"

	commonpb "go.temporal.io/api/common/v1"
	failurepb "go.temporal.io/api/failure/v1"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/temporal"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/runtime/protoimpl"
)

//go:isolate
func MetadataWorkflow(_ context.Context, mode string) (result string, err error) {
	defer func() {
		if p := recover(); p != nil {
			err = fmt.Errorf("metadata workflow unexpected panic: %v", p)
		}
	}()
	// These receiver checks are ownership faults, so the workflow's recover
	// above must not turn them into a normal result or workflow error.
	if mode == "private registry" {
		_ = (&protoregistry.Types{}).NumMessages()
		return "ownership violation recovered", nil
	}
	if mode == "private message info" {
		_ = (&protoimpl.MessageInfo{}).Descriptor()
		return "ownership violation recovered", nil
	}
	switch mode {
	case "reject visitor":
		protoregistry.GlobalTypes.RangeMessages(func(protoreflect.MessageType) bool { panic("registry visitor executed") })
		return "metadata violation returned", nil
	case "reject builder":
		_ = (protoimpl.TypeBuilder{}).Build()
		return "metadata violation returned", nil
	case "reject descriptor":
		_, _ = protoregistry.GlobalFiles.FindDescriptorByName("isolate_metadata_callback_probe.Record")
		return "metadata violation returned", nil
	case "reject mutation":
		message, err := protoregistry.GlobalTypes.FindMessageByName("temporal.api.common.v1.Payload")
		if err != nil {
			return "", err
		}
		_ = protoregistry.GlobalTypes.RegisterMessage(message)
		return "metadata violation returned", nil
	case "reject clone":
		_ = proto.Clone(&customMessage{})
		return "metadata violation returned", nil
	}
	// Message state retains its canonical MessageInfo, while the actual message
	// and bytes stay private. Heap-backed slots exercise reference publication;
	// Header also exercises a later element in the same metadata table.
	payload := &commonpb.Payload{Data: []byte("private payload")}
	types := messageTypeSlots()
	types[0], types[1] = payload.ProtoReflect().Type(), (&commonpb.Header{}).ProtoReflect().Type()
	created := types[1].New().Interface().(*commonpb.Header)
	created.Fields = map[string]*commonpb.Payload{"owned": payload}
	// The generated-value clone path must copy mutable containers without
	// granting access to protobuf's shared coder caches.
	clone := proto.Clone(created).(*commonpb.Header)
	clone.Fields["owned"].Data[0] = 'X'
	if string(payload.Data) != "private payload" {
		return "", fmt.Errorf("protobuf clone retained mutable input")
	}
	if proto.Clone((*commonpb.Payload)(nil)).(*commonpb.Payload) != nil || proto.Clone(nil) != nil {
		return "", fmt.Errorf("protobuf clone lost typed nil")
	}
	empty := proto.Clone(&commonpb.Payloads{Payloads: []*commonpb.Payload{nil, {Data: []byte{}}}}).(*commonpb.Payloads)
	if empty.Payloads[0] == nil || empty.Payloads[1].Data != nil {
		return "", fmt.Errorf("protobuf clone changed empty value semantics")
	}
	emptyMap := proto.Clone(&commonpb.Header{Fields: map[string]*commonpb.Payload{}}).(*commonpb.Header)
	if emptyMap.Fields != nil {
		return "", fmt.Errorf("protobuf clone retained empty map")
	}
	failure := &failurepb.Failure{Message: "structured", FailureInfo: &failurepb.Failure_ApplicationFailureInfo{ApplicationFailureInfo: &failurepb.ApplicationFailureInfo{Type: "checked", NonRetryable: true}}}
	dc := converter.NewCompositeDataConverter(converter.NewNilPayloadConverter(), converter.NewByteSlicePayloadConverter(), converter.NewJSONPayloadConverter())
	fc := temporal.NewDefaultFailureConverter(temporal.DefaultFailureConverterOptions{DataConverter: dc})
	decoded := fc.FailureToError(failure)
	application, ok := decoded.(*temporal.ApplicationError)
	if !ok || application.Type() != "checked" || !application.NonRetryable() {
		return "", fmt.Errorf("structured failure conversion lost its type")
	}
	if !errors.Is(application.Details(), temporal.ErrNoData) {
		return "", fmt.Errorf("missing error details lost SDK sentinel identity")
	}
	// Parse errors must be caller-owned, including errors in nested Payloads.
	// Protobuf's ParseError helper returns process-owned sentinel objects.
	for _, data := range [][]byte{{0}, {0x0a, 0x80}, {0x2a, 3, 0x1a, 1, 0x80}} {
		if _, err := failurewire.Decode(data); err == nil {
			return "", fmt.Errorf("malformed failure was accepted")
		}
	}
	var wg sync.WaitGroup
	failures := make(chan error, 8)
	for range 8 {
		wg.Go(func() {
			for n := 1; n <= 32; n++ {
				t := reflect.StructOf([]reflect.StructField{{Name: "Value", Type: reflect.ArrayOf(n, reflect.TypeFor[int]())}})
				for _, built := range []reflect.Type{
					reflect.PointerTo(t), reflect.SliceOf(t), reflect.ChanOf(reflect.BothDir, t),
					reflect.MapOf(reflect.TypeFor[string](), t), reflect.FuncOf([]reflect.Type{t}, []reflect.Type{t}, false),
				} {
					if built == nil {
						failures <- fmt.Errorf("missing constructed type")
						return
					}
				}
				// Name indexes and descriptor tables are initialized lazily. These
				// are metadata services; the actual message and values remain local.
				message, err := protoregistry.GlobalTypes.FindMessageByName("temporal.api.common.v1.Payload")
				if err != nil {
					failures <- err
					return
				}
				if message.Descriptor().Fields().ByName("data") == nil {
					failures <- fmt.Errorf("missing descriptor field")
					return
				}
			}
		})
	}
	wg.Wait()
	// Frequent GC in the host driver runs while the concurrent cache builders
	// allocate. These message values and metadata references remain live.
	if string(payload.Data) != "private payload" || created.Fields["owned"] != payload || types[0].Descriptor().Fields().ByName("data") == nil || types[1].Descriptor().Fields().ByName("fields") == nil {
		return "", fmt.Errorf("canonical message info or private payload changed")
	}
	close(failures)
	for err := range failures {
		return "", err
	}
	encoded, err := json.Marshal(applicationValue{})
	if err != nil || string(encoded) != `"application owner retained"` {
		return "", fmt.Errorf("application callback: %s %v", encoded, err)
	}
	return "metadata services passed", nil
}

type customMessage struct{}

func (*customMessage) ProtoMessage()                      {}
func (*customMessage) ProtoReflect() protoreflect.Message { panic("custom clone callback executed") }

//go:isolate
func StructuredFailureWorkflow(ctx context.Context, returnFailure bool) ([]byte, error) {
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{StartToCloseTimeout: time.Minute})
	err := workflow.ExecuteActivity(ctx, "echo", []byte("failure")).Get(ctx, nil)
	var activity *temporal.ActivityError
	var timeout *temporal.TimeoutError
	var application *temporal.ApplicationError
	if !errors.As(err, &activity) || activity.ActivityID() != "activity-id" || activity.RetryState() != enumspb.RETRY_STATE_MAXIMUM_ATTEMPTS_REACHED || !errors.As(err, &timeout) || timeout.TimeoutType() != enumspb.TIMEOUT_TYPE_HEARTBEAT || !errors.As(err, &application) || application.Type() != "invalid" || !application.NonRetryable() || application.NextRetryDelay() != 3*time.Second {
		return nil, fmt.Errorf("structured error chain changed: %v", err)
	}
	var detail string
	var number int64
	var data []byte
	if decodeErr := application.Details(&detail, &number, &data); decodeErr != nil || detail != "detail" || number != 9007199254740993 || string(data) != "private" {
		return nil, fmt.Errorf("application details changed: %v", decodeErr)
	}
	var heartbeat string
	if decodeErr := timeout.LastHeartbeatDetails(&heartbeat); decodeErr != nil || heartbeat != "heartbeat" {
		return nil, fmt.Errorf("heartbeat details changed: %v", decodeErr)
	}
	if returnFailure {
		return nil, err
	}
	dc := converter.NewCompositeDataConverter(converter.NewNilPayloadConverter(), converter.NewByteSlicePayloadConverter(), converter.NewJSONPayloadConverter())
	return failurecodec.Encode(err, dc)
}

//go:noinline
func messageTypeSlots() *[2]protoreflect.MessageType { return new([2]protoreflect.MessageType) }

type applicationValue struct{}

func (applicationValue) MarshalJSON() ([]byte, error) {
	// Struct keys in application sync.Map.Range remain unsupported by the
	// deterministic API. A leaked service scope would silently permit them.
	var values sync.Map
	values.Store(struct{ N int }{1}, 2)
	var rejected bool
	func() {
		defer func() { rejected = recover() != nil }()
		values.Range(func(any, any) bool { return true })
	}()
	if !rejected {
		return nil, fmt.Errorf("application callback inherited service privileges")
	}
	return []byte(`"application owner retained"`), nil
}
