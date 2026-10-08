// Package failurewire transfers the pinned Temporal Failure schema as protobuf
// bytes without using process-owned protobuf coder caches inside an isolate.
// It allocates only ordinary caller-owned values. Unknown fields and external
// payload references fail explicitly, as in the POC's Payloads transport.
package failurewire

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/mfateev/sdk-go-poc/internal/payloadwire"
	commonpb "go.temporal.io/api/common/v1"
	failurepb "go.temporal.io/api/failure/v1"
	"google.golang.org/protobuf/encoding/protowire"
)

const recursionLimit = 1000

func Encode(failure *failurepb.Failure) ([]byte, error) {
	if failure == nil {
		return nil, nil
	}
	return encodeMessage(reflect.ValueOf(failure), 0)
}

func Decode(data []byte) (*failurepb.Failure, error) {
	failure := new(failurepb.Failure)
	if err := decodeMessage(data, reflect.ValueOf(failure), 0); err != nil {
		return nil, err
	}
	return failure, nil
}

func tag(field reflect.StructField) (protowire.Number, protowire.Type, error) {
	parts := strings.Split(field.Tag.Get("protobuf"), ",")
	if len(parts) < 2 {
		return 0, 0, fmt.Errorf("failure: missing field tag for %s", field.Name)
	}
	n, err := strconv.Atoi(parts[1])
	if err != nil || n <= 0 {
		return 0, 0, fmt.Errorf("failure: invalid field tag for %s", field.Name)
	}
	switch parts[0] {
	case "bytes":
		return protowire.Number(n), protowire.BytesType, nil
	case "varint":
		return protowire.Number(n), protowire.VarintType, nil
	default:
		return 0, 0, fmt.Errorf("failure: unsupported field encoding for %s", field.Name)
	}
}

func encodeMessage(v reflect.Value, depth int) ([]byte, error) {
	if depth > recursionLimit {
		return nil, fmt.Errorf("failure: recursion limit exceeded")
	}
	if p, ok := v.Interface().(*commonpb.Payloads); ok {
		return payloadwire.Encode(p)
	}
	if p, ok := v.Interface().(*commonpb.Payload); ok {
		data, err := payloadwire.Encode(&commonpb.Payloads{Payloads: []*commonpb.Payload{p}})
		if err != nil {
			return nil, err
		}
		_, _, n := protowire.ConsumeTag(data)
		value, _ := protowire.ConsumeBytes(data[n:])
		return value, nil
	}
	v = v.Elem()
	var result []byte
	for i := range v.NumField() {
		field, value := v.Type().Field(i), v.Field(i)
		if field.PkgPath != "" {
			if field.Name == "unknownFields" && value.Len() != 0 {
				return nil, fmt.Errorf("failure: unknown fields are outside the POC transport")
			}
			continue
		}
		oneof := field.Tag.Get("protobuf_oneof") != ""
		if oneof {
			if value.IsNil() || value.Elem().IsNil() {
				continue
			}
			wrapper := value.Elem().Elem()
			field, value = wrapper.Type().Field(0), wrapper.Field(0)
		}
		if value.IsZero() && !oneof {
			continue
		}
		number, wireType, err := tag(field)
		if err != nil {
			return nil, err
		}
		result = protowire.AppendTag(result, number, wireType)
		switch value.Kind() {
		case reflect.String:
			if !utf8.ValidString(value.String()) {
				return nil, fmt.Errorf("failure: invalid UTF-8 in field %s", field.Name)
			}
			result = protowire.AppendString(result, value.String())
		case reflect.Bool:
			var n uint64
			if value.Bool() {
				n = 1
			}
			result = protowire.AppendVarint(result, n)
		case reflect.Int32, reflect.Int64:
			result = protowire.AppendVarint(result, uint64(value.Int()))
		case reflect.Pointer:
			var data []byte
			if !value.IsNil() {
				data, err = encodeMessage(value, depth+1)
				if err != nil {
					return nil, err
				}
			}
			result = protowire.AppendBytes(result, data)
		default:
			return nil, fmt.Errorf("failure: unsupported field %s", field.Name)
		}
	}
	return result, nil
}

func decodeMessage(data []byte, target reflect.Value, depth int) error {
	if depth > recursionLimit {
		return fmt.Errorf("failure: recursion limit exceeded")
	}
	if p, ok := target.Interface().(*commonpb.Payloads); ok {
		decoded, err := payloadwire.Decode(data)
		if err == nil {
			*p = *decoded
		}
		return err
	}
	if p, ok := target.Interface().(*commonpb.Payload); ok {
		wrapped := protowire.AppendTag(nil, 1, protowire.BytesType)
		wrapped = protowire.AppendBytes(wrapped, data)
		decoded, err := payloadwire.Decode(wrapped)
		if err == nil {
			*p = *decoded.Payloads[0]
		}
		return err
	}
	v := target.Elem()
	for len(data) != 0 {
		number, wireType, n := protowire.ConsumeTag(data)
		if n < 0 {
			return fmt.Errorf("failure: invalid protobuf wire (code %d)", n)
		}
		data = data[n:]
		var value reflect.Value
		var expected protowire.Type
		for i := range v.NumField() {
			field := v.Type().Field(i)
			if field.PkgPath != "" || field.Tag.Get("protobuf") == "" {
				continue
			}
			fieldNumber, fieldType, err := tag(field)
			if err != nil {
				return err
			}
			if fieldNumber == number {
				value, expected = v.Field(i), fieldType
				break
			}
		}
		if !value.IsValid() {
			if f, ok := target.Interface().(*failurepb.Failure); ok {
				wrapper := failureInfo(number)
				if wrapper != nil {
					if f.FailureInfo != nil && reflect.TypeOf(f.FailureInfo) == reflect.TypeOf(wrapper) {
						wrapper = f.FailureInfo
					}
					reflect.ValueOf(f).Elem().FieldByName("FailureInfo").Set(reflect.ValueOf(wrapper))
					value = reflect.ValueOf(wrapper).Elem().Field(0)
					expected = protowire.BytesType
				}
			}
		}
		if !value.IsValid() || expected != wireType {
			return fmt.Errorf("failure: unsupported field %d or wire type %d", number, wireType)
		}
		if wireType == protowire.VarintType {
			num, n := protowire.ConsumeVarint(data)
			if n < 0 {
				return fmt.Errorf("failure: invalid protobuf wire (code %d)", n)
			}
			data = data[n:]
			if value.Kind() == reflect.Bool {
				value.SetBool(num != 0)
			} else {
				value.SetInt(int64(num))
			}
		} else {
			bytes, n := protowire.ConsumeBytes(data)
			if n < 0 {
				return fmt.Errorf("failure: invalid protobuf wire (code %d)", n)
			}
			data = data[n:]
			if value.Kind() == reflect.String {
				if !utf8.Valid(bytes) {
					return fmt.Errorf("failure: invalid UTF-8 in field %d", number)
				}
				value.SetString(string(bytes))
			} else {
				if value.IsNil() {
					value.Set(reflect.New(value.Type().Elem()))
				}
				if err := decodeMessage(bytes, value, depth+1); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

// The schema's oneof is closed and pinned. No reflective or application
// message factory is called while decoding an incoming value.
func failureInfo(number protowire.Number) any {
	switch number {
	case 5:
		return &failurepb.Failure_ApplicationFailureInfo{}
	case 6:
		return &failurepb.Failure_TimeoutFailureInfo{}
	case 7:
		return &failurepb.Failure_CanceledFailureInfo{}
	case 8:
		return &failurepb.Failure_TerminatedFailureInfo{}
	case 9:
		return &failurepb.Failure_ServerFailureInfo{}
	case 10:
		return &failurepb.Failure_ResetWorkflowFailureInfo{}
	case 11:
		return &failurepb.Failure_ActivityFailureInfo{}
	case 12:
		return &failurepb.Failure_ChildWorkflowExecutionFailureInfo{}
	case 13:
		return &failurepb.Failure_NexusOperationExecutionFailureInfo{}
	case 14:
		return &failurepb.Failure_NexusHandlerFailureInfo{}
	}
	return nil
}
