// Package searchattrwire transports Temporal typed search attributes without
// sending SDK objects across the isolate boundary. Search attributes always use
// the SDK default converter, independently of application serializers/codecs.
package searchattrwire

import (
	"encoding/json"
	"fmt"
	"reflect"
	"time"

	"github.com/mfateev/sdk-go-poc/internal/payloadwire"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/temporal"
)

func defaultConverter() converter.DataConverter {
	return converter.NewCompositeDataConverter(converter.NewNilPayloadConverter(), converter.NewByteSlicePayloadConverter(), converter.NewProtoJSONPayloadConverter(), converter.NewProtoPayloadConverter(), converter.NewJSONPayloadConverter())
}

func EncodeFields(attributes temporal.SearchAttributes) (map[string][]byte, error) {
	fields := make(map[string][]byte)
	dc := defaultConverter()
	for key, value := range attributes.GetUntypedValues() {
		p, err := dc.ToPayload(value)
		if err != nil {
			return nil, err
		}
		if p.Data != nil {
			p.Metadata["type"] = []byte(key.GetValueType().String())
		}
		fields[key.GetName()], err = payloadwire.Encode(&commonpb.Payloads{Payloads: []*commonpb.Payload{p}})
		if err != nil {
			return nil, err
		}
	}
	// SDK v1.49.0's public GetUntypedValues drops ValueUnset entries. Read only
	// the primitive name for those nil values; no unsafe access or SDK object is
	// exposed. Keep this layout check and tests when upgrading the pinned SDK.
	entries := reflect.ValueOf(attributes).FieldByName("untypedValue")
	if !entries.IsValid() || entries.Kind() != reflect.Map {
		return nil, fmt.Errorf("search attributes: incompatible SDK layout")
	}
	iter := entries.MapRange()
	for iter.Next() {
		if !iter.Value().IsNil() {
			continue
		}
		name := iter.Key().Elem().FieldByName("name")
		if !name.IsValid() || name.Kind() != reflect.String {
			return nil, fmt.Errorf("search attributes: incompatible SDK key layout")
		}
		p, _ := dc.ToPayload(nil)
		raw, err := payloadwire.Encode(&commonpb.Payloads{Payloads: []*commonpb.Payload{p}})
		if err != nil {
			return nil, err
		}
		fields[name.String()] = raw
	}
	return fields, nil
}

func Encode(attributes temporal.SearchAttributes) ([]byte, error) {
	fields, err := EncodeFields(attributes)
	if err != nil {
		return nil, err
	}
	return json.Marshal(fields)
}

func Decode(raw []byte) (temporal.SearchAttributes, error) {
	var fields map[string][]byte
	if err := json.Unmarshal(raw, &fields); err != nil {
		return temporal.SearchAttributes{}, err
	}
	dc := defaultConverter()
	updates := make([]temporal.SearchAttributeUpdate, 0, len(fields))
	for name, raw := range fields {
		envelope, err := payloadwire.Decode(raw)
		if err != nil {
			return temporal.SearchAttributes{}, err
		}
		if len(envelope.Payloads) != 1 {
			return temporal.SearchAttributes{}, fmt.Errorf("search attributes: expected one payload")
		}
		p := envelope.Payloads[0]
		kind, err := indexedValueType(string(p.Metadata["type"]))
		if err != nil {
			return temporal.SearchAttributes{}, err
		}
		var update temporal.SearchAttributeUpdate
		switch kind {
		case enumspb.INDEXED_VALUE_TYPE_TEXT:
			var value string
			if err := dc.FromPayload(p, &value); err != nil {
				return temporal.SearchAttributes{}, err
			}
			update = temporal.NewSearchAttributeKeyString(name).ValueSet(value)
		case enumspb.INDEXED_VALUE_TYPE_KEYWORD:
			var value string
			if err := dc.FromPayload(p, &value); err != nil {
				return temporal.SearchAttributes{}, err
			}
			update = temporal.NewSearchAttributeKeyKeyword(name).ValueSet(value)
		case enumspb.INDEXED_VALUE_TYPE_BOOL:
			var value bool
			if err := dc.FromPayload(p, &value); err != nil {
				return temporal.SearchAttributes{}, err
			}
			update = temporal.NewSearchAttributeKeyBool(name).ValueSet(value)
		case enumspb.INDEXED_VALUE_TYPE_INT:
			var value int64
			if err := dc.FromPayload(p, &value); err != nil {
				return temporal.SearchAttributes{}, err
			}
			update = temporal.NewSearchAttributeKeyInt64(name).ValueSet(value)
		case enumspb.INDEXED_VALUE_TYPE_DOUBLE:
			var value float64
			if err := dc.FromPayload(p, &value); err != nil {
				return temporal.SearchAttributes{}, err
			}
			update = temporal.NewSearchAttributeKeyFloat64(name).ValueSet(value)
		case enumspb.INDEXED_VALUE_TYPE_DATETIME:
			var value time.Time
			if err := dc.FromPayload(p, &value); err != nil {
				return temporal.SearchAttributes{}, err
			}
			update = temporal.NewSearchAttributeKeyTime(name).ValueSet(value)
		case enumspb.INDEXED_VALUE_TYPE_KEYWORD_LIST:
			var value []string
			if err := dc.FromPayload(p, &value); err != nil {
				return temporal.SearchAttributes{}, err
			}
			update = temporal.NewSearchAttributeKeyKeywordList(name).ValueSet(value)
		default:
			return temporal.SearchAttributes{}, fmt.Errorf("search attributes: unsupported type %v", kind)
		}
		updates = append(updates, update)
	}
	return temporal.NewSearchAttributes(updates...), nil
}

// Generated FromString helpers use process-global lookup maps. Parse this small
// trusted wire vocabulary locally; both forms are accepted by the upstream SDK.
func indexedValueType(name string) (enumspb.IndexedValueType, error) {
	switch name {
	case "Text", "INDEXED_VALUE_TYPE_TEXT":
		return enumspb.INDEXED_VALUE_TYPE_TEXT, nil
	case "Keyword", "INDEXED_VALUE_TYPE_KEYWORD":
		return enumspb.INDEXED_VALUE_TYPE_KEYWORD, nil
	case "Bool", "INDEXED_VALUE_TYPE_BOOL":
		return enumspb.INDEXED_VALUE_TYPE_BOOL, nil
	case "Int", "INDEXED_VALUE_TYPE_INT":
		return enumspb.INDEXED_VALUE_TYPE_INT, nil
	case "Double", "INDEXED_VALUE_TYPE_DOUBLE":
		return enumspb.INDEXED_VALUE_TYPE_DOUBLE, nil
	case "Datetime", "INDEXED_VALUE_TYPE_DATETIME":
		return enumspb.INDEXED_VALUE_TYPE_DATETIME, nil
	case "KeywordList", "INDEXED_VALUE_TYPE_KEYWORD_LIST":
		return enumspb.INDEXED_VALUE_TYPE_KEYWORD_LIST, nil
	default:
		return enumspb.INDEXED_VALUE_TYPE_UNSPECIFIED, fmt.Errorf("search attributes: unsupported type %q", name)
	}
}
