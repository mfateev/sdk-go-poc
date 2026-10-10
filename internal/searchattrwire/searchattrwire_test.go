package searchattrwire

import (
	"reflect"
	"testing"
	"time"

	"github.com/mfateev/sdk-go-poc/internal/payloadwire"
	"go.temporal.io/sdk/temporal"
)

func TestTypedSearchAttributeWire(t *testing.T) {
	instant := time.Date(2026, 10, 10, 12, 34, 56, 123456789, time.FixedZone("explicit", 3600))
	attributes := temporal.NewSearchAttributes(
		temporal.NewSearchAttributeKeyString("text").ValueSet("hello"),
		temporal.NewSearchAttributeKeyKeyword("keyword").ValueSet("tag"),
		temporal.NewSearchAttributeKeyBool("bool").ValueSet(true),
		temporal.NewSearchAttributeKeyInt64("int").ValueSet(9007199254740993),
		temporal.NewSearchAttributeKeyFloat64("double").ValueSet(0.125),
		temporal.NewSearchAttributeKeyTime("time").ValueSet(instant),
		temporal.NewSearchAttributeKeyKeywordList("list").ValueSet([]string{"a", "b"}),
	)
	raw, err := Encode(attributes)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := Decode(raw)
	if err != nil {
		t.Fatal(err)
	}
	if value, ok := decoded.GetInt64(temporal.NewSearchAttributeKeyInt64("int")); !ok || value != 9007199254740993 {
		t.Fatal("integer precision lost")
	}
	if value, ok := decoded.GetTime(temporal.NewSearchAttributeKeyTime("time")); !ok || !value.Equal(instant) {
		t.Fatal("timestamp precision lost")
	}
	original := attributes.GetUntypedValues()
	actual := decoded.GetUntypedValues()
	delete(original, temporal.NewSearchAttributeKeyTime("time"))
	delete(actual, temporal.NewSearchAttributeKeyTime("time"))
	if !reflect.DeepEqual(original, actual) {
		t.Fatalf("typed values changed: %v", actual)
	}
}
func TestTypedSearchAttributeUnset(t *testing.T) {
	updates := temporal.NewSearchAttributes(temporal.NewSearchAttributeKeyInt64("int").ValueSet(12), temporal.NewSearchAttributeKeyInt64("int").ValueUnset(), temporal.NewSearchAttributeKeyKeywordList("list").ValueUnset())
	fields, err := EncodeFields(updates)
	if err != nil {
		t.Fatal(err)
	}
	if len(fields) != 2 {
		t.Fatal("unset entries were dropped")
	}
	for _, raw := range fields {
		p, err := payloadwire.Decode(raw)
		if err != nil {
			t.Fatal(err)
		}
		if p.Payloads[0].Data != nil || p.Payloads[0].Metadata["type"] != nil || string(p.Payloads[0].Metadata["encoding"]) != "binary/null" {
			t.Fatalf("invalid deletion encoding: %v", p)
		}
	}
}
