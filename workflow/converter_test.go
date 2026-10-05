package workflow

import (
	"testing"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/sdk/converter"
	"google.golang.org/protobuf/proto"
)

func TestInstanceConverterMatchesPinnedDefault(t *testing.T) {
	type record struct {
		Name   string
		When   time.Time
		Values map[string]int
	}
	values := []any{
		nil, []byte(nil), []byte("binary"), "hello", 42,
		(*record)(nil),
		record{Name: "unicode: 世界", When: time.Date(2025, 1, 2, 3, 4, 5, 0, time.UTC), Values: map[string]int{"b": 2, "a": 1}},
		&commonpb.Payload{Metadata: map[string][]byte{"example": []byte("value")}, Data: []byte("data")},
	}
	host := converter.GetDefaultDataConverter()
	for index, value := range values {
		expected, err := host.ToPayload(value)
		if err != nil {
			t.Fatalf("case %d: host encoding: %v", index, err)
		}
		actual, err := instanceDataConverter.ToPayload(value)
		if err != nil {
			t.Fatalf("case %d: instance encoding: %v", index, err)
		}
		if !proto.Equal(expected, actual) {
			t.Errorf("case %d: payload differs from pinned default: actual=%v expected=%v", index, actual, expected)
		}
	}
	// Invalid payloads must retain the same error behavior as the worker.
	invalid := &commonpb.Payload{Metadata: map[string][]byte{converter.MetadataEncoding: []byte("unknown")}}
	var a, b any
	expected := host.FromPayload(invalid, &a)
	actual := instanceDataConverter.FromPayload(invalid, &b)
	if actual == nil || expected == nil || actual.Error() != expected.Error() {
		t.Fatalf("decode error: actual=%v expected=%v", actual, expected)
	}
}
