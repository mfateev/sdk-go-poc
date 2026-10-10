package workflow

import (
	"errors"
	"strings"
	"testing"

	"github.com/mfateev/sdk-go-poc/internal/payloadwire"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/sdk/converter"
)

type rejectingMemoConverter struct {
	converter.DataConverter
	calls int
}

func (c *rejectingMemoConverter) ToPayload(any) (*commonpb.Payload, error) {
	c.calls++
	return nil, errors.New("configured converter failure")
}
func TestMemoEncodingPolicyAndFallback(t *testing.T) {
	userDC := &rejectingMemoConverter{DataConverter: newDefaultDataConverter()}
	for _, useUser := range []bool{false, true} {
		userDC.calls = 0
		fields, err := encodeMemoFields(map[string]any{"number": int64(9007199254740993)}, userDC, useUser)
		if err != nil {
			t.Fatal(err)
		}
		p, err := payloadwire.Decode(fields["number"])
		if err != nil {
			t.Fatal(err)
		}
		var number int64
		if err := newDefaultDataConverter().FromPayloads(p, &number); err != nil || number != 9007199254740993 {
			t.Fatal("memo fallback lost precision")
		}
		wantCalls := 0
		if useUser {
			wantCalls = 1
		}
		if userDC.calls != wantCalls {
			t.Fatal("memo replay encoding policy ignored")
		}
	}
	if _, err := encodeMemoFields(map[string]any{"unsupported": func() {}}, userDC, true); err == nil || !strings.Contains(err.Error(), "configured converter failure") {
		t.Fatalf("original converter error lost: %v", err)
	}
}
func TestRawSearchAttributePayloadNotReserialized(t *testing.T) {
	raw := &commonpb.Payload{Metadata: map[string][]byte{"encoding": []byte("json/plain"), "type": []byte("Int")}, Data: []byte("9007199254740993")}
	fields, err := encodeSearchAttributeValues(map[string]any{"number": raw})
	if err != nil {
		t.Fatal(err)
	}
	p, err := payloadwire.Decode(fields["number"])
	if err != nil {
		t.Fatal(err)
	}
	if string(p.Payloads[0].Data) != string(raw.Data) || string(p.Payloads[0].Metadata["type"]) != "Int" {
		t.Fatal("raw search attribute changed")
	}
}
