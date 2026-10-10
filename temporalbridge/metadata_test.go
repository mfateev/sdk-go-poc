package temporalbridge

import (
	"encoding/json"
	"errors"
	"reflect"
	"testing"
	"unsafe"

	"github.com/mfateev/sdk-go-poc/internal/failurecodec"
	"github.com/mfateev/sdk-go-poc/internal/payloadwire"
	"github.com/mfateev/sdk-go-poc/workflow"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/temporal"
	goWorkflow "go.temporal.io/sdk/workflow"
)

type metadataEnvironment struct {
	activityEnvironment
	info goWorkflow.Info
	dc   converter.DataConverter
}

func (e *metadataEnvironment) WorkflowInfo() *goWorkflow.Info            { return &e.info }
func (e *metadataEnvironment) GetDataConverter() converter.DataConverter { return e.dc }
func setInfoField(t *testing.T, info *goWorkflow.Info, name string, value any) {
	t.Helper()
	field := reflect.ValueOf(info).Elem().FieldByName(name)
	if !field.IsValid() {
		t.Fatalf("SDK field %s missing", name)
	}
	reflect.NewAt(field.Type(), unsafe.Pointer(field.UnsafeAddr())).Elem().Set(reflect.ValueOf(value))
}
func TestPreviousRunSnapshotsPreservePresenceAndDecodeCodecs(t *testing.T) {
	defaultDC := converter.GetDefaultDataConverter()
	dc := converter.NewCodecDataConverter(defaultDC, converter.NewZlibCodec(converter.ZlibCodecOptions{AlwaysEncode: true}))
	env := &metadataEnvironment{dc: dc}
	d := &definition{env: env}
	check := func(wantPresent bool) *commonpb.Payloads {
		t.Helper()
		raw, err := d.readMetadata(workflow.OpLastCompletionResult)
		if err != nil {
			t.Fatal(err)
		}
		var snapshot workflow.LastCompletionSnapshot
		if err := json.Unmarshal(raw, &snapshot); err != nil {
			t.Fatal(err)
		}
		if snapshot.Present != wantPresent {
			t.Fatalf("presence=%v", snapshot.Present)
		}
		p, err := payloadwire.Decode(snapshot.Payloads)
		if err != nil {
			t.Fatal(err)
		}
		return p
	}
	check(false)
	setInfoField(t, &env.info, "lastCompletionResult", &commonpb.Payloads{})
	if p := check(true); len(p.Payloads) != 0 {
		t.Fatal("empty payloads gained data")
	}
	previous, err := dc.ToPayloads(int64(9007199254740993), "previous")
	if err != nil {
		t.Fatal(err)
	}
	setInfoField(t, &env.info, "lastCompletionResult", previous)
	var number int64
	var text string
	if err := defaultDC.FromPayloads(check(true), &number, &text); err != nil || number != 9007199254740993 || text != "previous" {
		t.Fatalf("previous result: %d %q %v", number, text, err)
	}
	if raw, err := d.readMetadata(workflow.OpLastError); err != nil || len(raw) != 0 {
		t.Fatal("absent failure changed")
	}
	failure := temporal.NewDefaultFailureConverter(temporal.DefaultFailureConverterOptions{DataConverter: dc, EncodeCommonAttributes: true}).ErrorToFailure(temporal.NewNonRetryableApplicationError("secret", "Previous", nil, int64(9007199254740993)))
	setInfoField(t, &env.info, "lastFailure", failure)
	raw, err := d.readMetadata(workflow.OpLastError)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := failurecodec.Decode(raw, defaultDC)
	if err != nil {
		t.Fatal(err)
	}
	var application *temporal.ApplicationError
	if !errors.As(decoded, &application) || application.Type() != "Previous" || !application.NonRetryable() || application.Message() != "secret" {
		t.Fatalf("failure=%v", decoded)
	}
	if err := application.Details(&number); err != nil || number != 9007199254740993 {
		t.Fatalf("details=%d %v", number, err)
	}
}
