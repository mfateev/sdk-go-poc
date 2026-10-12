package main

import (
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/testsuite"
	"google.golang.org/protobuf/proto"
	"testing"
)

// These upstream cases use a host activity test environment. Retain that
// coverage, and exercise the same activities through native workflows above.
func TestUpstreamActivityWithProtoPayload(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestActivityEnvironment()
	env.RegisterActivity(Payloads)
	first := &commonpb.Payloads{Payloads: []*commonpb.Payload{{Metadata: map[string][]byte{"encoding": []byte("someencoding")}, Data: []byte("input1")}}}
	second := &commonpb.Payloads{Payloads: []*commonpb.Payload{{Data: []byte("input2")}}}
	value, err := env.ExecuteActivity(Payloads, first, second)
	if err != nil {
		t.Fatal(err)
	}
	var out commonpb.Payloads
	if err := value.Get(&out); err != nil {
		t.Fatal(err)
	}
	if !proto.Equal(&out, &commonpb.Payloads{Payloads: []*commonpb.Payload{{Data: []byte("result")}}}) {
		t.Fatalf("payload result: %v", &out)
	}
}
func TestUpstreamActivityWithRandomProto(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestActivityEnvironment()
	env.RegisterActivity(Random)
	value, err := env.ExecuteActivity(Random, &commonpb.WorkflowType{Name: "input1"}, &commonpb.DataBlob{EncodingType: enumspb.ENCODING_TYPE_PROTO3})
	if err != nil {
		t.Fatal(err)
	}
	var out *commonpb.WorkflowType
	if err := value.Get(&out); err != nil {
		t.Fatal(err)
	}
	if !proto.Equal(out, &commonpb.WorkflowType{Name: "result"}) {
		t.Fatalf("random protobuf result: %v", out)
	}
}
