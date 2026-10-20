package protobufcopy_test

import (
	"bytes"
	"testing"

	"github.com/mfateev/sdk-go-poc/example/protobufvalues/model"
	"github.com/mfateev/sdk-go-poc/protobufcopy"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/gofeaturespb"
)

func TestSnapshotPreservesMessageDataWithoutAliasing(t *testing.T) {
	source := &model.Record{Sequence: 9007199254740993, Data: []byte{0, 255}, Entries: map[string]*model.Record{"nested": {Sequence: 42}}, Choice: &model.Record_Child{Child: &model.Record{Sequence: 17}}}
	source.ProtoReflect().SetUnknown([]byte{0xa0, 0x06, 0x01})
	// Populate generated coder and size caches before snapshotting.
	before, err := proto.Marshal(source)
	if err != nil {
		t.Fatal(err)
	}
	snapshot := protobufcopy.Clone(source)
	if !proto.Equal(source, snapshot) {
		t.Fatal("snapshot changed protobuf data")
	}
	snapshot.Data[0] = 7
	snapshot.Entries["nested"].Sequence = 99
	snapshot.GetChild().Sequence = 100
	snapshot.ProtoReflect().SetUnknown(nil)
	after, err := proto.Marshal(source)
	if err != nil || !bytes.Equal(before, after) {
		t.Fatalf("snapshot mutation reached source: %v", err)
	}
}
func TestSnapshotPreservesExtensions(t *testing.T) {
	source := new(descriptorpb.FeatureSet)
	proto.SetExtension(source, gofeaturespb.E_Go, &gofeaturespb.GoFeatures{ApiLevel: gofeaturespb.GoFeatures_API_OPEN.Enum()})
	snapshot := protobufcopy.Clone(source)
	if !proto.Equal(source, snapshot) {
		t.Fatal("extension lost")
	}
	proto.GetExtension(snapshot, gofeaturespb.E_Go).(*gofeaturespb.GoFeatures).ApiLevel = gofeaturespb.GoFeatures_API_OPAQUE.Enum()
	if proto.GetExtension(source, gofeaturespb.E_Go).(*gofeaturespb.GoFeatures).GetApiLevel() != gofeaturespb.GoFeatures_API_OPEN {
		t.Fatal("extension data aliases source")
	}
}
