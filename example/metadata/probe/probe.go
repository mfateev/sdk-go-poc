// Package probe prepares host-owned custom descriptors for metadata isolation tests.
package probe

import (
	"fmt"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/descriptorpb"
	"sync/atomic"
)

var callbackProbeCount atomic.Int32

// PrepareCallbackProbe runs on the host. Normal host registry visitors and
// custom descriptors must keep working; an isolate service must reject their
// callbacks before giving them process ownership.
func PrepareCallbackProbe() error {
	file, err := protodesc.NewFile(&descriptorpb.FileDescriptorProto{
		Name:        proto.String("isolate_metadata_callback_probe.proto"),
		Package:     proto.String("isolate_metadata_callback_probe"),
		Syntax:      proto.String("proto3"),
		MessageType: []*descriptorpb.DescriptorProto{{Name: proto.String("Record")}},
	}, nil)
	if err != nil {
		return err
	}
	if err := protoregistry.GlobalFiles.RegisterFile(callbackFile{file}); err != nil {
		return err
	}
	if callbackProbeCount.Load() == 0 {
		return fmt.Errorf("host custom descriptor callback did not run")
	}
	callbackProbeCount.Store(0)
	return nil
}

func CallbackProbeCount() int32 { return callbackProbeCount.Load() }

type callbackFile struct{ protoreflect.FileDescriptor }

func (f callbackFile) Messages() protoreflect.MessageDescriptors {
	return callbackMessages{f.FileDescriptor.Messages()}
}

type callbackMessages struct {
	protoreflect.MessageDescriptors
}

func (m callbackMessages) Get(i int) protoreflect.MessageDescriptor {
	return callbackMessage{m.MessageDescriptors.Get(i)}
}

type callbackMessage struct{ protoreflect.MessageDescriptor }

func (m callbackMessage) FullName() protoreflect.FullName {
	callbackProbeCount.Add(1)
	return m.MessageDescriptor.FullName()
}
