// Package metadata exercises trusted type services through the real SDK
// dispatcher. The driver runs it without a Temporal server.
package metadata

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"sync"

	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/runtime/protoimpl"
)

//go:isolate
func MetadataWorkflow(_ context.Context, mode string) (string, error) {
	if mode == "reject" {
		var callback bool
		if err := rejected("registry visitor", func() {
			protoregistry.GlobalTypes.RangeMessages(func(protoreflect.MessageType) bool { callback = true; return true })
		}); err != nil {
			return "", err
		}
		if callback {
			return "", fmt.Errorf("registry visitor ran inside a service")
		}
		if err := rejected("private registry", func() { _ = (&protoregistry.Types{}).NumMessages() }); err != nil {
			return "", err
		}
		if err := rejected("private message info", func() { _ = (&protoimpl.MessageInfo{}).Descriptor() }); err != nil {
			return "", err
		}
		if err := rejected("descriptor callback", func() {
			_, _ = protoregistry.GlobalFiles.FindDescriptorByName("isolate_metadata_callback_probe.Record")
		}); err != nil {
			return "", err
		}
		if err := rejected("registry mutation", func() {
			message, err := protoregistry.GlobalTypes.FindMessageByName("temporal.api.common.v1.Payload")
			if err != nil {
				panic(err)
			}
			_ = protoregistry.GlobalTypes.RegisterMessage(message)
		}); err != nil {
			return "", err
		}
		return "unsafe metadata operations rejected", nil
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

func rejected(name string, fn func()) (err error) {
	defer func() {
		if p := recover(); p != nil {
			if strings.Contains(fmt.Sprint(p), "unaudited metadata") || strings.Contains(fmt.Sprint(p), "requires a process-owned receiver") {
				err = nil
			} else {
				err = fmt.Errorf("%s: unexpected rejection: %v", name, p)
			}
		}
	}()
	err = fmt.Errorf("%s: unaudited operation was allowed", name)
	fn()
	return err
}

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
