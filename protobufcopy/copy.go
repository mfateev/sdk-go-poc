// Package protobufcopy snapshots message values without retaining generated
// reflection caches from the source execution domain. Ordinary proto.Clone calls
// MessageInfo.New, which binds new messages to the current package state and
// cannot merge cached messages from a different (workflow or query) descriptor
// graph. No object or callback is transferred to host execution.
package protobufcopy

import (
	"reflect"
	"unsafe"

	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
)

// Message is the protobuf reflection contract, without importing proto itself.
// This keeps the read-only Clone hook acyclic.
type Message interface{ ProtoReflect() protoreflect.Message }

type identity struct {
	typ reflect.Type
	ptr uintptr
}
type copier struct{ seen map[identity]reflect.Value }

// Clone copies data, unknown fields and extensions, leaving generated state and
// size caches empty. Generated ProtoReflect methods then use the current domain's
// MessageInfo. Ownership instrumentation remains enabled for every memory access.
func Clone[T Message](message T) T {
	if any(message) == nil {
		return message
	}
	c := copier{seen: make(map[identity]reflect.Value)}
	return c.value(reflect.ValueOf(message)).Interface().(T)
}

// addressable removes reflection's unexported-field restriction for this
// implementation's data copy. It grants no memory ownership permission: source
// reads and destination writes still pass the runtime's ordinary checks.
func addressable(v reflect.Value) reflect.Value {
	if v.CanInterface() {
		return v
	}
	return reflect.NewAt(v.Type(), unsafe.Pointer(v.UnsafeAddr())).Elem()
}
func (c *copier) value(v reflect.Value) reflect.Value {
	v = addressable(v)
	if (v.Kind() == reflect.Pointer || v.Kind() == reflect.Interface || v.Kind() == reflect.Map || v.Kind() == reflect.Slice) && v.IsNil() {
		return reflect.Zero(v.Type())
	}
	if v.Type() == reflect.TypeFor[reflect.Value]() {
		inner := v.Interface().(reflect.Value)
		if !inner.IsValid() {
			return v
		}
		return reflect.ValueOf(c.value(inner))
	}
	if v.Type() == reflect.TypeFor[protoreflect.Value]() {
		value := v.Interface().(protoreflect.Value)
		if !value.IsValid() {
			return v
		}
		if message, ok := value.Interface().(protoreflect.Message); ok {
			copied := c.value(reflect.ValueOf(message.Interface())).Interface().(Message)
			return reflect.ValueOf(protoreflect.ValueOfMessage(copied.ProtoReflect()))
		}
		copied := c.value(reflect.ValueOf(value.Interface())).Interface()
		return reflect.ValueOf(protoreflect.ValueOf(copied))
	}
	if v.CanInterface() {
		if descriptor, ok := v.Interface().(protoreflect.Descriptor); ok {
			if local, err := protoregistry.GlobalFiles.FindDescriptorByName(descriptor.FullName()); err == nil {
				candidate := reflect.ValueOf(local)
				if candidate.Type().AssignableTo(v.Type()) {
					return candidate
				}
			}
			return v
		}
		if extension, ok := v.Interface().(protoreflect.ExtensionType); ok {
			if local, err := protoregistry.GlobalTypes.FindExtensionByName(extension.TypeDescriptor().FullName()); err == nil {
				candidate := reflect.ValueOf(local)
				if candidate.Type().AssignableTo(v.Type()) {
					return candidate
				}
			}
			return v
		}
		if _, ok := v.Interface().(reflect.Type); ok {
			return v
		}
	}
	switch v.Kind() {
	case reflect.Pointer:
		if v.IsNil() {
			return reflect.Zero(v.Type())
		}
		key := identity{v.Type(), v.Pointer()}
		if old, ok := c.seen[key]; ok {
			return old
		}
		out := reflect.New(v.Type().Elem())
		c.seen[key] = out
		out.Elem().Set(c.value(v.Elem()))
		return out
	case reflect.Interface:
		if v.IsNil() {
			return reflect.Zero(v.Type())
		}
		out := reflect.New(v.Type()).Elem()
		out.Set(c.value(v.Elem()))
		return out
	case reflect.Struct:
		// A copy makes every source field addressable, including values supplied
		// through an interface. Its protobuf caches are cleared before it is used.
		source := v
		if !source.CanAddr() {
			source = reflect.New(v.Type()).Elem()
			source.Set(v)
		}
		out := reflect.New(v.Type()).Elem()
		generated := false
		for i := 0; i < v.NumField(); i++ {
			if messageState(v.Type().Field(i).Type) {
				generated = true
				break
			}
		}
		for i := 0; i < v.NumField(); i++ {
			field := v.Type().Field(i)
			if generated && (messageState(field.Type) || field.Name == "sizeCache") {
				continue
			}
			addressable(out.Field(i)).Set(c.value(source.Field(i)))
		}
		return out
	case reflect.Slice:
		if v.IsNil() {
			return reflect.Zero(v.Type())
		}
		out := reflect.MakeSlice(v.Type(), v.Len(), v.Len())
		if v.Type().Elem().Kind() == reflect.Uint8 {
			reflect.Copy(out, v)
			return out
		}
		for i := 0; i < v.Len(); i++ {
			out.Index(i).Set(c.value(v.Index(i)))
		}
		return out
	case reflect.Array:
		out := reflect.New(v.Type()).Elem()
		for i := 0; i < v.Len(); i++ {
			out.Index(i).Set(c.value(v.Index(i)))
		}
		return out
	case reflect.Map:
		if v.IsNil() {
			return reflect.Zero(v.Type())
		}
		key := identity{v.Type(), v.Pointer()}
		if old, ok := c.seen[key]; ok {
			return old
		}
		out := reflect.MakeMapWithSize(v.Type(), v.Len())
		c.seen[key] = out
		iter := v.MapRange()
		for iter.Next() {
			out.SetMapIndex(c.value(iter.Key()), c.value(iter.Value()))
		}
		return out
	default:
		return v
	}
}

func messageState(t reflect.Type) bool {
	return t.PkgPath() == "google.golang.org/protobuf/internal/impl" && t.Name() == "MessageState"
}
