// Package activityref identifies activity functions without executing them.
package activityref

import (
	"context"
	"fmt"
	"reflect"
	"runtime"
	"strings"
)

// Name follows the pinned Temporal SDK's function-name convention, including
// bound methods (a nil receiver is allowed). Registration aliases live on host.
func Name(fn any) (string, error) {
	value := reflect.ValueOf(fn)
	if !value.IsValid() || value.Kind() != reflect.Func || value.IsNil() {
		return "", fmt.Errorf("workflow: activity must be a non-nil function")
	}
	function := runtime.FuncForPC(value.Pointer())
	if function == nil {
		return "", fmt.Errorf("workflow: activity function has no runtime name")
	}
	name := function.Name()
	return strings.TrimSuffix(name[strings.LastIndex(name, ".")+1:], "-fm"), nil
}

// InputOffset follows sdk-go internal.isActivityContext and skips an optional
// activity context. It accepts context implementations, as the SDK does.
func InputOffset(typ reflect.Type) int {
	if typ.NumIn() > 0 && typ.In(0).Implements(reflect.TypeFor[context.Context]()) {
		return 1
	}
	return 0
}

// ValidateArgs adapts sdk-go internal.validateFunctionArgs for activity function
// references. The caller has already checked that fn is a non-nil function.
// Adapted from Temporal SDK v1.50.0 internal/internal_activity.go (MIT; see LICENSE).
func ValidateArgs(fn any, args []any) error {
	typ := reflect.TypeOf(fn)
	offset := InputOffset(typ)
	if typ.NumIn()-offset != len(args) {
		name, _ := Name(fn)
		return fmt.Errorf("expected %d args for function: %v but found %v", typ.NumIn()-offset, name, len(args))
	}
	for i, arg := range args {
		target := typ.In(i + offset)
		argType := reflect.TypeOf(arg)
		if argType != nil && !argType.AssignableTo(target) {
			return fmt.Errorf("cannot assign function argument: %d from type: %s to type: %s", i+offset+1, argType, target)
		}
	}
	return nil
}
