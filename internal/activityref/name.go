// Package activityref identifies activity functions without executing them.
package activityref

import (
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
