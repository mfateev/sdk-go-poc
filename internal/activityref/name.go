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

// ValidateContext enforces the POC's activity context contract at registration.
func ValidateContext(fn any) error {
	typ := reflect.TypeOf(fn)
	if typ == nil {
		return fmt.Errorf("activity must be a non-nil function or pointer to struct")
	}
	validate := func(typ reflect.Type, receiver int) error {
		if typ.NumIn() <= receiver || typ.In(receiver) != reflect.TypeFor[context.Context]() {
			return fmt.Errorf("activity must take context.Context first")
		}
		return nil
	}
	if typ.Kind() == reflect.Func {
		return validate(typ, 0)
	}
	if typ.Kind() == reflect.Pointer && typ.Elem().Kind() == reflect.Struct {
		for i := 0; i < typ.NumMethod(); i++ {
			method := typ.Method(i)
			if method.PkgPath == "" {
				if err := validate(method.Type, 1); err != nil {
					return fmt.Errorf("%s: %w", method.Name, err)
				}
			}
		}
	}
	return nil // SDK validates other registration forms.
}
