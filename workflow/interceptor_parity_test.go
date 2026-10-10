package workflow

import (
	"reflect"
	"testing"

	sdkinterceptor "go.temporal.io/sdk/interceptor"
)

// Keep this gate tied to the pinned SDK rather than a second copy of our method
// list. Native concurrency/time and SideEffect APIs are deliberate exclusions.
func TestWorkflowInterceptorAPIParity(t *testing.T) {
	excluded := map[string]bool{"Go": true, "Await": true, "AwaitWithTimeout": true, "AwaitWithOptions": true, "NewTimer": true, "NewTimerWithOptions": true, "SideEffect": true, "SideEffectWithOptions": true, "MutableSideEffect": true, "MutableSideEffectWithOptions": true}
	for _, pair := range [][2]reflect.Type{
		{reflect.TypeFor[sdkinterceptor.WorkflowInboundInterceptor](), reflect.TypeFor[WorkflowInboundInterceptor]()},
		{reflect.TypeFor[sdkinterceptor.WorkflowOutboundInterceptor](), reflect.TypeFor[WorkflowOutboundInterceptor]()},
	} {
		for i := 0; i < pair[0].NumMethod(); i++ {
			method := pair[0].Method(i)
			if method.PkgPath != "" || excluded[method.Name] {
				continue
			}
			native, ok := pair[1].MethodByName(method.Name)
			if !ok {
				t.Errorf("missing hook %s", method.Name)
				continue
			}
			if method.Type.NumIn() != native.Type.NumIn() || method.Type.NumOut() != native.Type.NumOut() || method.Type.IsVariadic() != native.Type.IsVariadic() {
				t.Errorf("signature shape differs for %s", method.Name)
			}
		}
	}
}
