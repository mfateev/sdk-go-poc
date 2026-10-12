package workflow

import (
	"context"
	"testing"
)

func TestRandomStreamRequiresNativeContext(t *testing.T) {
	for _, ctx := range []context.Context{nil, context.Background()} {
		func() {
			defer func() {
				if recover() == nil {
					t.Error("random stream accepted an invalid execution context")
				}
			}()
			GetRandomStream(ctx, "name")
		}()
	}
}
