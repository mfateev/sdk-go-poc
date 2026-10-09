package workflow

import (
	"fmt"
	"testing"
)

func TestSinkRejectsReservedOperations(t *testing.T) {
	for _, op := range []uint32{0, OpActivity, 0xffff, 0xffff0000, 0xffff0001, 0xffffffff} {
		t.Run(fmt.Sprintf("%#x", op), func(t *testing.T) {
			defer func() {
				if recover() == nil {
					t.Errorf("accepted reserved operation %#x", op)
				}
			}()
			NewSink(op)
		})
	}
	for _, op := range []uint32{0x10000, 0xfffeffff} {
		if NewSink(op).op != op {
			t.Fatal("sink changed the operation code")
		}
	}
}

func TestZeroSinkCannotEmit(t *testing.T) {
	defer func() {
		if got := recover(); got != "workflow: uninitialized sink" {
			t.Fatalf("zero sink panic = %v", got)
		}
	}()
	var sink Sink
	sink.Emit(nil)
}
