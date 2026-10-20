package temporalbridge

import (
	"encoding/binary"
	"isolate"
	"math"
	"testing"
)

func identifierPayload(id uint64) []byte { return binary.LittleEndian.AppendUint64(nil, id) }
func TestIdentifierFraming(t *testing.T) {
	for _, id := range []uint64{0, 1, 1<<53 + 1, math.MaxUint64} {
		got, err := commandID(&isolate.Command{Payload: identifierPayload(id)})
		if err != nil || got != id {
			t.Fatalf("%d: %d, %v", id, got, err)
		}
	}
	for _, raw := range [][]byte{nil, []byte("1"), make([]byte, 7), make([]byte, 9)} {
		if _, err := commandID(&isolate.Command{Payload: raw}); err == nil {
			t.Fatalf("accepted malformed identifier %x", raw)
		}
	}
}
