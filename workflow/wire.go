//go:generate python3 ../internal/cmd/transportgen/generate.py

package workflow

import (
	"encoding/binary"
)

// Operations whose entire request is one identifier use a fixed eight-byte
// little-endian encoding rather than a JSON document. This is transport framing
// only: no durable payload, recorded command or history artifact is affected.
//
// OpSleep is deliberately excluded. It is the runtime's configured TimerOp, so
// its decimal nanosecond encoding is fixed by the isolate clock contract rather
// than chosen by this SDK.
func encodeID(id uint64) []byte {
	payload := make([]byte, 8)
	binary.LittleEndian.PutUint64(payload, id)
	return payload
}
