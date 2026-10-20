package temporalbridge

import (
	"encoding/binary"
	"errors"
	"isolate"
)

// commandID decodes an identifier-only request payload. See workflow/wire.go
// for the encoding and for why OpSleep keeps its decimal form.
func commandID(command *isolate.Command) (uint64, error) {
	if len(command.Payload) != 8 {
		return 0, errors.New("malformed identifier payload")
	}
	return binary.LittleEndian.Uint64(command.Payload), nil
}
