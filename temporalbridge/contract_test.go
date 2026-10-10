package temporalbridge

import (
	"encoding/binary"
	"isolate"
	"strings"
	"testing"

	"github.com/mfateev/sdk-go-poc/internal/integrationcontract"
)

func TestStartupMismatchFailsTaskBeforeDecoding(t *testing.T) {
	// A nil environment would panic if startup reached input conversion, workflow
	// metadata or SDK commands. Contract rejection must precede all those paths.
	for _, op := range []uint32{6, 7} {
		for _, version := range []uint32{0, 2} {
			d := &definition{}
			raw := integrationcontract.Pack(nil)
			binary.LittleEndian.PutUint32(raw[4:], version)
			func() {
				defer func() {
					failure, ok := recover().(*WorkflowTaskError)
					if !ok || !strings.Contains(failure.Error(), "incompatible protocol") {
						t.Fatalf("unexpected task failure: %v", failure)
					}
				}()
				_ = d.handle(&isolate.Command{Op: op, Payload: raw})
				t.Fatal("accepted incompatible startup")
			}()
		}
	}
}
