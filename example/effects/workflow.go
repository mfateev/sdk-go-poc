// Package effects exercises worker-configured logging and fatal effect fences.
package effects

import (
	"context"
	"fmt"
	"log"
	"log/slog"
	"os"

	"google.golang.org/protobuf/reflect/protoregistry"
)

func init() { log.SetFlags(0); log.Print("effects initializer") }

//go:isolate
func EffectsWorkflow(_ context.Context, mode string) (string, error) {
	fmt.Print("formatted")
	log.Print("standard")
	slog.Info("structured", "count", 7)
	println("builtin")
	if mode == "deny" {
		writeFile()
	} else if mode == "metadata" {
		mutateMetadata()
	}
	return "logged", nil
}
func writeFile() {
	defer func() {
		if recover() != nil {
			panic("effect was recoverable")
		}
	}()
	_ = os.WriteFile("effect-sentinel", []byte("changed"), 0600)
}

func mutateMetadata() {
	defer func() {
		if recover() != nil {
			panic("metadata violation was recoverable")
		}
	}()
	var registry protoregistry.Types
	_ = registry.RegisterMessage(nil)
}
