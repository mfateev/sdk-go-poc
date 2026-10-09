// Package effects exercises worker-configured logging and fatal effect fences.
package effects

import (
	"context"
	"fmt"
	"log"
	"log/slog"
	"os"

	"github.com/mfateev/sdk-go-poc/workflow"
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

//go:isolate
func ReadOnlyLoggingWorkflow(ctx context.Context) (int, error) {
	count := 0
	if err := workflow.SetQueryHandler(ctx, "logs", func() (int, error) {
		observe("query")
		return count, nil
	}); err != nil {
		return 0, err
	}
	if err := workflow.SetUpdateHandlerWithOptions(ctx, "bump", func(context.Context) error {
		count++
		return nil
	}, workflow.UpdateHandlerOptions{Validator: func(context.Context) error {
		observe("validator")
		return nil
	}}); err != nil {
		return 0, err
	}
	<-workflow.GetSignalChannel(ctx, "finish")
	fmt.Print("final")
	return count, nil
}

func observe(message string) {
	fmt.Print(message)
	log.Print(message)
	slog.Info(message)
	println(message)
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
