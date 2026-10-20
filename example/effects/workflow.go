// Package effects exercises worker-configured logging and fatal effect fences.
package effects

import (
	"context"
	"fmt"
	"log"
	"log/slog"
	"os"

	"github.com/mfateev/sdk-go-poc/workflow"
)

func init() { log.SetFlags(0); log.Print("effects initializer") }

// Application wire IDs remain stable and are shared with host registration.
const (
	TelemetryOp uint32 = 0x10000
	AuditOp     uint32 = 0x10001
)

//temporal:workflow
func EffectsWorkflow(_ context.Context, mode string) (string, error) {
	payload := []byte{0, 255, 'a'}
	workflow.NewSink(TelemetryOp).Emit(payload)
	payload[2] = 'b' // The host must receive the snapshot, not this mutation.
	workflow.NewSink(AuditOp).Emit(nil)
	fmt.Print("formatted")
	log.Print("standard")
	slog.Info("structured", "count", 7)
	println("builtin")
	if mode == "deny" {
		writeFile()
	} else if mode == "metadata" {
		mutateProcessState()
	}
	return "logged", nil
}

//temporal:workflow
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
	<-workflow.GetSignalChannel[[]byte](ctx, "finish")
	fmt.Print("final")
	workflow.NewSink(TelemetryOp).Emit([]byte("final"))
	return count, nil
}

func observe(message string) {
	workflow.NewSink(TelemetryOp).Emit([]byte(message))
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

func mutateProcessState() {
	defer func() {
		if recover() != nil {
			panic("process-state violation was recoverable")
		}
	}()
	os.Args[0] = "private mutation"
}
