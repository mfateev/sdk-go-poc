package main

import (
	"context"
	"isolate"
	"log"
	"os"

	"github.com/mfateev/sdk-go-poc/temporalbridge"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
	goWorkflow "go.temporal.io/sdk/workflow"
)

func echo(_ context.Context, input []byte) ([]byte, error) { return input, nil }

// PlainEcho is a normal Temporal workflow. It shares the worker with isolate
// workflows and keeps the usual worker.RegisterWorkflow path.
func PlainEcho(_ goWorkflow.Context, input string) (string, error) {
	return "plain:" + input, nil
}

func main() {
	program, ok := isolate.LookupProgram("temporal-order")
	if !ok {
		log.Fatal("missing temporal-order isolate; build with -isolate-dir=./example/order")
	}
	signalProgram, ok := isolate.LookupProgram("temporal-signal")
	if !ok {
		log.Fatal("missing temporal-signal isolate; build with -isolate-dir=./example/signal")
	}
	clockProgram, ok := isolate.LookupProgram("temporal-clock")
	if !ok {
		log.Fatal("missing temporal-clock isolate; build with -isolate-dir=./example/clock")
	}
	address := os.Getenv("TEMPORAL_ADDRESS")
	if address == "" {
		address = "localhost:7233"
	}
	c, err := client.Dial(client.Options{HostPort: address})
	if err != nil {
		log.Fatal(err)
	}
	defer c.Close()
	w := worker.New(c, "isolate-poc", worker.Options{})
	temporalbridge.Register(w, "IsolateOrder", program)
	temporalbridge.Register(w, "IsolateEcho", program)
	temporalbridge.Register(w, "IsolateTypedEcho", program)
	temporalbridge.Register(w, "IsolateSignal", signalProgram)
	temporalbridge.Register(w, "IsolateClock", clockProgram)
	w.RegisterWorkflow(PlainEcho)
	w.RegisterActivityWithOptions(echo, activity.RegisterOptions{Name: "echo"})
	if err := w.Run(worker.InterruptCh()); err != nil {
		log.Fatal(err)
	}
}
