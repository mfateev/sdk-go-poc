package main

import (
	"context"
	"log"
	"os"

	"github.com/mfateev/sdk-go-poc/example/clock"
	"github.com/mfateev/sdk-go-poc/example/concurrent"
	"github.com/mfateev/sdk-go-poc/example/order"
	"github.com/mfateev/sdk-go-poc/example/signal"
	"github.com/mfateev/sdk-go-poc/worker"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	goWorkflow "go.temporal.io/sdk/workflow"
)

func echo(_ context.Context, input []byte) ([]byte, error) { return input, nil }

// PlainEcho is a normal Temporal workflow. It shares the worker with isolate
// workflows and keeps the usual worker.RegisterWorkflow path.
func PlainEcho(_ goWorkflow.Context, input string) (string, error) {
	return "plain:" + input, nil
}

func main() {
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
	w.RegisterWorkflowWithOptions(order.OrderWorkflow, goWorkflow.RegisterOptions{Name: "IsolateOrder"})
	w.RegisterWorkflowWithOptions(order.EchoWorkflow, goWorkflow.RegisterOptions{Name: "IsolateEcho"})
	w.RegisterWorkflowWithOptions(order.TypedEchoWorkflow, goWorkflow.RegisterOptions{Name: "IsolateTypedEcho"})
	w.RegisterWorkflowWithOptions(signal.SignalWorkflow, goWorkflow.RegisterOptions{Name: "IsolateSignal"})
	w.RegisterWorkflowWithOptions(clock.ClockWorkflow, goWorkflow.RegisterOptions{Name: "IsolateClock"})
	w.RegisterWorkflowWithOptions(concurrent.ConcurrentWorkflow, goWorkflow.RegisterOptions{Name: "IsolateConcurrent"})
	w.RegisterWorkflow(PlainEcho)
	w.RegisterActivityWithOptions(echo, activity.RegisterOptions{Name: "echo"})
	if err := w.Run(worker.InterruptCh()); err != nil {
		log.Fatal(err)
	}
}
