// Record a history with the versioned integration for rollback replay checks.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/mfateev/sdk-go-poc/example/determinism"
	"github.com/mfateev/sdk-go-poc/worker"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/sdk/client"
	"google.golang.org/protobuf/encoding/protojson"
)

func main() {
	address := flag.String("address", "localhost:7233", "Temporal server")
	output := flag.String("history", "example/compatibility/testdata/history-v1.json", "history destination")
	flag.Parse()
	if err := record(*address, *output); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func record(address, filename string) error {
	info := worker.GetIntegrationInfo()
	if info.Runtime.Metadata != 1 || info.Runtime.API != 1 || info.Runtime.Determinism != 1 || info.Protocol != 1 || info.SDKBindings != 1 || info.SDKModule != "github.com/mfateev/temporal-go-sdk" || info.SDKVersion != "v1.49.0-isolates.1" {
		return fmt.Errorf("unexpected integration identity: %+v", info)
	}
	encodedInfo, err := json.Marshal(info)
	if err != nil {
		return err
	}
	fmt.Println(string(encodedInfo))
	c, err := client.Dial(client.Options{HostPort: address})
	if err != nil {
		return err
	}
	defer c.Close()
	queue := fmt.Sprintf("isolate-contract-%d", time.Now().UnixNano())
	w := worker.New(c, queue, worker.Options{})
	w.RegisterWorkflow(determinism.DeterminismWorkflow)
	w.RegisterActivity(determinism.RecordTrace)
	if err := w.Start(); err != nil {
		return err
	}
	defer w.Stop()
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	run, err := c.ExecuteWorkflow(ctx, client.StartWorkflowOptions{ID: queue, TaskQueue: queue}, "DeterminismWorkflow", 8)
	if err != nil {
		return err
	}
	var result []string
	if err := run.Get(ctx, &result); err != nil {
		return err
	}
	history := &historypb.History{}
	iterator := c.GetWorkflowHistory(ctx, run.GetID(), run.GetRunID(), false, enumspb.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)
	for iterator.HasNext() {
		event, err := iterator.Next()
		if err != nil {
			return err
		}
		history.Events = append(history.Events, event)
	}
	raw, err := (protojson.MarshalOptions{Indent: "  "}).Marshal(history)
	if err != nil {
		return err
	}
	if err := os.WriteFile(filename, raw, 0644); err != nil {
		return err
	}
	fmt.Printf("recorded %d events and %d observations to %s\n", len(history.Events), len(result), filename)
	return nil
}
