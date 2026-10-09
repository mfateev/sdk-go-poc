package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/mfateev/sdk-go-poc/example/apicoverage"
	"github.com/mfateev/sdk-go-poc/worker"
	"github.com/nexus-rpc/sdk-go/nexus"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	nexuspb "go.temporal.io/api/nexus/v1"
	"go.temporal.io/api/operatorservice/v1"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporalnexus"
	"google.golang.org/protobuf/encoding/protojson"
)

func register(w interface{ RegisterWorkflow(any) }) {
	w.RegisterWorkflow(apicoverage.Coverage)
	w.RegisterWorkflow(apicoverage.Target)
	w.RegisterWorkflow(apicoverage.NexusTarget)
}
func replay(history *historypb.History) error {
	r := worker.NewWorkflowReplayer()
	register(r)
	r.RegisterActivityWithOptions(apicoverage.Local, activity.RegisterOptions{Name: "LocalAlias"})
	if err := r.ReplayWorkflowHistory(nil, history); err != nil {
		return err
	}
	var result string
	if err := r.(interface{ GetWorkflowResult(string, any) error }).GetWorkflowResult("", &result); err != nil {
		return err
	}
	if result != "ok" {
		return fmt.Errorf("replay result %q", result)
	}
	return nil
}
func replayDirectory(dir string) error {
	files, err := filepath.Glob(filepath.Join(dir, "*.json"))
	if err != nil {
		return err
	}
	if len(files) == 0 {
		return fmt.Errorf("no histories in %s", dir)
	}
	for _, file := range files {
		raw, err := os.ReadFile(file)
		if err != nil {
			return err
		}
		h := new(historypb.History)
		if err = protojson.Unmarshal(raw, h); err != nil {
			return err
		}
		if err = replay(h); err != nil {
			return fmt.Errorf("%s: %w", file, err)
		}
	}
	fmt.Printf("replayed %d saved API histories and checked computed results\n", len(files))
	return nil
}
func runLive(address, dir string) error {
	c, err := client.Dial(client.Options{HostPort: address})
	if err != nil {
		return err
	}
	defer c.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	queue := fmt.Sprintf("isolate-api-%d", time.Now().UnixNano())
	endpoint := fmt.Sprintf("isolate-api-%d", time.Now().UnixNano())
	created, err := c.OperatorService().CreateNexusEndpoint(ctx, &operatorservice.CreateNexusEndpointRequest{Spec: &nexuspb.EndpointSpec{Name: endpoint, Target: &nexuspb.EndpointTarget{Variant: &nexuspb.EndpointTarget_Worker_{Worker: &nexuspb.EndpointTarget_Worker{Namespace: "default", TaskQueue: queue}}}}})
	if err != nil {
		return fmt.Errorf("create Nexus endpoint: %w", err)
	}
	defer c.OperatorService().DeleteNexusEndpoint(context.Background(), &operatorservice.DeleteNexusEndpointRequest{Id: created.Endpoint.Id, Version: created.Endpoint.Version})
	w := worker.New(c, queue, worker.Options{})
	register(w)
	w.RegisterActivityWithOptions(apicoverage.Local, activity.RegisterOptions{Name: "LocalAlias"})
	w.RegisterActivity(apicoverage.Identity)
	service := nexus.NewService("coverage")
	if err := service.Register(nexus.NewSyncOperation("echo", func(context.Context, string, nexus.StartOperationOptions) (int64, error) {
		return apicoverage.Precise, nil
	})); err != nil {
		return err
	}
	if err := service.Register(nexus.NewSyncOperation("failure", func(context.Context, string, nexus.StartOperationOptions) (int64, error) {
		return 0, nexus.NewOperationFailedErrorf("failed")
	})); err != nil {
		return err
	}
	if err := service.Register(temporalnexus.NewWorkflowRunOperation("async", apicoverage.NexusTarget, func(_ context.Context, _ string, o nexus.StartOperationOptions) (client.StartWorkflowOptions, error) {
		return client.StartWorkflowOptions{ID: "api-nexus-" + o.RequestID, TaskQueue: queue}, nil
	})); err != nil {
		return err
	}
	w.RegisterNexusService(service)
	if err := w.Start(); err != nil {
		return err
	}
	defer w.Stop()
	sessions := worker.New(c, queue, worker.Options{EnableSessionWorker: true, DisableWorkflowWorker: true})
	sessions.RegisterActivity(apicoverage.Identity)
	if err := sessions.Start(); err != nil {
		return err
	}
	defer sessions.Stop()
	if dir == "" {
		dir = "/tmp/isolate-api-histories"
	}
	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}
	for _, mode := range []string{"external", "external-failure", "local", "local-retry", "local-failure", "local-cancel", "session", "session-recreate", "nexus", "nexus-async", "nexus-failure", "nexus-cancel", "nexus-abandon", "nexus-try-cancel", "nexus-wait-requested", "session-failure"} {
		target := "missing"
		if mode == "external" {
			run, err := c.ExecuteWorkflow(ctx, client.StartWorkflowOptions{TaskQueue: queue}, apicoverage.Target)
			if err != nil {
				return err
			}
			target = run.GetID()
			defer c.TerminateWorkflow(context.Background(), target, "", "coverage cleanup")
		}
		run, err := c.ExecuteWorkflow(ctx, client.StartWorkflowOptions{TaskQueue: queue, WorkflowTaskTimeout: time.Second}, "Coverage", apicoverage.Input{Mode: mode, Target: target, Endpoint: endpoint})
		if err != nil {
			return err
		}
		var result string
		if mode == "session-failure" {
			for {
				value, err := c.QueryWorkflow(ctx, run.GetID(), run.GetRunID(), "session-ready")
				if err == nil {
					var ready bool
					if err = value.Get(&ready); err != nil {
						return err
					}
					if ready {
						break
					}
				}
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(20 * time.Millisecond):
				}
			}
			// Deliberately stop the session host while a separate workflow worker
			// stays alive, then verify heartbeat failure cancels native context.
			sessions.Stop()
		}
		if err = run.Get(ctx, &result); err != nil {
			return fmt.Errorf("live %s: %w", mode, err)
		}
		if result != "ok" {
			return fmt.Errorf("live %s result %q", mode, result)
		}
		h := new(historypb.History)
		it := c.GetWorkflowHistory(ctx, run.GetID(), run.GetRunID(), false, enumspb.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)
		for it.HasNext() {
			ev, err := it.Next()
			if err != nil {
				return err
			}
			h.Events = append(h.Events, ev)
		}
		if mode == "local-retry" {
			found := false
			for _, ev := range h.Events {
				if ev.GetTimerStartedEventAttributes() != nil {
					found = true
				}
			}
			if !found {
				return fmt.Errorf("local retry did not record durable backoff timer")
			}
		}
		raw, err := (protojson.MarshalOptions{Indent: "  "}).Marshal(h)
		if err != nil {
			return err
		}
		if err = os.WriteFile(filepath.Join(dir, mode+".json"), raw, 0644); err != nil {
			return err
		}
		if err = replay(h); err != nil {
			return fmt.Errorf("live replay %s: %w", mode, err)
		}
		fmt.Println("live and replay:", mode)
	}
	fmt.Println("live local activities, sessions, external operations and Nexus passed")
	return nil
}
