package main

import (
	"fmt"
	"reflect"
	"strings"
	"sync"
	"time"

	example "github.com/mfateev/sdk-go-poc/example/protobufvalues"
	"github.com/mfateev/sdk-go-poc/example/protobufvalues/model"
	"github.com/mfateev/sdk-go-poc/worker"
	"go.temporal.io/sdk/testsuite"
)

func cacheReuse() error {
	info := (&model.Record{}).ProtoReflect().Type()
	unused := (&model.Settings{}).ProtoReflect().Type()
	ready := func(value any) uint64 { return reflect.ValueOf(value).Elem().FieldByName("initDone").Uint() }
	if ready(info) != 0 || ready(unused) != 0 {
		return fmt.Errorf("fixture metadata initialized before first use")
	}
	identity := fmt.Sprintf("%p", info)
	run := func() error {
		var suite testsuite.WorkflowTestSuite
		env := suite.NewTestWorkflowEnvironment()
		w := worker.Wrap(&hostEnvironment{env: env})
		w.RegisterWorkflow(example.SharedMetadata)
		var queryError error
		env.RegisterDelayedCallback(func() {
			defer env.SignalWorkflow("finish", true)
			for _, name := range []string{"mutateLibrary", "mutateLibraryMap"} {
				if _, err := env.QueryWorkflow(name); err == nil || !strings.Contains(err.Error(), "read-only handler cannot mutate shared library") {
					queryError = fmt.Errorf("library mutation query %s: %v", name, err)
					return
				}
			}
			if _, err := env.QueryWorkflow("mutateMetadata"); err == nil || !strings.Contains(err.Error(), "read-only handler cannot mutate host state") {
				queryError = fmt.Errorf("metadata mutation query: %v", err)
				return
			}
			if _, err := env.QueryWorkflow("mutateRegistry"); err == nil || !strings.Contains(err.Error(), "unsupported metadata operation") {
				queryError = fmt.Errorf("registry mutation query: %v", err)
				return
			}
			value, err := env.QueryWorkflow("metadata")
			if err != nil {
				queryError = fmt.Errorf("query panic killed the isolate: %w", err)
				return
			}
			var result string
			if err := value.Get(&result); err != nil {
				queryError = err
				return
			}
			if result != identity {
				queryError = fmt.Errorf("query changed metadata identity")
			}
		}, time.Millisecond)
		env.ExecuteWorkflow("SharedMetadata")
		if queryError != nil {
			return queryError
		}
		if err := env.GetWorkflowError(); err != nil {
			return err
		}
		var result string
		if err := env.GetWorkflowResult(&result); err != nil {
			return err
		}
		if result != identity {
			return fmt.Errorf("isolate metadata %s differs from host %s", result, identity)
		}
		return nil
	}
	// Concurrent cold first use tests the existing initializer's publication
	// protocol as well as reuse of the canonical tables across isolates.
	var wg sync.WaitGroup
	errors := make(chan error, 16)
	for range 16 {
		wg.Go(func() { errors <- run() })
	}
	wg.Wait()
	close(errors)
	for err := range errors {
		if err != nil {
			return err
		}
	}
	if ready(info) != 1 {
		return fmt.Errorf("used metadata was not initialized in the host cache")
	}
	if ready(unused) != 0 {
		return fmt.Errorf("unused Settings metadata was eagerly initialized")
	}
	if err := run(); err != nil {
		return err
	}
	fmt.Println("shared protobuf metadata reused across isolates; unused types remain lazy")
	return nil
}
