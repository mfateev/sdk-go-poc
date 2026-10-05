// Package worker keeps Temporal's worker API while routing functions marked
// //go:isolate through the isolate workflow factory. Ordinary workflow and
// activity registrations are passed to the Temporal Go SDK.
package worker

import (
	"fmt"
	"isolate"
	"strings"

	"github.com/mfateev/sdk-go-poc/temporalbridge"
	"go.temporal.io/sdk/client"
	goWorker "go.temporal.io/sdk/worker"
	goWorkflow "go.temporal.io/sdk/workflow"
)

type Worker = goWorker.Worker
type Options = goWorker.Options
type WorkflowReplayer = goWorker.WorkflowReplayer
type WorkflowReplayerOptions = goWorker.WorkflowReplayerOptions

// New constructs a normal Temporal worker with isolate-aware registration.
func New(c client.Client, taskQueue string, options Options) Worker {
	return Wrap(goWorker.New(c, taskQueue, options))
}

// Wrap adds isolate registration to an existing worker. All lifecycle,
// activity, dynamic workflow, and service methods retain their SDK behavior.
func Wrap(w Worker) Worker {
	if _, ok := w.(*isolateWorker); ok {
		return w
	}
	return &isolateWorker{Worker: w}
}

type isolateWorker struct{ Worker }

func (w *isolateWorker) RegisterWorkflow(fn any) {
	w.RegisterWorkflowWithOptions(fn, goWorkflow.RegisterOptions{})
}

func (w *isolateWorker) RegisterWorkflowWithOptions(fn any, options goWorkflow.RegisterOptions) {
	fn, options = registration(fn, options)
	w.Worker.RegisterWorkflowWithOptions(fn, options)
}

func registration(fn any, options goWorkflow.RegisterOptions) (any, goWorkflow.RegisterOptions) {
	handle, ok := isolate.LookupFunction(fn)
	if !ok {
		return fn, options
	}
	if options.Name == "" {
		name := handle.Name()
		options.Name = name[strings.LastIndex(name, ".")+1:]
	}
	return temporalbridge.Factory{Function: handle}, options
}

type isolateReplayer struct{ WorkflowReplayer }

func (r *isolateReplayer) RegisterWorkflow(fn any) {
	r.RegisterWorkflowWithOptions(fn, goWorkflow.RegisterOptions{})
}

func (r *isolateReplayer) RegisterWorkflowWithOptions(fn any, options goWorkflow.RegisterOptions) {
	fn, options = registration(fn, options)
	r.WorkflowReplayer.RegisterWorkflowWithOptions(fn, options)
}

// GetWorkflowResult preserves the pinned SDK replayer's result-inspection
// extension used by the deterministic clock replay check.
func (r *isolateReplayer) GetWorkflowResult(id string, result any) error {
	getter, ok := r.WorkflowReplayer.(interface{ GetWorkflowResult(string, any) error })
	if !ok {
		return fmt.Errorf("Temporal replayer does not expose its completion result")
	}
	return getter.GetWorkflowResult(id, result)
}

func NewWorkflowReplayer() WorkflowReplayer {
	return &isolateReplayer{WorkflowReplayer: goWorker.NewWorkflowReplayer()}
}

func NewWorkflowReplayerWithOptions(options WorkflowReplayerOptions) (WorkflowReplayer, error) {
	r, err := goWorker.NewWorkflowReplayerWithOptions(options)
	if err != nil {
		return nil, err
	}
	return &isolateReplayer{WorkflowReplayer: r}, nil
}

func InterruptCh() <-chan any { return goWorker.InterruptCh() }
