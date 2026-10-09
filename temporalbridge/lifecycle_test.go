package temporalbridge

import (
	"context"
	"encoding/json"
	"errors"
	"isolate"
	"runtime"
	"strings"
	"testing"
	"time"
	"weak"

	"github.com/mfateev/sdk-go-poc/workflow"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/sdk/converter"
	bindings "go.temporal.io/sdk/internalbindings"
)

func TestLifecycleFailuresDoNotCompleteExecution(t *testing.T) {
	for _, cause := range []error{
		&isolate.PanicError{Phase: "goroutine", Message: "child panic", Stack: "child stack"},
		&isolate.OwnershipError{Reason: "private receiver", Stack: "ownership stack"},
		&isolate.GoexitError{Phase: "main", Stack: "root stack"},
		&isolate.ExitError{Code: 42},
		isolate.ErrRevoked,
		&isolate.KillPendingError{LiveGoroutines: 1},
	} {
		env := new(effectEnvironment)
		d := &definition{env: env}
		func() {
			defer func() {
				failure, ok := recover().(*WorkflowTaskError)
				if !ok || !errors.Is(failure, cause) {
					t.Errorf("task failure did not preserve lifecycle cause: %v", failure)
				}
			}()
			d.fail(cause)
		}()
		if env.completes != 0 || !d.closed || !d.completed {
			t.Fatalf("lifecycle completed execution: completes=%d closed=%t completed=%t", env.completes, d.closed, d.completed)
		}
		if failure, ok := cause.(*isolate.PanicError); ok && d.StackTrace() != failure.Stack {
			t.Fatal("child panic stack was lost during close")
		}
		if failure, ok := cause.(*isolate.OwnershipError); ok && d.StackTrace() != failure.Stack {
			t.Fatal("ownership fault stack was lost during close")
		}
	}
}

type lifecycleEnvironment struct {
	activityEnvironment
	cancelHandler func()
	signalHandler func(string, *commonpb.Payloads, *commonpb.Header) error
}

func (e *lifecycleEnvironment) RegisterCancelHandler(fn func()) { e.cancelHandler = fn }
func (e *lifecycleEnvironment) RegisterSignalHandler(fn func(string, *commonpb.Payloads, *commonpb.Header) error) {
	e.signalHandler = fn
}
func (e *lifecycleEnvironment) GetDataConverter() converter.DataConverter {
	return converter.GetDefaultDataConverter()
}

//go:noinline
func lifecycleClosedCallback(t *testing.T) (*definition, *lifecycleEnvironment, weak.Pointer[isolate.Command]) {
	t.Helper()
	env := new(lifecycleEnvironment)
	d := new(definition)
	d.Execute(env, nil, nil)
	payload, _ := json.Marshal(workflow.ActivityPayloadRequest{Name: "Foo", StartToCloseTimeout: time.Minute})
	command := &isolate.Command{Op: workflow.OpActivityPayloads, Payload: payload}
	if err := d.handle(command); err != nil {
		t.Fatal(err)
	}
	reference := weak.Make(command)
	state := d.callsByCommand[command]
	// Closing a cached definition must detach callbacks without sending Temporal
	// cancellation commands: another worker will recreate it from history.
	state.cancel = func() { env.cancels++ }
	d.pending = []reply{{command: command, payload: make([]byte, 1<<20)}}
	d.wantSignals = []signalWaiter{{name: "signal", command: command}}
	d.cancelWaiter = command
	d.Close()
	if !state.done || state.command != nil || state.cancel != nil || env.cancels != 0 {
		t.Fatal("close retained command state or canceled server-side work")
	}
	return d, env, reference
}

func TestLifecycleCloseRetiresLateCallbacks(t *testing.T) {
	d, env, reference := lifecycleClosedCallback(t)
	for range 5 {
		runtime.GC()
		if reference.Value() == nil {
			break
		}
		runtime.Gosched()
	}
	if reference.Value() != nil {
		t.Fatal("retained SDK activity callback kept its command alive")
	}
	env.callback(nil, nil)
	env.cancelHandler()
	if err := env.signalHandler("late", nil, nil); err != nil {
		t.Fatal(err)
	}
	d.OnWorkflowTaskStarted(time.Second)
	d.Close()
	if !d.closed || d.env != nil || d.instance != nil || len(d.pending) != 0 || len(d.immediate) != 0 || len(d.signals) != 0 || len(d.wantSignals) != 0 || len(d.callsByCommand) != 0 {
		t.Fatal("late callbacks revived closed workflow state")
	}
	if env.cancels != 0 || d.CloseError() != nil {
		t.Fatal("closed definition canceled server work or reported spurious pending cleanup")
	}
}

func TestLifecycleTaskFailurePreservesStartupCause(t *testing.T) {
	env := new(effectEnvironment)
	d := &definition{env: env}
	cause := &isolate.InitializationError{Cause: context.DeadlineExceeded, Pending: &isolate.KillPendingError{LiveGoroutines: 1, Stack: "initializer stack"}}
	func() {
		defer func() {
			failure, ok := recover().(*WorkflowTaskError)
			if !ok || !errors.Is(failure, context.DeadlineExceeded) || !strings.Contains(failure.Error(), "pending cleanup") {
				t.Errorf("startup task failure = %v", failure)
			}
		}()
		d.failTask(cause)
	}()
	if env.completes != 0 || d.StackTrace() != cause.Pending.Stack {
		t.Fatal("pending initialization became an execution failure or lost diagnostics")
	}
}

func (*lifecycleEnvironment) RegisterUpdateHandler(func(string, string, *commonpb.Payloads, *commonpb.Header, bindings.UpdateCallbacks)) {
}
