package worker

import (
	"context"
	"go.temporal.io/sdk/activity"
	"testing"
	"time"

	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/testsuite"
	goWorker "go.temporal.io/sdk/worker"
	goWorkflow "go.temporal.io/sdk/workflow"
)

type testWorker struct {
	goWorker.Worker
	env *testsuite.TestWorkflowEnvironment
}

func (w *testWorker) RegisterWorkflowWithOptions(fn any, options goWorkflow.RegisterOptions) {
	w.env.RegisterWorkflowWithOptions(fn, options)
}

func ordinary(_ goWorkflow.Context, input string) (string, error) {
	return "ordinary:" + input, nil
}

func TestOrdinaryWorkflowPreservesWorkerOptionsAndCustomConverter(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.SetDataConverter(converter.NewCompositeDataConverter(converter.NewJSONPayloadConverter()))
	w := Wrap(&testWorker{env: env})
	w.RegisterWorkflowWithOptions(ordinary, goWorkflow.RegisterOptions{Name: "custom-name"})
	env.ExecuteWorkflow("custom-name", "hello")
	if err := env.GetWorkflowError(); err != nil {
		t.Fatal(err)
	}
	var result string
	if err := env.GetWorkflowResult(&result); err != nil {
		t.Fatal(err)
	}
	if result != "ordinary:hello" {
		t.Fatalf("result = %q", result)
	}
}

func (w *testWorker) RegisterActivityWithOptions(fn any, options activity.RegisterOptions) {
	w.env.RegisterActivityWithOptions(fn, options)
}
func testActivity(_ context.Context, input int) (string, error) { return "host", nil }

type testActivities struct{}

func (*testActivities) Method(_ context.Context, input int) (string, error) {
	panic("must not execute while registering")
}

func TestActivityAliasesAndOrdinaryActivities(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	w := Wrap(&testWorker{env: env}).(*isolateWorker)
	// The resolver is captured before registration and must observe later aliases.
	resolve := w.activities.resolve
	w.RegisterActivityWithOptions(testActivity, activity.RegisterOptions{Name: "alias"})
	if got := resolve("testActivity"); got != "alias" {
		t.Fatalf("alias = %q", got)
	}
	if got := resolve("unregistered"); got != "unregistered" {
		t.Fatalf("default name = %q", got)
	}
	env.ExecuteWorkflow(func(ctx goWorkflow.Context) (string, error) {
		ctx = goWorkflow.WithActivityOptions(ctx, goWorkflow.ActivityOptions{StartToCloseTimeout: time.Second})
		var result string
		err := goWorkflow.ExecuteActivity(ctx, testActivity, 5).Get(ctx, &result)
		return result, err
	})
	if err := env.GetWorkflowError(); err != nil {
		t.Fatal(err)
	}
	var got string
	if err := env.GetWorkflowResult(&got); err != nil || got != "host" {
		t.Fatalf("ordinary activity result: %q, %v", got, err)
	}
	var receiver *testActivities
	w.RegisterActivityWithOptions(receiver.Method, activity.RegisterOptions{Name: "method-alias"})
	if got := resolve("Method"); got != "method-alias" {
		t.Fatalf("method alias = %q", got)
	}
}

func TestReplayActivityAliasesAndDisabledAliasing(t *testing.T) {
	for _, disabled := range []bool{false, true} {
		r, err := NewWorkflowReplayerWithOptions(WorkflowReplayerOptions{DisableRegistrationAliasing: disabled})
		if err != nil {
			t.Fatal(err)
		}
		r.RegisterActivityWithOptions(testActivity, activity.RegisterOptions{Name: "alias"})
		want := "alias"
		if disabled {
			want = "testActivity"
		}
		if got := r.(*isolateReplayer).activities.resolve("testActivity"); got != want {
			t.Fatalf("disabled=%t: got %q, want %q", disabled, got, want)
		}
	}
}

func TestRejectsActivityWithoutStandardContext(t *testing.T) {
	for _, fn := range []any{func(int) (string, error) { return "", nil }, func() {}, (*contextlessActivities)(nil)} {
		r := NewWorkflowReplayer()
		func() {
			defer func() {
				if recover() == nil {
					t.Errorf("accepted contextless activity %T", fn)
				}
			}()
			r.RegisterActivity(fn)
		}()
	}
}

type contextlessActivities struct{}

func (*contextlessActivities) Wrong(string) error { return nil }

func TestWorkflowAliasesTrackOrdinaryRegistrations(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	w := Wrap(&testWorker{env: env}).(*isolateWorker)
	resolve := w.workflows.resolve
	w.RegisterWorkflowWithOptions(ordinary, goWorkflow.RegisterOptions{Name: "ordinary-alias"})
	if resolve("ordinary") != "ordinary-alias" {
		t.Fatal("workflow registration alias not tracked")
	}
	if resolve("unregistered") != "unregistered" {
		t.Fatal("literal name changed")
	}
}

func TestReplayWorkflowAliasingCanBeDisabled(t *testing.T) {
	for _, disabled := range []bool{false, true} {
		r, err := NewWorkflowReplayerWithOptions(WorkflowReplayerOptions{DisableRegistrationAliasing: disabled})
		if err != nil {
			t.Fatal(err)
		}
		r.RegisterWorkflowWithOptions(ordinary, goWorkflow.RegisterOptions{Name: "ordinary-alias"})
		want := "ordinary-alias"
		if disabled {
			want = "ordinary"
		}
		if got := r.(*isolateReplayer).workflows.resolve("ordinary"); got != want {
			t.Fatalf("disabled=%t: got %q, want %q", disabled, got, want)
		}
	}
}
