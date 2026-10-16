// Package contextfree exercises SDK activity signatures with optional contexts.
package contextfree

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/mfateev/sdk-go-poc/workflow"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/temporal"
)

func NoInput() (int, error)            { return 11, nil }
func NoContext(value int) (int, error) { return value + 1, nil }
func WithContext(ctx context.Context, value int) (int, error) {
	if activity.GetInfo(ctx).ActivityType.Name == "" {
		return 0, fmt.Errorf("missing activity context")
	}
	return value + 2, nil
}
func TwoArgs(a, b int) (int, error) { return a*10 + b, nil }
func Pointer(value *int) (int, error) {
	if value == nil {
		return 13, nil
	}
	return *value, nil
}
func Large(value int64) (int, error) { return int(value), nil }
func ErrorOnly() error               { return nil }
func Failure() error {
	return temporal.NewNonRetryableApplicationError("context-free failure", "ContextFree", nil)
}

type Activities struct{ Offset int }

func (a *Activities) Add(value int) (int, error) { return a.Offset + value, nil }

type Call struct {
	Fn   any
	Args []any
}

func Calls() []Call {
	var a *Activities
	return []Call{
		{Fn: NoInput}, {Fn: NoContext, Args: []any{5}},
		{Fn: WithContext, Args: []any{5}}, {Fn: TwoArgs, Args: []any{2, 3}},
		{Fn: Pointer, Args: []any{nil}}, {Fn: Large, Args: []any{int64(1<<60 + 3)}},
		{Fn: a.Add, Args: []any{10}}, {Fn: "group.Add", Args: []any{10}},
	}
}
func Expected() []int { return []int{11, 6, 7, 23, 13, int(1<<60 + 3), 17, 19} }

type Registry interface {
	RegisterActivityWithOptions(any, activity.RegisterOptions)
}

func RegisterActivities(r Registry) {
	for _, fn := range []any{NoInput, NoContext, WithContext, TwoArgs, Pointer, Large, ErrorOnly, Failure} {
		r.RegisterActivityWithOptions(fn, activity.RegisterOptions{})
	}
	r.RegisterActivityWithOptions(NoContext, activity.RegisterOptions{Name: "no-context-alias", DisableAlreadyRegisteredCheck: true})
	r.RegisterActivityWithOptions((&Activities{Offset: 7}).Add, activity.RegisterOptions{Name: "bound-add"})
	r.RegisterActivityWithOptions(&Activities{Offset: 9}, activity.RegisterOptions{Name: "group."})
}

//temporal:workflow
func Workflow(ctx context.Context, local bool) ([]int, error) {
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{StartToCloseTimeout: time.Second * 10})
	ctx = workflow.WithLocalActivityOptions(ctx, workflow.LocalActivityOptions{StartToCloseTimeout: time.Second * 10})
	execute := func(fn any, args ...any) workflow.Future {
		if local {
			return workflow.ExecuteLocalActivity(ctx, fn, args...)
		}
		return workflow.ExecuteActivity(ctx, fn, args...)
	}
	var results []int
	for _, call := range Calls() {
		var result int
		if err := execute(call.Fn, call.Args...).Get(ctx, &result); err != nil {
			return nil, err
		}
		results = append(results, result)
	}
	if err := execute(ErrorOnly).Get(ctx, nil); err != nil {
		return nil, err
	}
	if err := execute(Failure).Get(ctx, nil); err == nil || !strings.Contains(err.Error(), "context-free failure") {
		return nil, fmt.Errorf("unexpected failure result: %v", err)
	}
	for _, call := range []Call{{Fn: NoInput, Args: []any{1}}, {Fn: NoContext}, {Fn: NoContext, Args: []any{"bad"}}, {Fn: WithContext, Args: []any{"bad"}}} {
		if err := execute(call.Fn, call.Args...).Get(ctx, nil); err == nil {
			return nil, fmt.Errorf("invalid arguments accepted")
		}
	}
	return results, nil
}
