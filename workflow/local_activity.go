package workflow

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"isolate"
	"reflect"
	"slices"
	"time"

	"github.com/mfateev/sdk-go-poc/internal/activityref"
	goWorkflow "go.temporal.io/sdk/workflow"
)

type LocalActivityOptions = goWorkflow.LocalActivityOptions
type localOptionsKey struct{}

func cloneLocalOptions(o LocalActivityOptions) LocalActivityOptions {
	if o.RetryPolicy != nil {
		p := *o.RetryPolicy
		p.NonRetryableErrorTypes = slices.Clone(p.NonRetryableErrorTypes)
		o.RetryPolicy = &p
	}
	return o
}
func WithLocalActivityOptions(ctx context.Context, o LocalActivityOptions) context.Context {
	return context.WithValue(ctx, localOptionsKey{}, cloneLocalOptions(o))
}
func GetLocalActivityOptions(ctx context.Context) LocalActivityOptions {
	if ctx == nil {
		return LocalActivityOptions{}
	}
	o, _ := ctx.Value(localOptionsKey{}).(LocalActivityOptions)
	return cloneLocalOptions(o)
}

type LocalActivityRequest struct {
	Header        map[string][]byte
	ID            uint64
	Name          string
	Function      bool
	Payloads      []byte
	Options       LocalActivityOptions
	Attempt       int32
	ScheduledTime time.Time
}
type LocalActivityOutcome struct {
	ActivityOutcome
	Backoff time.Duration
	Attempt int32
}

// ExecuteLocalActivity executes a registered host activity. SDK bindings record
// its result marker and own in-task retries; longer backoff uses durable timers.
func executeLocalActivity(ctx context.Context, activity any, args ...any) Future {
	assertWritable()
	f := newOperationFuture()
	if ctx == nil {
		return failOperation(f, errors.New("workflow: nil context"))
	}
	if err := ctx.Err(); err != nil {
		return failOperation(f, err)
	}
	name, byName := activity.(string)
	if !byName {
		var err error
		name, err = activityref.Name(activity)
		if err != nil {
			return failOperation(f, err)
		}
		if err = activityref.ValidateContext(activity); err != nil {
			return failOperation(f, err)
		}
		t := reflect.TypeOf(activity)
		if t.NumOut() < 1 || t.NumOut() > 2 || !t.Out(t.NumOut()-1).Implements(reflect.TypeFor[error]()) {
			return failOperation(f, errors.New("workflow: local activity must return error or (result, error)"))
		}
		if len(args) != t.NumIn()-1 {
			return failOperation(f, fmt.Errorf("workflow: local activity got %d arguments, want %d", len(args), t.NumIn()-1))
		}
		for i, arg := range args {
			if arg != nil && !reflect.TypeOf(arg).AssignableTo(t.In(i+1)) {
				return failOperation(f, fmt.Errorf("workflow: local argument %d has incompatible type", i))
			}
		}
	}
	if name == "" {
		return failOperation(f, errors.New("workflow: empty local activity name"))
	}
	o := GetLocalActivityOptions(ctx)
	if o.StartToCloseTimeout < 0 || o.ScheduleToCloseTimeout < 0 || (o.StartToCloseTimeout == 0 && o.ScheduleToCloseTimeout == 0) {
		return failOperation(f, errors.New("workflow: local activity requires a nonnegative timeout"))
	}
	if o.StartToCloseTimeout == 0 {
		o.StartToCloseTimeout = o.ScheduleToCloseTimeout
	}
	if o.ScheduleToCloseTimeout == 0 {
		o.ScheduleToCloseTimeout = o.StartToCloseTimeout
	}
	p, err := encodeActivityArgs(args)
	if err != nil {
		panic(err)
	}
	header, err := outgoingHeader(ctx)
	if err != nil {
		panic(err)
	}
	r := LocalActivityRequest{Name: name, Function: !byName, Payloads: p, Options: o, Attempt: 1, ScheduledTime: time.Now(), Header: header}
	if err = scheduleLocal(&r); err != nil {
		return failOperation(f, err)
	}
	go func() {
		for {
			id := r.ID
			stop := context.AfterFunc(ctx, func() { p, _ := json.Marshal(id); _, _ = isolate.Call(OpCancelLocal, p) })
			p, _ := json.Marshal(id)
			response, err := isolate.Call(OpAwaitLocal, p)
			stop()
			var outcome LocalActivityOutcome
			if err == nil {
				err = json.Unmarshal(response, &outcome)
			}
			if err == nil && outcome.Backoff > 0 && !outcome.Canceled {
				if err = Sleep(ctx, outcome.Backoff); err == nil {
					r.Attempt = outcome.Attempt + 1
					err = scheduleLocal(&r)
					if err == nil {
						continue
					}
				}
			}
			completeOperation(f, response, err, ctx)
			return
		}
	}()
	return f
}
func scheduleLocal(r *LocalActivityRequest) error {
	r.ID = nextCallID.Add(1)
	p, err := json.Marshal(r)
	if err != nil {
		return err
	}
	_, err = isolate.Call(OpScheduleLocal, p)
	return err
}
