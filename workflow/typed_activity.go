package workflow

// Retained API experiment. These typed activity calls are intentionally disabled
// while the POC follows the current Temporal Go SDK activity API.
/*
// ExecuteActivity infers input/result types from a host activity. Every
// workflow and activity takes standard context.Context as its first argument.
// ctx controls cancellation; the activity's own context is supplied by its host.
func ExecuteActivity[I, R any](ctx context.Context, activity func(context.Context, I) (R, error), timeout time.Duration, input I) (R, error) {
	name, err := activityref.Name(activity)
	if err != nil {
		var zero R
		return zero, err
	}
	return executeActivity[R](ctx, name, true, timeout, input)
}

// ExecuteActivityNoInput infers a result from an activity taking only context.
func ExecuteActivityNoInput[R any](ctx context.Context, activity func(context.Context) (R, error), timeout time.Duration) (R, error) {
	name, err := activityref.Name(activity)
	if err != nil {
		var zero R
		return zero, err
	}
	return executeActivity[R](ctx, name, true, timeout)
}

// ExecuteActivityError identifies an activity returning only an error.
func ExecuteActivityError[I any](ctx context.Context, activity func(context.Context, I) error, timeout time.Duration, input I) error {
	name, err := activityref.Name(activity)
	if err != nil {
		return err
	}
	_, err = executeActivity[struct{}](ctx, name, true, timeout, input)
	return err
}

// ExecuteActivityAsync delivers one typed result and closes. ctx controls the
// activity lifetime even after this function returns the channel.
func ExecuteActivityAsync[I, R any](ctx context.Context, activity func(context.Context, I) (R, error), timeout time.Duration, input I) <-chan ActivityResult[R] {
	return activityAsync(func() (R, error) { return ExecuteActivity(ctx, activity, timeout, input) })
}

// ExecuteActivityAsyncError identifies an activity returning only error and
// delivers its completion on a channel. Result is always the empty struct;
// Err reports failure or cancellation. The channel closes after one completion.
func ExecuteActivityAsyncError[I any](ctx context.Context, activity func(context.Context, I) error, timeout time.Duration, input I) <-chan ActivityResult[struct{}] {
	return activityAsync(func() (struct{}, error) {
		return struct{}{}, ExecuteActivityError(ctx, activity, timeout, input)
	})
}

func activityAsync[R any](call func() (R, error)) <-chan ActivityResult[R] {
	results := make(chan ActivityResult[R], 1)
	go func() {
		defer close(results)
		result, err := call()
		results <- ActivityResult[R]{Result: result, Err: err}
	}()
	return results
}

// ExecuteActivityByName schedules a host activity with zero or more typed arguments
// and waits for its typed result. R must be explicit because Go cannot infer
// type parameters from return values. Use struct{} for error-only activities.
// Arguments/results use Temporal's default converter inside the isolate.
func ExecuteActivityByName[R any](ctx context.Context, name string, timeout time.Duration, args ...any) (R, error) {
	return executeActivity[R](ctx, name, false, timeout, args...)
}

func executeActivity[R any](ctx context.Context, name string, function bool, timeout time.Duration, args ...any) (R, error) {
	var zero R
	if ctx == nil {
		return zero, errors.New("workflow: nil context")
	}
	if err := ctx.Err(); err != nil {
		return zero, err
	}
	if name == "" || timeout <= 0 {
		return zero, errors.New("workflow: activity name and timeout are required")
	}
	if isProtoValue(zero) || isProtoValue(&zero) {
		return zero, errors.New("workflow: protobuf values are outside the isolate POC subset")
	}
	payloads, err := encodeActivityArgs(args)
	if err != nil {
		return zero, err
	}
	request, err := json.Marshal(ActivityPayloadRequest{Name: name, Function: function, Payloads: payloads, StartToCloseTimeout: timeout})
	if err != nil {
		return zero, err
	}
	response, err := call(ctx, OpActivityPayloads, request)
	if err != nil {
		return zero, err
	}
	return decodeActivityResult[R](response)
}

// ExecuteActivityAsyncByName starts a typed activity call in an isolate-owned
// goroutine. The buffered channel receives one result and then closes.
func ExecuteActivityAsyncByName[R any](ctx context.Context, name string, timeout time.Duration, args ...any) <-chan ActivityResult[R] {
	return activityAsync(func() (R, error) { return ExecuteActivityByName[R](ctx, name, timeout, args...) })
}

*/
