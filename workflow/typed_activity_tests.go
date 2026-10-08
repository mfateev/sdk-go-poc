package workflow

// Retained tests for the disabled typed activity API experiment.
/*
func TestExecuteActivityAsyncErrorCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	activity := func(context.Context, string) error { panic("activity must execute on the host") }
	results := ExecuteActivityAsyncError(ctx, activity, time.Second, "input")
	select {
	case result, open := <-results:
		if !open || !errors.Is(result.Err, context.Canceled) {
			t.Fatalf("completion: %+v, open: %t", result, open)
		}
	case <-time.After(time.Second):
		t.Fatal("canceled activity did not complete")
	}
	if _, open := <-results; open {
		t.Fatal("completion channel did not close")
	}
}

func TestExecuteActivityAsyncReturnsOneErrorAndCloses(t *testing.T) {
	results := ExecuteActivityAsyncByName[[]byte](context.Background(), "", time.Second)
	select {
	case result, ok := <-results:
		if !ok || result.Err == nil || result.Result != nil {
			t.Fatalf("unexpected activity result: %+v, open: %t", result, ok)
		}
	case <-time.After(time.Second):
		t.Fatal("activity result did not arrive")
	}
	if _, ok := <-results; ok {
		t.Fatal("activity result channel did not close")
	}
}

func TestInferredActivityRejectsNilAndNeverInvokesFunction(t *testing.T) {
	var fn func(context.Context, int) (string, error)
	var got string
	var err error
	// Assignment to string also checks inferred result type at compile time.
	got, err = ExecuteActivity(context.Background(), fn, time.Second, 5)
	if got != "" || err == nil {
		t.Fatalf("nil activity: %q, %v", got, err)
	}
	fn = func(context.Context, int) (string, error) { panic("activity executed in workflow") }
	got, err = ExecuteActivity(context.Background(), fn, 0, 5)
	if got != "" || err == nil {
		t.Fatalf("invalid timeout: %q, %v", got, err)
	}
	results := ExecuteActivityAsync(context.Background(), fn, 0, 5)
	result := <-results
	got = result.Result
	if result.Err == nil || got != "" {
		t.Fatalf("async invalid timeout: %+v", result)
	}
	if _, open := <-results; open {
		t.Fatal("async result channel stayed open")
	}
	var contextFn func(context.Context, int) (string, error)
	got, err = ExecuteActivity(context.Background(), contextFn, time.Second, 5)
	if got != "" || err == nil {
		t.Fatalf("nil context activity: %q, %v", got, err)
	}
	contextResult := <-ExecuteActivityAsync(context.Background(), contextFn, time.Second, 5)
	if contextResult.Err == nil {
		t.Fatal("async nil context activity accepted")
	}
}

func TestActivityReferenceSignatureVariants(t *testing.T) {
	var noInput func(context.Context) (string, error)
	var errorOnly func(context.Context, string) error
	var got string
	got, err := ExecuteActivityNoInput(context.Background(), noInput, time.Second)
	if got != "" || err == nil {
		t.Fatalf("nil no-input reference: %q, %v", got, err)
	}
	if err := ExecuteActivityError(context.Background(), errorOnly, time.Second, "input"); err == nil {
		t.Fatal("accepted nil error-only activity")
	}
	noInput = func(context.Context) (string, error) { panic("must run on host") }
	errorOnly = func(context.Context, string) error { panic("must run on host") }
	if _, err := ExecuteActivityNoInput(context.Background(), noInput, 0); err == nil {
		t.Fatal("accepted invalid timeout")
	}
	if err := ExecuteActivityError(context.Background(), errorOnly, 0, "input"); err == nil {
		t.Fatal("accepted invalid timeout")
	}
}

*/
