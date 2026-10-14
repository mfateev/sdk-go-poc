package random

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	mathrand "math/rand"
	randv2 "math/rand/v2"
	"slices"
	"strconv"
	"sync"
	"time"

	"github.com/mfateev/sdk-go-poc/workflow"
)

type StreamTrace struct {
	Primary       []uint64
	QueryOnly     []uint64
	ValidatorOnly []uint64
	Concurrent    []uint64
	Default       []string
}

func defaultValues() []string {
	var data [13]byte
	_, _ = rand.Read(data[:])
	closed := make(chan struct{})
	close(closed)
	choice := ""
	select {
	case <-closed:
		choice = "a"
	case <-closed:
		choice = "b"
	}
	return []string{hex.EncodeToString(data[:]), strconv.FormatUint(mathrand.Uint64(), 16), strconv.FormatUint(randv2.Uint64(), 16), choice}
}

// StreamsWorkflow is a compiled regression fixture for named stream isolation.
//
//temporal:workflow
func StreamsWorkflow(ctx context.Context, mode string) (StreamTrace, error) {
	trace := StreamTrace{Default: defaultValues()}
	var primary workflow.WorkflowRandomStream
	if mode != "default-only" {
		primary = workflow.GetRandomStream(ctx, "primary")
		derived := context.WithValue(ctx, streamContextKey{}, "derived")
		canceled, cancel := context.WithCancel(derived)
		cancel()
		if primary != workflow.GetRandomStream(derived, "primary") || primary != workflow.GetRandomStream(canceled, "primary") {
			return trace, errors.New("derived or canceled context reset a stream")
		}
		for range 4 {
			trace.Primary = append(trace.Primary, primary.Uint64())
		}
		if mode == "other-noise" {
			for _, name := range []string{"other", "", "Primary", "other\x00stream", "日本語"} {
				source := workflow.GetRandomStream(ctx, name)
				if source == primary {
					return trace, errors.New("different names shared a generator")
				}
				for range 97 {
					_ = source.Uint64()
				}
			}
		}
	}
	trace.Default = append(trace.Default, defaultValues()...)
	if err := workflow.SetQueryHandler(ctx, "named", func() ([]uint64, error) {
		first := primary.Uint64()
		_ = workflow.GetInfo(ctx) // An approved read-only host call must retain the scratch stream.
		local := workflow.GetRandomStream(ctx, "primary")
		if local != workflow.GetRandomStream(ctx, "primary") {
			return nil, errors.New("scratch stream lookup reset its state")
		}
		return []uint64{first, local.Uint64(), primary.Uint64()}, nil
	}); err != nil {
		return trace, err
	}
	if err := workflow.SetQueryHandler(ctx, "new-name", func() ([]uint64, error) {
		local := workflow.GetRandomStream(ctx, "query-only")
		values := []uint64{local.Uint64(), workflow.GetRandomStream(ctx, "query-only").Uint64()}
		return values, nil
	}); err != nil {
		return trace, err
	}
	protected := make([]uint64, 1)
	if err := workflow.SetQueryHandler(ctx, "bad-named", func() (int, error) {
		protected[0] = primary.Uint64()
		return 0, nil
	}); err != nil {
		return trace, err
	}
	if err := workflow.SetUpdateHandlerWithOptions(ctx, "probe", func(context.Context) error {
		return errors.New("rejected validator executed its handler")
	}, workflow.UpdateHandlerOptions{Validator: func(context.Context) error {
		var prefix [3]byte
		_, _ = primary.Read(prefix[:])
		_ = primary.Uint64()
		_ = workflow.GetRandomStream(ctx, "primary").Uint64()
		_ = workflow.GetRandomStream(ctx, "validator-only").Uint64()
		return errors.New("probe rejected")
	}}); err != nil {
		return trace, err
	}
	activityCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{StartToCloseTimeout: time.Minute})
	var echo StreamTrace
	if err := workflow.ExecuteActivity(activityCtx, "EchoStream", trace).Get(ctx, &echo); err != nil {
		return trace, err
	}
	if !slices.Equal(echo.Primary, trace.Primary) || !slices.Equal(echo.Default, trace.Default) {
		return trace, errors.New("activity trace changed")
	}
	if primary != nil {
		for range 3 {
			trace.Primary = append(trace.Primary, workflow.GetRandomStream(ctx, "primary").Uint64())
		}
		queryOnly := workflow.GetRandomStream(ctx, "query-only")
		validatorOnly := workflow.GetRandomStream(ctx, "validator-only")
		for range 3 {
			trace.QueryOnly = append(trace.QueryOnly, queryOnly.Uint64())
			trace.ValidatorOnly = append(trace.ValidatorOnly, validatorOnly.Uint64())
		}
		results := make(chan []uint64, 8)
		var pending sync.WaitGroup
		for range 8 {
			pending.Add(1)
			go func() {
				defer pending.Done()
				local := workflow.GetRandomStream(ctx, "concurrent")
				values := make([]uint64, 32)
				for i := range values {
					values[i] = local.Uint64()
				}
				results <- values
			}()
		}
		pending.Wait()
		close(results)
		for values := range results {
			trace.Concurrent = append(trace.Concurrent, values...)
		}
		slices.Sort(trace.Concurrent)
	}
	trace.Default = append(trace.Default, defaultValues()...)
	return trace, nil
}

type streamContextKey struct{}

func EchoStream(_ context.Context, input StreamTrace) (StreamTrace, error) { return input, nil }

// DistributionWorkflow exercises the public v2.Rand facade, not just Uint64.
//
//temporal:workflow
func DistributionWorkflow(ctx context.Context) ([]string, error) {
	stream := randv2.New(workflow.GetRandomStream(ctx, "distributions"))
	return []string{fmt.Sprint(stream.IntN(123), stream.Float64(), stream.NormFloat64(), stream.ExpFloat64(), stream.Perm(7))}, nil
}

//temporal:workflow
func UpdateStreamsWorkflow(ctx context.Context) (StreamTrace, error) {
	primary := workflow.GetRandomStream(ctx, "primary")
	trace := StreamTrace{Primary: []uint64{primary.Uint64()}}
	if err := workflow.SetUpdateHandlerWithOptions(ctx, "draw", func(ctx context.Context) (uint64, error) {
		if workflow.IsReadOnly(ctx) {
			return 0, errors.New("accepted handler remained read-only")
		}
		return workflow.GetRandomStream(ctx, "primary").Uint64(), nil
	}, workflow.UpdateHandlerOptions{Validator: func(context.Context) error {
		var prefix [3]byte
		_, _ = primary.Read(prefix[:])
		_ = primary.Uint64()
		_ = workflow.GetRandomStream(ctx, "primary").Uint64()
		return nil
	}}); err != nil {
		return trace, err
	}
	var echo StreamTrace
	activityCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{StartToCloseTimeout: time.Minute})
	if err := workflow.ExecuteActivity(activityCtx, "EchoStream", trace).Get(ctx, &echo); err != nil {
		return trace, err
	}
	trace.Primary = append(trace.Primary, primary.Uint64())
	return trace, nil
}

const GoldenStreamName = "go.temporal.io/sdk/internal/test"

//temporal:workflow
func GoldenStreamsWorkflow(ctx context.Context, mode string) (string, error) {
	source := workflow.GetRandomStream(ctx, GoldenStreamName)
	if mode == "uint64" {
		return fmt.Sprintf("%016x", source.Uint64()), nil
	}
	if mode == "interleaved" {
		first, last := make([]byte, 3), make([]byte, 5)
		_, _ = source.Read(first)
		var middle [8]byte
		binary.LittleEndian.PutUint64(middle[:], source.Uint64())
		_, _ = source.Read(last)
		return hex.EncodeToString(append(append(first, middle[:]...), last...)), nil
	}
	data := make([]byte, 32)
	_, _ = source.Read(data)
	return hex.EncodeToString(data), nil
}

type ByteTrace struct{ Before, After string }

//temporal:workflow
func ByteStreamsWorkflow(ctx context.Context) (ByteTrace, error) {
	source := workflow.GetRandomStream(ctx, "bytes")
	prefix := make([]byte, 3)
	_, _ = source.Read(prefix)
	trace := ByteTrace{Before: hex.EncodeToString(prefix)}
	if err := workflow.SetQueryHandler(ctx, "next-bytes", func() (string, error) {
		first, last := make([]byte, 3), make([]byte, 5)
		_, _ = source.Read(first)
		var middle [8]byte
		binary.LittleEndian.PutUint64(middle[:], workflow.GetRandomStream(ctx, "bytes").Uint64())
		_, _ = source.Read(last)
		return hex.EncodeToString(append(append(first, middle[:]...), last...)), nil
	}); err != nil {
		return trace, err
	}
	if err := workflow.SetQueryHandler(ctx, "bad-bytes", func() (int, error) {
		return source.Read(prefix)
	}); err != nil {
		return trace, err
	}
	activityCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{StartToCloseTimeout: time.Minute})
	var echo ByteTrace
	if err := workflow.ExecuteActivity(activityCtx, "EchoBytes", trace).Get(ctx, &echo); err != nil {
		return trace, err
	}
	data := make([]byte, 16)
	_, _ = source.Read(data)
	trace.After = hex.EncodeToString(data)
	return trace, nil
}

func EchoBytes(_ context.Context, trace ByteTrace) (ByteTrace, error) { return trace, nil }

//temporal:workflow
func ContinueStreamsWorkflow(ctx context.Context, previous uint64) ([]uint64, error) {
	current := workflow.GetRandomStream(ctx, "continue").Uint64()
	if previous == 0 {
		return nil, workflow.NewContinueAsNewError(ctx, ContinueStreamsWorkflow, current)
	}
	return []uint64{previous, current}, nil
}

//temporal:workflow
func ParentStreamsWorkflow(ctx context.Context) ([]uint64, error) {
	parent := workflow.GetRandomStream(ctx, "continue").Uint64()
	childCtx := workflow.WithChildWorkflowOptions(ctx, workflow.ChildWorkflowOptions{WorkflowRunTimeout: time.Minute})
	var child []uint64
	if err := workflow.ExecuteChildWorkflow(childCtx, ContinueStreamsWorkflow, uint64(0)).Get(ctx, &child); err != nil {
		return nil, err
	}
	return append([]uint64{parent}, child...), nil
}

//temporal:workflow
func ResetStreamsWorkflow(ctx context.Context, late bool) ([]uint64, error) {
	name := "reset"
	if late {
		name = "other"
	}
	captured := workflow.GetRandomStream(ctx, name)
	first := captured.Uint64()
	// Leave unaligned byte state to ensure in-place reseeding clears leftovers.
	var prefix [3]byte
	_, _ = captured.Read(prefix[:])
	activityCtx := workflow.WithActivityOptions(context.WithoutCancel(ctx), workflow.ActivityOptions{StartToCloseTimeout: time.Minute})
	var ignored StreamTrace
	if err := workflow.ExecuteActivity(activityCtx, "EchoStream", StreamTrace{}).Get(activityCtx, &ignored); err != nil {
		return nil, err
	}
	source := captured
	if late {
		source = workflow.GetRandomStream(ctx, "reset")
	} else if source != workflow.GetRandomStream(ctx, name) {
		return nil, errors.New("reset replaced a captured source")
	}
	return []uint64{first, source.Uint64()}, nil
}
