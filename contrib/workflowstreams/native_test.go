package workflowstreams

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mfateev/sdk-go-poc/testsuite"
	"github.com/mfateev/sdk-go-poc/workflow"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/temporal"
)

//go:isolate
func namespaceWorkflow(ctx context.Context, kind string) (bool, error) {
	switch kind {
	case "signal":
		workflow.GetSignalChannel[[]byte](ctx, "__temporal_other")
	case "query":
		return workflow.SetQueryHandler(ctx, "__temporal_other", func() (int, error) { return 0, nil }) != nil, nil
	case "update":
		return workflow.SetUpdateHandler(ctx, "__temporal_other", func(context.Context) error { return nil }) != nil, nil
	}
	workflow.GetSignalChannel[PublishInput](ctx, PublishSignalName)
	if err := workflow.SetUpdateHandler(ctx, PollUpdateName, func(context.Context) error { return nil }); err != nil {
		return false, err
	}
	if err := workflow.SetQueryHandler(ctx, OffsetQueryName, func() (int, error) { return 0, nil }); err != nil {
		return false, err
	}
	return true, nil
}

func TestIsWorkflowStreamReservedNameAllowsHandlers(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment(t)
	env.ExecuteWorkflow(namespaceWorkflow, "allowed")
	require.NoError(t, env.GetWorkflowError())
	var allowed bool
	require.NoError(t, env.GetWorkflowResult(&allowed))
	require.True(t, allowed)
}

func TestIsWorkflowStreamReservedNameStillRejectsOtherNames(t *testing.T) {
	for _, kind := range []string{"signal", "query", "update"} {
		t.Run(kind, func(t *testing.T) {
			var suite testsuite.WorkflowTestSuite
			env := suite.NewTestWorkflowEnvironment(t)
			env.ExecuteWorkflow(namespaceWorkflow, kind)
			if kind == "signal" {
				require.Error(t, env.GetWorkflowError())
				return
			}
			require.NoError(t, env.GetWorkflowError())
			var rejected bool
			require.NoError(t, env.GetWorkflowResult(&rejected))
			require.True(t, rejected)
		})
	}
}

//go:isolate
func controlledStreamWorkflow(ctx context.Context, state *WorkflowStreamState) (*WorkflowStreamState, error) {
	stream, err := NewWorkflowStream(ctx, state)
	if err != nil {
		return nil, err
	}
	if err := workflow.SetUpdateHandler(ctx, "truncate", func(_ context.Context, offset int64) error { return stream.Truncate(offset) }); err != nil {
		return nil, err
	}
	if err := workflow.SetUpdateHandler(ctx, "detach", func(context.Context) error { stream.DetachPollers(); return nil }); err != nil {
		return nil, err
	}
	if err := workflow.SetQueryHandler(ctx, "snapshot", func() (*WorkflowStreamState, error) { return stream.GetState(time.Hour) }); err != nil {
		return nil, err
	}
	<-workflow.GetSignalChannel[[]byte](ctx, "finish")
	if err := workflow.WaitHandlersFinished(ctx); err != nil {
		return nil, err
	}
	return stream.GetState(time.Hour)
}

func TestNativePollWakeupTruncationAndDrain(t *testing.T) {
	for _, procs := range []int{1, 2, 8} {
		t.Run(fmt.Sprint(procs), func(t *testing.T) {
			old := runtime.GOMAXPROCS(procs)
			defer runtime.GOMAXPROCS(old)
			var suite testsuite.WorkflowTestSuite
			env := suite.NewTestWorkflowEnvironment(t)
			completed := map[string]bool{}
			poll := func(id string, offset int64, wantType string, wantItems int) {
				env.UpdateWorkflow(PollUpdateName, id, &testsuite.TestUpdateCallback{
					OnAccept: func() {},
					OnReject: func(err error) {
						require.Equal(t, ErrTypeStreamDraining, wantType)
						var app *temporal.ApplicationError
						require.ErrorAs(t, err, &app)
						require.Equal(t, wantType, app.Type())
						completed[id] = true
					},
					OnComplete: func(value any, err error) {
						if wantType != "" {
							var app *temporal.ApplicationError
							require.ErrorAs(t, err, &app)
							require.Equal(t, wantType, app.Type())
						} else {
							require.NoError(t, err)
							result := testsuite.DecodeResult[PollResult](value)
							require.Len(t, result.Items, wantItems)
						}
						completed[id] = true
					},
				}, PollInput{FromOffset: offset})
			}
			control := func(name string, args ...any) {
				env.UpdateWorkflow(name, name, &testsuite.TestUpdateCallback{
					OnAccept: func() {}, OnReject: func(err error) { t.Errorf("%s rejected: %v", name, err) },
					OnComplete: func(_ any, err error) { require.NoError(t, err) },
				}, args...)
			}
			env.RegisterDelayedCallback(func() { poll("wake", 0, "", 3) }, time.Second)
			env.RegisterDelayedCallback(func() {
				require.False(t, completed["wake"])
				env.SignalWorkflow(PublishSignalName, mustPublishInput(t, "pub", 1, topicVal{"a", "one"}, topicVal{"b", "two"}, topicVal{"a", "three"}))
			}, 2*time.Second)
			env.RegisterDelayedCallback(func() {
				require.True(t, completed["wake"])
				query, err := env.QueryWorkflow("snapshot")
				require.NoError(t, err)
				var snapshot WorkflowStreamState
				require.NoError(t, query.Get(&snapshot))
				require.Len(t, snapshot.Log, 3)
				require.EqualValues(t, 0, snapshot.BaseOffset)
				require.EqualValues(t, 1, snapshot.PublisherSeqs["pub"])
				control("truncate", int64(2))
			}, 3*time.Second)
			env.RegisterDelayedCallback(func() {
				poll("truncated", 1, ErrTypeTruncatedOffset, 0)
				poll("remaining", 0, "", 1)
				poll("waiting", 3, "", 0)
			}, 4*time.Second)
			env.RegisterDelayedCallback(func() {
				require.True(t, completed["truncated"])
				require.True(t, completed["remaining"])
				require.False(t, completed["waiting"])
				control("detach")
			}, 5*time.Second)
			env.RegisterDelayedCallback(func() {
				require.True(t, completed["waiting"])
				poll("draining", 3, ErrTypeStreamDraining, 0)
			}, 6*time.Second)
			env.RegisterDelayedCallback(func() {
				require.True(t, completed["draining"])
				env.SignalWorkflow("finish", []byte("done"))
			}, 7*time.Second)
			env.ExecuteWorkflow(controlledStreamWorkflow, (*WorkflowStreamState)(nil))
			require.NoError(t, env.GetWorkflowError())
			var state WorkflowStreamState
			require.NoError(t, env.GetWorkflowResult(&state))
			require.EqualValues(t, 2, state.BaseOffset)
			require.Len(t, state.Log, 1)
			require.EqualValues(t, 1, state.PublisherSeqs["pub"])
			query, err := env.QueryWorkflow("snapshot")
			require.NoError(t, err)
			var snapshot WorkflowStreamState
			require.NoError(t, query.Get(&snapshot))
			require.Equal(t, state, snapshot)

		})
	}
}

//go:isolate
func stateAndPagingWorkflow(ctx context.Context, state *WorkflowStreamState) (*WorkflowStreamState, error) {
	s, err := NewWorkflowStream(ctx, state)
	if err != nil {
		return nil, err
	}

	s.publisherSeqs["expired"] = 1
	s.publisherLastSeen["expired"] = float64(time.Now().Add(-time.Hour).Unix())
	var wg sync.WaitGroup
	for range 3 {
		wg.Go(func() {
			if err := s.Topic("large").Publish(strings.Repeat("x", 600000)); err != nil {
				panic(err)
			}
		})
	}
	wg.Wait()
	if s.Topic("large") != s.Topic("large") {
		return nil, errors.New("topic handle was not reused")
	}
	offset := int64(0)
	count := 0
	for {
		result, err := s.onPoll(ctx, PollInput{FromOffset: offset})
		if err != nil {
			return nil, err
		}
		if len(result.Items) != 1 {
			return nil, fmt.Errorf("page items: %d", len(result.Items))
		}
		count++
		offset = result.NextOffset
		if !result.MoreReady {
			break
		}
	}
	if count != 3 {
		return nil, fmt.Errorf("pages: %d", count)
	}
	var app *temporal.ApplicationError
	if !errors.As(s.Truncate(offset+1), &app) || app.Type() != ErrTypeTruncateOutOfRange {
		return nil, errors.New("truncate out of range accepted")
	}
	return s.GetState(15 * time.Minute)
}

func TestNativeStreamPagingStateTTLAndConcurrentPublish(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment(t)
	env.ExecuteWorkflow(stateAndPagingWorkflow, (*WorkflowStreamState)(nil))
	require.NoError(t, env.GetWorkflowError())
	var state WorkflowStreamState
	require.NoError(t, env.GetWorkflowResult(&state))
	require.Len(t, state.Log, 3)
	require.NotContains(t, state.PublisherSeqs, "expired")
	env = suite.NewTestWorkflowEnvironment(t)
	state.BaseOffset = 2
	state.Log = state.Log[:1]
	state.Log[0] = WireItem{Topic: "old", Data: mustPublishInput(t, "", 0, topicVal{"old", "small"}).Items[0].Data}
	state.PublisherSeqs = map[string]int64{"pub": 1}
	state.PublisherLastSeen = map[string]float64{"pub": 0}
	env.ExecuteWorkflow(restoredStreamWorkflow, &state)
	require.NoError(t, env.GetWorkflowError())
}

//go:isolate
func restoredStreamWorkflow(ctx context.Context, state *WorkflowStreamState) error {
	s, err := NewWorkflowStream(ctx, state)
	if err != nil {
		return err
	}
	s.onPublish(PublishInput{PublisherID: "pub", Sequence: 1, Items: []PublishEntry{{Topic: "dup", Data: state.Log[0].Data}}})
	result, err := s.onPoll(ctx, PollInput{FromOffset: 0})
	if err != nil {
		return err
	}
	if len(result.Items) != 1 || result.Items[0].Offset != 2 || result.NextOffset != 3 {
		return errors.New("restored offsets or dedup changed")
	}
	return nil
}

//go:isolate
func canceledPollWorkflow(ctx context.Context) error {
	s, err := NewWorkflowStream(ctx, nil)
	if err != nil {
		return err
	}
	_, err = s.onPoll(ctx, PollInput{})
	return err
}

func TestNativeStreamPollCancellation(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment(t)
	env.RegisterDelayedCallback(env.CancelWorkflow, time.Second)
	env.ExecuteWorkflow(canceledPollWorkflow)
	require.True(t, temporal.IsCanceledError(env.GetWorkflowError()))
}

func TestWorkflowStreamRejectsInvalidState(t *testing.T) {
	_, err := NewWorkflowStream(nil, nil)
	require.ErrorContains(t, err, "non-nil context")
	_, err = NewWorkflowStream(context.Background(), &WorkflowStreamState{Log: []WireItem{{Data: "invalid base64"}}})
	require.ErrorContains(t, err, "restore log")
}

//go:isolate
func snapshotRolloverWorkflow(ctx context.Context, state *WorkflowStreamState) error {
	s, err := NewWorkflowStream(ctx, state)
	if err != nil {
		return err
	}
	<-workflow.GetSignalChannel[[]byte](ctx, "rollover")
	return s.NewContinueAsNewError(ctx, snapshotRolloverWorkflow, func(state *WorkflowStreamState) []any { return []any{state} })
}

func TestNativeContinuationCapturesAdmittedPublishes(t *testing.T) {
	for _, procs := range []int{1, 2, 8} {
		t.Run(fmt.Sprint(procs), func(t *testing.T) {
			old := runtime.GOMAXPROCS(procs)
			defer runtime.GOMAXPROCS(old)
			var suite testsuite.WorkflowTestSuite
			env := suite.NewTestWorkflowEnvironment(t)
			env.RegisterDelayedCallback(func() {
				env.SignalWorkflow(PublishSignalName, mustPublishInput(t, "pub", 1, topicVal{"events", "queued"}))
				env.SignalWorkflow("rollover", []byte("now"))
			}, time.Second)
			env.ExecuteWorkflow(snapshotRolloverWorkflow, (*WorkflowStreamState)(nil))
			var continuation *workflow.ContinueAsNewError
			require.ErrorAs(t, env.GetWorkflowError(), &continuation)
			var state WorkflowStreamState
			require.NoError(t, converter.GetDefaultDataConverter().FromPayloads(continuation.Input, &state))
			require.Len(t, state.Log, 1)
			require.EqualValues(t, 1, state.PublisherSeqs["pub"])
		})
	}
}
