package testsuite_test

import (
	"context"
	"testing"
	"time"

	"github.com/mfateev/sdk-go-poc/testref"
	"github.com/mfateev/sdk-go-poc/testsuite"
	"github.com/mfateev/sdk-go-poc/workflow"
	"github.com/stretchr/testify/require"
)

//temporal:workflow
func referenceWorkflow(ctx context.Context, ref testref.Ref[int]) (int, error) {
	value, err := ref.Load()
	if err != nil {
		return 0, err
	}
	if err := ref.Store(value + 1); err != nil {
		return 0, err
	}
	if err := workflow.SetQueryHandler(ctx, "observed", func() (int, error) { return ref.Load() }); err != nil {
		return 0, err
	}
	time.Sleep(time.Hour)
	return ref.Load()
}

func TestDirectGoTestReferencesAndCompletedQueries(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment(t).SetTestTimeout(10 * time.Second)
	ref, err := testsuite.NewRef(env, 41)
	require.NoError(t, err)
	start := env.Now()
	env.ExecuteWorkflow(referenceWorkflow, ref)
	require.NoError(t, env.GetWorkflowError())
	var result int
	require.NoError(t, env.GetWorkflowResult(&result))
	require.Equal(t, 42, result)
	require.Equal(t, time.Hour, env.Now().Sub(start))
	value, err := ref.Load()
	require.NoError(t, err)
	require.Equal(t, 42, value)
	query, err := env.QueryWorkflow("observed")
	require.NoError(t, err)
	require.NoError(t, query.Get(&result))
	require.Equal(t, 42, result)
	require.NoError(t, env.Close())
	_, err = ref.Load()
	require.ErrorContains(t, err, "closed")
}
