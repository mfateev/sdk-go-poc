package main

import (
	example "github.com/mfateev/sdk-go-poc/example/random"
	"github.com/mfateev/sdk-go-poc/testsuite"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestChildContinueAsNewDrawsNewValues(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment(t)
	env.RegisterWorkflow(example.ParentStreamsWorkflow)
	env.RegisterWorkflow(example.ContinueStreamsWorkflow)
	env.ExecuteWorkflow(example.ParentStreamsWorkflow)
	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())
	var result []uint64
	require.NoError(t, env.GetWorkflowResult(&result))
	require.Len(t, result, 3)
	require.NotEqual(t, result[0], result[1])
	require.NotEqual(t, result[0], result[2])
	require.NotEqual(t, result[1], result[2])
}
