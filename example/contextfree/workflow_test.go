package contextfree_test

import (
	"fmt"
	"testing"

	"github.com/mfateev/sdk-go-poc/example/contextfree"
	"github.com/mfateev/sdk-go-poc/testsuite"
	"github.com/stretchr/testify/require"
)

func TestOptionalActivityContext(t *testing.T) {
	for _, native := range []bool{false, true} {
		for _, local := range []bool{false, true} {
			for _, mocked := range []bool{false, true} {
				t.Run(fmt.Sprintf("native=%t/local=%t/mock=%t", native, local, mocked), func(t *testing.T) {
					var suite testsuite.WorkflowTestSuite
					env := suite.NewTestWorkflowEnvironment(t)
					defer func() { require.NoError(t, env.Close()) }()
					contextfree.RegisterActivities(env)
					expected := contextfree.Expected()
					if mocked {
						env.OnActivity(contextfree.NoContext, 5).Return(func(value int) (int, error) { return value + 96, nil }).Once()
						env.OnActivity(contextfree.ErrorOnly).Return(func() error { return nil }).Once()
						expected[1] = 101
					}
					var fn any = contextfree.Baseline
					if native {
						fn = contextfree.Workflow
					}
					env.ExecuteWorkflow(fn, local)
					require.NoError(t, env.GetWorkflowError())
					var got []int
					require.NoError(t, env.GetWorkflowResult(&got))
					require.Equal(t, expected, got)
					if mocked {
						require.True(t, env.AssertExpectations(t))
					}
				})
			}
		}
	}
}
