package testsuite

import (
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
	goWorker "go.temporal.io/sdk/worker"
	"time"
)

// Preserve the isolate-aware receiver when fluent configuration is chained.
func (e *TestWorkflowEnvironment) SetWorkerOptions(o goWorker.Options) *TestWorkflowEnvironment {
	e.TestWorkflowEnvironment.SetWorkerOptions(o)
	return e
}
func (e *TestWorkflowEnvironment) SetStartWorkflowOptions(o client.StartWorkflowOptions) *TestWorkflowEnvironment {
	e.TestWorkflowEnvironment.SetStartWorkflowOptions(o)
	return e
}
func (e *TestWorkflowEnvironment) SetDataConverter(c converter.DataConverter) *TestWorkflowEnvironment {
	e.TestWorkflowEnvironment.SetDataConverter(c)
	return e
}
func (e *TestWorkflowEnvironment) SetFailureConverter(c converter.FailureConverter) *TestWorkflowEnvironment {
	e.TestWorkflowEnvironment.SetFailureConverter(c)
	return e
}
func (e *TestWorkflowEnvironment) SetIdentity(identity string) *TestWorkflowEnvironment {
	e.TestWorkflowEnvironment.SetIdentity(identity)
	return e
}
func (e *TestWorkflowEnvironment) SetDetachedChildWait(wait bool) *TestWorkflowEnvironment {
	e.TestWorkflowEnvironment.SetDetachedChildWait(wait)
	return e
}
func (e *TestWorkflowEnvironment) SetTestTimeout(timeout time.Duration) *TestWorkflowEnvironment {
	e.TestWorkflowEnvironment.SetTestTimeout(timeout)
	return e
}
func (e *TestWorkflowEnvironment) SetWorkflowRunTimeout(timeout time.Duration) *TestWorkflowEnvironment {
	e.TestWorkflowEnvironment.SetWorkflowRunTimeout(timeout)
	return e
}
func (e *TestWorkflowEnvironment) SetActivityTimeoutGracePeriod(timeout time.Duration) *TestWorkflowEnvironment {
	e.TestWorkflowEnvironment.SetActivityTimeoutGracePeriod(timeout)
	return e
}
