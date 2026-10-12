package testsuite

import (
	"context"
	"time"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/converter"
	goWorkflow "go.temporal.io/sdk/workflow"
)

// Fluent listeners retain the native environment rather than returning its
// embedded host environment, which would bypass isolate registration.
func (e *TestWorkflowEnvironment) SetContextPropagators(p []goWorkflow.ContextPropagator) *TestWorkflowEnvironment {
	e.TestWorkflowEnvironment.SetContextPropagators(p)
	return e
}
func (e *TestWorkflowEnvironment) SetOnActivityStartedListener(f func(*activity.Info, context.Context, converter.EncodedValues)) *TestWorkflowEnvironment {
	e.TestWorkflowEnvironment.SetOnActivityStartedListener(f)
	return e
}
func (e *TestWorkflowEnvironment) SetOnActivityCompletedListener(f func(*activity.Info, converter.EncodedValue, error)) *TestWorkflowEnvironment {
	e.TestWorkflowEnvironment.SetOnActivityCompletedListener(f)
	return e
}
func (e *TestWorkflowEnvironment) SetOnActivityCanceledListener(f func(*activity.Info)) *TestWorkflowEnvironment {
	e.TestWorkflowEnvironment.SetOnActivityCanceledListener(f)
	return e
}
func (e *TestWorkflowEnvironment) SetOnActivityHeartbeatListener(f func(*activity.Info, converter.EncodedValues)) *TestWorkflowEnvironment {
	e.TestWorkflowEnvironment.SetOnActivityHeartbeatListener(f)
	return e
}
func (e *TestWorkflowEnvironment) SetOnChildWorkflowCompletedListener(f func(*goWorkflow.Info, converter.EncodedValue, error)) *TestWorkflowEnvironment {
	e.TestWorkflowEnvironment.SetOnChildWorkflowCompletedListener(f)
	return e
}
func (e *TestWorkflowEnvironment) SetOnChildWorkflowCanceledListener(f func(*goWorkflow.Info)) *TestWorkflowEnvironment {
	e.TestWorkflowEnvironment.SetOnChildWorkflowCanceledListener(f)
	return e
}
func (e *TestWorkflowEnvironment) SetOnTimerScheduledListener(f func(string, time.Duration)) *TestWorkflowEnvironment {
	e.TestWorkflowEnvironment.SetOnTimerScheduledListener(f)
	return e
}
func (e *TestWorkflowEnvironment) SetOnTimerFiredListener(f func(string)) *TestWorkflowEnvironment {
	e.TestWorkflowEnvironment.SetOnTimerFiredListener(f)
	return e
}
func (e *TestWorkflowEnvironment) SetOnTimerCanceledListener(f func(string)) *TestWorkflowEnvironment {
	e.TestWorkflowEnvironment.SetOnTimerCanceledListener(f)
	return e
}
func (e *TestWorkflowEnvironment) SetOnLocalActivityStartedListener(f func(*activity.Info, context.Context, []any)) *TestWorkflowEnvironment {
	e.TestWorkflowEnvironment.SetOnLocalActivityStartedListener(f)
	return e
}
func (e *TestWorkflowEnvironment) SetOnLocalActivityCompletedListener(f func(*activity.Info, converter.EncodedValue, error)) *TestWorkflowEnvironment {
	e.TestWorkflowEnvironment.SetOnLocalActivityCompletedListener(f)
	return e
}
func (e *TestWorkflowEnvironment) SetOnLocalActivityCanceledListener(f func(*activity.Info)) *TestWorkflowEnvironment {
	e.TestWorkflowEnvironment.SetOnLocalActivityCanceledListener(f)
	return e
}
func (e *TestWorkflowEnvironment) SetOnNexusOperationStartedListener(f func(string, string, converter.EncodedValue)) *TestWorkflowEnvironment {
	e.TestWorkflowEnvironment.SetOnNexusOperationStartedListener(f)
	return e
}
func (e *TestWorkflowEnvironment) SetOnNexusOperationCompletedListener(f func(string, string, converter.EncodedValue, error)) *TestWorkflowEnvironment {
	e.TestWorkflowEnvironment.SetOnNexusOperationCompletedListener(f)
	return e
}
func (e *TestWorkflowEnvironment) SetOnNexusOperationCanceledListener(f func(string, string)) *TestWorkflowEnvironment {
	e.TestWorkflowEnvironment.SetOnNexusOperationCanceledListener(f)
	return e
}
