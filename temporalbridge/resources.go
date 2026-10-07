package temporalbridge

import (
	"context"
	"errors"
	"isolate"
	"time"
)

// ResourceOptions are host policy, sampled once when an execution is created.
// Zero values preserve unlimited admission and the SDK's existing task deadline.
// Cached time consumes neither watchdog budget. RunningNanoseconds is scheduled
// interval accounting, not a hardware CPU quota. Uninterrupted code may leave
// pending termination until it reaches a supported runtime fence.
type ResourceOptions struct {
	Limits                isolate.ResourceLimits
	MaxTaskDuration       time.Duration
	MaxNoProgressDuration time.Duration
	Observer              func(ResourceEvent)
}

func (o ResourceOptions) Validate() error {
	if o.MaxTaskDuration < 0 || o.MaxNoProgressDuration < 0 {
		return errors.New("isolate: watchdog durations must not be negative")
	}
	return nil
}

// ResourceEvent is copied host data suitable for logging or metrics. Kind is
// created, task, limit, closed or termination-pending. Replay must be considered
// when aggregating execution statistics. Error never becomes a workflow input.
type ResourceEvent struct {
	Kind                            string
	WorkflowID, RunID, WorkflowType string
	Replay                          bool
	Stats                           isolate.ResourceStats
	Error                           string
}

func (d *definition) observeResources(kind string, cause error) {
	if d.instance != nil {
		d.lastResources = d.instance.Resources()
	}
	handler := d.resources.Observer
	if handler == nil {
		return
	}
	event := d.resourceIdentity
	event.Kind, event.Stats = kind, d.lastResources
	if d.env != nil {
		info := d.env.WorkflowInfo()
		event.WorkflowID, event.RunID, event.WorkflowType = info.WorkflowExecution.ID, info.WorkflowExecution.RunID, info.WorkflowType.Name
		event.Replay = d.env.IsReplaying()
		d.resourceIdentity = event
	}
	if cause != nil {
		event.Error = cause.Error()
	} else {
		event.Error = ""
	}
	// If a host observer panics, leave it disabled while the task's recovery path
	// closes the instance. This prevents a second observer panic during cleanup.
	d.resources.Observer = nil
	handler(event)
	d.resources.Observer = handler
}

type resourceTaskBudget struct {
	start, lastProgress time.Time
	progress            uint64
	options             ResourceOptions
	monitoring          bool
}

func (b *resourceTaskBudget) check(i *isolate.Isolate) error {
	now := time.Now()
	if limit := b.options.MaxTaskDuration; limit > 0 && now.Sub(b.start) >= limit {
		return i.FailTaskDuration(uint64(limit), uint64(now.Sub(b.start)))
	}
	if b.monitoring && b.options.MaxNoProgressDuration > 0 {
		progress := i.Resources().Progress
		if progress != b.progress {
			b.progress, b.lastProgress = progress, now
		} else if elapsed := now.Sub(b.lastProgress); elapsed >= b.options.MaxNoProgressDuration {
			return i.FailProgress(uint64(b.options.MaxNoProgressDuration), uint64(elapsed))
		}
	}
	return nil
}

func (b *resourceTaskBudget) startupError(err error) error {
	var prior *isolate.ResourceLimitError
	if errors.As(err, &prior) || !errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	if limit := b.options.MaxTaskDuration; limit > 0 && time.Since(b.start) >= limit {
		failure := &isolate.ResourceLimitError{Resource: "task duration", Limit: uint64(limit), Usage: uint64(time.Since(b.start))}
		var startup *isolate.InitializationError
		if errors.As(err, &startup) {
			failure.Stack = startup.Pending.Stack
		}
		return errors.Join(failure, err)
	}
	return err
}

// Resources returns current host accounting, or the last cleanup snapshot.
func (d *definition) Resources() isolate.ResourceStats {
	if d.instance != nil {
		return d.instance.Resources()
	}
	return d.lastResources
}
