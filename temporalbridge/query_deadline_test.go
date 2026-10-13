package temporalbridge

import (
	"context"
	"testing"
	"time"
)

func TestReadOnlyWorkerBudgetAndResourceCap(t *testing.T) {
	for _, test := range []struct {
		name          string
		worker, limit time.Duration
		want          time.Duration
	}{
		{"worker", 30 * time.Second, 0, 30 * time.Second},
		{"resource-cap", 30 * time.Second, 5 * time.Second, 5 * time.Second},
		{"resource-cannot-extend", time.Second, 5 * time.Second, time.Second},
		{"default", 0, 0, time.Second},
	} {
		t.Run(test.name, func(t *testing.T) {
			d := &definition{taskTimeout: test.worker, resources: ResourceOptions{MaxTaskDuration: test.limit}}
			before := time.Now()
			ctx, cancel := d.readOnlyContext()
			defer cancel()
			after := time.Now()
			deadline, ok := ctx.Deadline()
			if !ok || deadline.Before(before.Add(test.want)) || deadline.After(after.Add(test.want)) {
				t.Fatalf("deadline=%v, expected worker/resource budget %s", deadline, test.want)
			}
		})
	}
}

func TestReadOnlyInheritsRemainingTaskBudgetAndCancellation(t *testing.T) {
	// A validator must not start a new full budget after earlier task work.
	deadline := time.Now().Add(5 * time.Second)
	parent, cancelParent := context.WithDeadline(context.Background(), deadline)
	defer cancelParent()
	d := &definition{taskTimeout: 30 * time.Second, activeTaskContext: parent}
	ctx, cancel := d.readOnlyContext()
	defer cancel()
	if got, ok := ctx.Deadline(); !ok || !got.Equal(deadline) {
		t.Fatalf("validator extended its task deadline: %v, want %v", got, deadline)
	}
	cancelParent()
	if ctx.Err() != context.Canceled {
		t.Fatalf("validator lost task cancellation: %v", ctx.Err())
	}
	// A completed task must not leave its canceled context attached to cached
	// query state. The last worker budget remains applicable to later queries.
	d.activeTaskContext = nil
	query, cancelQuery := d.readOnlyContext()
	defer cancelQuery()
	if query.Err() != nil {
		t.Fatalf("cached query inherited the completed task's cancellation: %v", query.Err())
	}
}
