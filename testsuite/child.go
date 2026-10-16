package testsuite

import (
	"errors"
	"strconv"
	"sync"

	commonpb "go.temporal.io/api/common/v1"
	bindings "go.temporal.io/sdk/internalbindings"
	goWorkflow "go.temporal.io/sdk/workflow"
)

// A factory starts before its native mock has run. Hold the parent start
// notification until mock admission, including simulated child-start failure.
type childStart struct {
	mu        sync.Mutex
	callback  func(bindings.WorkflowExecution, error)
	execution bindings.WorkflowExecution
	seen      bool
	admitted  bool
	err       error
	delivered bool
}

func (s *childStart) update(execution bindings.WorkflowExecution, err error, admit bool) {
	s.mu.Lock()
	if admit {
		s.admitted = true
		s.err = err
	} else {
		s.seen = true
		s.execution = execution
		if err != nil {
			s.err = err
			s.admitted = true
		}
	}
	ready := !s.delivered && s.admitted && (s.seen || s.err != nil)
	if ready {
		s.delivered = true
	}
	callback, result, cause := s.callback, s.execution, s.err
	if cause != nil {
		result = bindings.WorkflowExecution{}
	}
	s.mu.Unlock()
	if ready {
		callback(result, cause)
	}
}

func (e *TestWorkflowEnvironment) admitChild(id string, err error) {
	e.mu.Lock()
	start := e.starts[id]
	e.mu.Unlock()
	if start != nil {
		start.update(bindings.WorkflowExecution{}, err, true)
	}
}

type testHost struct {
	bindings.WorkflowEnvironment
	owner *TestWorkflowEnvironment
}

// Unit environments omit OriginalRunID. Native session identity and random
// seeding use the stable run ID supplied by the host test engine instead.
func (h testHost) WorkflowInfo() *goWorkflow.Info {
	info := *h.WorkflowEnvironment.WorkflowInfo()
	if info.OriginalRunID == "" {
		info.OriginalRunID = info.WorkflowExecution.RunID
	}
	return &info
}

func (h testHost) ExecuteChildWorkflow(params bindings.ExecuteWorkflowParams, callback bindings.ResultHandler, started func(bindings.WorkflowExecution, error)) {
	if _, native := h.owner.native[params.WorkflowType.Name]; !native {
		h.WorkflowEnvironment.ExecuteChildWorkflow(params, callback, started)
		return
	}
	if params.WorkflowID == "" {
		params.WorkflowID = h.WorkflowInfo().OriginalRunID + "_" + strconv.FormatInt(h.GenerateSequence(), 10)
	}
	start := &childStart{callback: started}
	h.owner.mu.Lock()
	if previous := h.owner.starts[params.WorkflowID]; previous == nil {
		h.owner.starts[params.WorkflowID] = start
	} else {
		start.admitted = true
	}
	h.owner.mu.Unlock()
	h.WorkflowEnvironment.ExecuteChildWorkflow(params, func(result *commonpb.Payloads, err error) {
		var startErr error
		if errors.Is(err, ErrMockStartChildWorkflowFailed) {
			startErr = err
		}
		start.update(bindings.WorkflowExecution{}, startErr, true)
		h.owner.mu.Lock()
		if h.owner.starts[params.WorkflowID] == start {
			delete(h.owner.starts, params.WorkflowID)
		}
		h.owner.mu.Unlock()
		callback(result, err)
	}, func(execution bindings.WorkflowExecution, err error) { start.update(execution, err, false) })
}

func (d *testDefinition) complete(result *commonpb.Payloads, err error) {
	d.finished = true
	var startErr error
	if errors.Is(err, ErrMockStartChildWorkflowFailed) {
		startErr = err
	}
	d.owner.admitChild(d.host.WorkflowInfo().WorkflowExecution.ID, startErr)
	d.host.Complete(result, err)
}
