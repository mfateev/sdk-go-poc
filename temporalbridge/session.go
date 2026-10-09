package temporalbridge

import (
	"encoding/json"
	"errors"
	"fmt"
	"isolate"

	"github.com/mfateev/sdk-go-poc/workflow"
	goWorkflow "go.temporal.io/sdk/workflow"
)

func (d *definition) handleSession(c *isolate.Command) error {
	switch c.Op {
	case workflow.OpSessionID:
		info := d.env.WorkflowInfo()
		if info.OriginalRunID == "" {
			return errors.New("session identity requires the original workflow run ID")
		}
		// OriginalRunID is stable in live execution and standalone replay.
		// Standalone replay substitutes a synthetic workflow ID. The original
		// run UUID alone identifies the execution and survives that substitution.
		id := fmt.Sprintf("isolate-session/%s/%d", info.OriginalRunID, d.env.GenerateSequence())
		d.replyWhenSuspended(c, []byte(id), nil)
	case workflow.OpAddSession:
		var s goWorkflow.SessionInfo
		if err := json.Unmarshal(c.Payload, &s); err != nil {
			return err
		}
		if s.SessionID == "" || s.SessionState != goWorkflow.SessionStateOpen {
			return errors.New("invalid session metadata")
		}
		d.env.AddSession(&s)
		d.replyWhenSuspended(c, nil, nil)
	case workflow.OpRemoveSession:
		if len(c.Payload) == 0 {
			return errors.New("empty session ID")
		}
		d.env.RemoveSession(string(c.Payload))
		d.replyWhenSuspended(c, nil, nil)
	}
	return nil
}
