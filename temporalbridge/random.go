package temporalbridge

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"errors"
	"github.com/mfateev/sdk-go-poc/workflow"

	goWorkflow "go.temporal.io/sdk/workflow"
)

// OriginalRunID comes from history, unlike the replay worker's synthetic
// namespace, workflow ID and RunID. Temporal run IDs are UUIDs; no worker-local
// identifiers are needed to distinguish execution streams.
// Keep the encoding and domain stable: changing either changes replayed bytes.
func workflowRandomSeed(info *goWorkflow.Info) [32]byte {
	identity := []byte("isolate/crypto-rand/v1")
	runID := workflowRandomRunID(info)
	identity = binary.LittleEndian.AppendUint64(identity, uint64(len(runID)))
	identity = append(identity, runID...)
	return sha256.Sum256(identity)
}

func workflowRandomRunID(info *goWorkflow.Info) string {
	if info.OriginalRunID != "" {
		return info.OriginalRunID
	}
	return info.WorkflowExecution.RunID
}

func (d *definition) controlBytes() []byte {
	raw, err := json.Marshal(workflow.WorkflowControl{RandomRunID: d.randomRunID, Canceled: d.canceled})
	if err != nil {
		d.failTask(err)
	}
	return raw
}

func (d *definition) updateRandomIdentity() {
	runID := d.pendingRandomRunID
	if !d.started {
		d.randomRunID = runID
		return
	}
	if runID == d.randomRunID {
		return
	}
	d.randomRunID = runID
	if d.cancelWaiter != nil {
		d.pending = append(d.pending, reply{command: d.cancelWaiter, payload: d.controlBytes()})
		d.cancelWaiter = nil
		return
	}
	// Cancellation and reset may have been queued in the same history batch.
	for i := range d.pending {
		if d.pending[i].command.Op == workflow.OpWorkflowCancel {
			d.pending[i].payload = d.controlBytes()
			return
		}
	}
	d.failTask(errors.New("workflow control service is not suspended"))
}
