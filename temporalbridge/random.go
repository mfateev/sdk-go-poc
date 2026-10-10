package temporalbridge

import (
	"crypto/sha256"
	"encoding/binary"

	goWorkflow "go.temporal.io/sdk/workflow"
)

// OriginalRunID comes from history, unlike the replay worker's synthetic
// namespace, workflow ID and RunID. Temporal run IDs are UUIDs; no worker-local
// identifiers are needed to distinguish execution streams.
// Keep the encoding and domain stable: changing either changes replayed bytes.
func workflowRandomSeed(info *goWorkflow.Info) [32]byte {
	identity := []byte("isolate/crypto-rand/v1")
	runID := info.OriginalRunID
	if runID == "" {
		runID = info.WorkflowExecution.RunID
	}
	identity = binary.LittleEndian.AppendUint64(identity, uint64(len(runID)))
	identity = append(identity, runID...)
	return sha256.Sum256(identity)
}
