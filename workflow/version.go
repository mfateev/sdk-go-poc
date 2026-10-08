package workflow

import (
	"context"
	"encoding/json"
	goWorkflow "go.temporal.io/sdk/workflow"
	"isolate"
)

type Version = goWorkflow.Version

const DefaultVersion = goWorkflow.DefaultVersion

type VersionRequest struct {
	ChangeID string  `json:"change_id"`
	Min      Version `json:"min"`
	Max      Version `json:"max"`
}

// GetVersion delegates marker creation, replay lookup, and range checks to the
// pinned SDK. Cancellation does not suppress history compatibility decisions.
func GetVersion(ctx context.Context, changeID string, minSupported, maxSupported Version) Version {
	if ctx == nil {
		panic("workflow: nil context")
	}
	request, err := json.Marshal(VersionRequest{ChangeID: changeID, Min: minSupported, Max: maxSupported})
	if err != nil {
		panic(err)
	}
	raw, err := isolate.Call(OpGetVersion, request)
	if err != nil {
		panic(err)
	}
	var version Version
	if err := json.Unmarshal(raw, &version); err != nil {
		panic(err)
	}
	return version
}

// IsReplaying has the SDK meaning; use it for logging, never to alter commands.
func IsReplaying(ctx context.Context) bool {
	if ctx == nil {
		panic("workflow: nil context")
	}
	raw, err := isolate.Call(OpIsReplaying, nil)
	if err != nil {
		panic(err)
	}
	var replay bool
	if err := json.Unmarshal(raw, &replay); err != nil {
		panic(err)
	}
	return replay
}
