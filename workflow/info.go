package workflow

import (
	"context"
	"encoding/json"
	goWorkflow "go.temporal.io/sdk/workflow"
	"isolate"
)

// Info preserves SDK metadata and getters using a copied snapshot. Host SDK
// internals and protobuf objects never become workflow-owned shared objects.
type Info struct {
	goWorkflow.Info
	details InfoDetails
}

// InfoDetails supplies SDK getter values whose underlying fields are private.
type InfoDetails struct {
	BinaryChecksum, BuildID              string
	HistoryLength, HistorySize           int
	ContinueAsNewSuggested               bool
	ContinueAsNewReasons                 []goWorkflow.ContinueAsNewSuggestedReason
	TargetWorkerDeploymentVersionChanged bool
}
type InfoSnapshot struct {
	Info    goWorkflow.Info
	Details InfoDetails
}

func (i *Info) GetBinaryChecksum() string       { return i.details.BinaryChecksum }
func (i *Info) GetCurrentBuildID() string       { return i.details.BuildID }
func (i *Info) GetCurrentHistoryLength() int    { return i.details.HistoryLength }
func (i *Info) GetCurrentHistorySize() int      { return i.details.HistorySize }
func (i *Info) GetContinueAsNewSuggested() bool { return i.details.ContinueAsNewSuggested }
func (i *Info) GetContinueAsNewSuggestedReasons() []goWorkflow.ContinueAsNewSuggestedReason {
	return append([]goWorkflow.ContinueAsNewSuggestedReason(nil), i.details.ContinueAsNewReasons...)
}
func (i *Info) GetTargetWorkerDeploymentVersionChanged() bool {
	return i.details.TargetWorkerDeploymentVersionChanged
}

func GetInfo(ctx context.Context) *Info {
	if out := currentOutbound(ctx); out != nil {
		return out.GetInfo(ctx)
	}
	return getInfo(ctx)
}
func getInfo(ctx context.Context) *Info {
	if ctx == nil {
		panic("workflow: nil context")
	}
	call := isolate.Call
	if isolate.IsReadOnly() {
		call = isolate.ReadOnlyCall
	}
	raw, err := call(OpInfo, nil)
	if err != nil {
		panic(err)
	}
	var snapshot InfoSnapshot
	if err := json.Unmarshal(raw, &snapshot); err != nil {
		panic(err)
	}
	return &Info{Info: snapshot.Info, details: snapshot.Details}
}
