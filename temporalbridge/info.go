package temporalbridge

import (
	"encoding/json"
	"github.com/mfateev/sdk-go-poc/workflow"
	"isolate"
)

func (d *definition) infoBytes() ([]byte, error) {
	info := d.env.WorkflowInfo()
	return json.Marshal(workflow.InfoSnapshot{Info: *info, Details: workflow.InfoDetails{
		BinaryChecksum: info.GetBinaryChecksum(), BuildID: info.GetCurrentBuildID(),
		HistoryLength: info.GetCurrentHistoryLength(),
		HistorySize:   info.GetCurrentHistorySize(), ContinueAsNewSuggested: info.GetContinueAsNewSuggested(),
		ContinueAsNewReasons:                 info.GetContinueAsNewSuggestedReasons(),
		TargetWorkerDeploymentVersionChanged: info.GetTargetWorkerDeploymentVersionChanged(),
	}})
}
func (d *definition) handleInfo(command *isolate.Command) error {
	raw, err := d.infoBytes()
	d.replyWhenSuspended(command, raw, err)
	return nil
}
