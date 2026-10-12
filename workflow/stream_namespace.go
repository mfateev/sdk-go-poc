package workflow

import "strings"

// The cross-language workflow stream protocol owns this reserved sub-namespace.
func isWorkflowStreamName(name string) bool {
	return strings.HasPrefix(name, "__temporal_workflow_stream_")
}
