package workflow

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestIsWorkflowStreamReservedName(t *testing.T) {
	for _, name := range []string{"__temporal_workflow_stream_publish", "__temporal_workflow_stream_poll", "__temporal_workflow_stream_offset", "__temporal_workflow_stream_custom"} {
		require.True(t, isWorkflowStreamName(name))
	}
	for _, name := range []string{"__temporal_", "__temporal_foo", "__internal", "events", "__temporal_workflow_stream"} {
		require.False(t, isWorkflowStreamName(name))
	}
}
