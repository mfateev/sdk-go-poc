// Package workflowstreams provides a durable publish/subscribe log inside a
// native isolate workflow. Construct a WorkflowStream with context.Context in
// a marked workflow, and use Client from host code or activities to publish
// batches and subscribe with a range-over-func iterator.
//
// Named topics share global offsets. The stream deduplicates publisher batches,
// pages poll responses, supports truncation and captures state for continue-as-new.
// Signals, updates, queries and base64 protobuf item envelopes use the upstream
// cross-language protocol. Payload codecs apply once to each batch envelope.
package workflowstreams
