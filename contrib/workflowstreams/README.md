# Native workflow streams

Import `github.com/mfateev/sdk-go-poc/contrib/workflowstreams` to host a durable
publish/subscribe log inside a native isolate workflow. This package is part of
the POC module and uses the upstream cross-language workflow stream protocol.

```go
//temporal:workflow
func Events(ctx context.Context, state *workflowstreams.WorkflowStreamState) error {
    stream, err := workflowstreams.NewWorkflowStream(ctx, state)
    if err != nil {
        return err
    }
    if err := stream.Topic("events").Publish("started"); err != nil {
        return err
    }
    <-workflow.GetSignalChannel[[]byte](ctx, "rollover")
    return stream.NewContinueAsNewError(ctx, Events, func(state *workflowstreams.WorkflowStreamState) []any {
        return []any{state}
    })
}
```

Construct the stream once per run. Pass nil state on the first run; carry the
snapshot in the next run's arguments. `NewContinueAsNewError` detaches waiting
polls, rejects new polls, waits for accepted update handlers to finish, processes
admitted publish signals and captures the log, offsets and publisher sequence
numbers. Return its error from the workflow. Publishers inactive for 15 minutes
are omitted from the snapshot. For a custom publisher TTL, use `DetachPollers`,
`workflow.WaitHandlersFinished`, `GetState(ttl)` and `workflow.NewContinueAsNewError`.

The stream synchronizes native goroutines with a mutex and notification channels.
Pending polls block without creating timers, and wake on publish, truncation,
detach or cancellation. Queries and validators read the runtime's frozen snapshot.

Use the client from host code or an activity:

```go
stream := workflowstreams.NewClient(temporalClient, workflowID, workflowstreams.Options{})
defer stream.Close(ctx)
stream.Topic("events").Publish("hello", false)
if err := stream.Flush(ctx); err != nil {
    return err
}
for item, err := range stream.Subscribe(ctx, workflowstreams.SubscribeOptions{
    Topics: []string{"events"},
}) {
    if err != nil {
        return err
    }
    var value string
    if err := converter.GetDefaultDataConverter().FromPayload(item.Data, &value); err != nil {
        return err
    }
    fmt.Println(item.Offset, item.Topic, value)
}
```

`NewClientFromActivity(ctx, opts)` targets the activity's parent workflow. Client
uses network operations and belongs to host code. `WorkflowStream.Topic` publishes
locally from workflow code. `TopicHandle.Subscribe(ctx, fromOffset)` filters one
topic; `Client.GetOffset` queries the current global offset.

Subscriptions follow continue-as-new, recover from truncation and end on a
terminal workflow. Break the iterator or cancel its context to stop polling.
`FromOffset: 0` starts at the earliest retained entry. Topic filters preserve
global offsets. Poll responses page at approximately 1 MB, with immediate polling
when more entries are ready. `Truncate(offset)` discards entries before that
position; truncating beyond the log returns `TruncateOutOfRange`.

| Option | Default | Purpose |
| --- | --- | --- |
| `Options.BatchInterval` | 2s | Automatic publisher flush interval |
| `Options.MaxBatchSize` | 0 | Flush when this many items are buffered; zero disables |
| `Options.MaxRetryDuration` | 10m | Retry a pending batch before `FlushTimeoutError`; keep below publisher TTL |
| `Options.PayloadConverters` | SDK default set | Codec-free per-item serialization |
| `SubscribeOptions.PollCooldown` | 100ms | Delay between polls unless another response page is ready |

Use `WithPayloadConverters` for workflow-side item serialization. Prebuilt
`*commonpb.Payload` values bypass item conversion. Encryption and compression
codecs configured on the Temporal client/worker apply once to the signal/update
envelope. The handler namespace `__temporal_workflow_stream_` is permitted for
signals, updates and queries; other reserved names remain rejected.

Run with the custom toolchain:

```sh
../golang-go/bin/tgo test ./contrib/workflowstreams ./workflow ./example/workflowstreams/check
STREAMS_DEV_SERVER=1 ../golang-go/bin/tgo test -count=1 ./example/workflowstreams/check -run TestLiveWorkflowStreams
```

The live checker covers subscription through rollover, truncation recovery,
paging, compressed envelopes, terminal completion, saved-history replay and
workflow cache sizes 1000 and 0. Saved histories replay at GOMAXPROCS 1/2/8.
