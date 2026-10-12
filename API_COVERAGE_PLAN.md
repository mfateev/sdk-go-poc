# Remaining workflow API coverage

The implementation uses regular Go SDK contracts with native context, goroutines and
channels, and the existing copied-byte host bridge:

1. External workflow signaling and cancellation: schedule before returning a
   repeatable Future; preserve namespace, workflow ID, run ID and SDK failures.
2. Local activities: execute on the host through SDK local-activity bindings;
   preserve markers, retries (including durable backoff), cancellation and codecs.
3. Sessions: use the SDK session-worker creation/completion activities and signal
   protocol. Keep session context/state inside the isolate, route activities to
   the resource queue, and register open-session metadata with the host SDK.
   Generate session IDs deterministically from execution identity and sequence.
4. Nexus: expose client/options and separate execution/result futures; retain
   the SDK cancellation policies and marker/history handling on the host.

SideEffect and MutableSideEffect are intentionally excluded. Nondeterministic
work belongs in host activities, including local activities.

Coverage checks exercise scheduling, synchronous/asynchronous callbacks,
cancellation, precise result types, failure identity, replay, and eviction with
late callbacks. Existing ordinary SDK workflows retain their behavior. Local
activity implementations require host registration; session IDs use original
run identity so synthetic offline replay IDs cannot change signal matching.

The compiled checker is `example/apicoverage/check`; saved server histories live
in `example/apicoverage/testdata`. The logger/metrics adapters and workflow interceptor chain are implemented;
see [configuration and tracing adapters](INTERCEPTORS.md).

## Observability

Printing and standard logging already use copied, one-way `isolate.Write` log messages. Worker log
handlers receive execution identity and replay metadata; the default SDK logger
suppresses replay output according to SDK configuration. Task/query fences and
shutdown drain pending messages; no logging acknowledgment resumes workflow code.
Operational visibility
is feature 10 in the runtime productization plan.

Isolate-owned implementations of the regular SDK GetLogger and
GetMetricsHandler interfaces use dedicated byte encodings to send records to host-owned logger/metric
backends, independently of Temporal DataConverter, payload codecs and encryption.
No backend object, network client or lock is shared with an isolate.

Tracing and application telemetry can now use `workflow.NewSink(op).Emit(bytes)`
with a matching `worker.RegisterSink` host handler. Operation codes are stable
application contracts in `0x00010000..0xfffeffff`; names are optional diagnostic
labels. Interceptors own serialization, independently of DataConverter and
codecs. The host supplies execution metadata and suppresses replay delivery
unless `SinkOptions.EnableReplay` is set. The shared runtime queue bounds
observations to 64 messages of at most 64 KiB each. Overflow drops messages;
missing handlers and backend panics remain host diagnostics without replies or
workflow failures. Query, validator, completion and shutdown fences drain writes.
Activities remain the mechanism for durable external effects. SDK-style logger,
metrics and OpenTelemetry v1/v2 and Datadog tracing adapters use these byte sinks.
OpenTracing uses the same private native provider and sink to preserve exact
span IDs.
Backend lifecycle, transport behavior and supported hooks are documented in
[INTERCEPTORS.md](INTERCEPTORS.md).

Reference: https://typescript.temporal.io/api/namespaces/workflow#proxysinks

## Completed interceptor API parity

The remaining SDK v1.49.0 outbound hooks are implemented: typed search-attribute
reads, typed/untyped upserts, memo upserts, signal-channel options, and previous-run
result/error getters. Native Go concurrency/time and SideEffect exclusions remain
explicit. Method-set conformance, compiled isolate checks, race/ownership checks
and saved server-history replay cover this contract. See the parity table and
metadata behavior in [INTERCEPTORS.md](INTERCEPTORS.md).

## Upstream unit-test migration

[TEST_PORTING.md](TEST_PORTING.md) lists every source unit test, its disposition,
new SDK coverage, and missing-feature decisions. The normal test suite enforces
that list. Compare it with the upstream checkout when changing SDK dependencies:

```sh
../golang-go/bin/go run ./internal/cmd/testports -upstream ../temporal-go-sdk
```

The user-selected context propagation and converter feature is implemented.
Marked factories construct native propagators inside each isolate; native
context-aware converters and `WithDataConverter` choose serialization per context.
Queries and validators use scratch instances, and futures preserve their scheduling
converter. The eight associated source entries are resolved in the checklist:
five native ports, one retained host activity-environment test with native boundary
coverage, and two obsolete cooperative deadlock-detector wrapper tests.
`example/contextpropagation/check` exercises the ports and saved server-history
replay at GOMAXPROCS 1/2/8. Live checks passed with workflow cache sizes 1000 and 0.

Custom failure conversion is also implemented using the existing Temporal
`converter.FailureConverter` interface. Marked native factories receive copied
configuration and the operation's data converter; fresh instances cover read-only
validation/query ownership. Host converters retain activity retry and client
decoding responsibilities. `example/failureconversion/check` ports
`TestCustomFailureConverter`, asserting custom error identity and one activity
attempt, and adds workflow/local/child/update/previous-run coverage, per-context
details, compressed live histories and replay. The startup protocol is now 5.


Generated Google protobuf values are implemented for the native workflow API
boundaries using the pinned SDK's binary and JSON converters. Canonical descriptors
stay process-owned; reflection/coder caches and pools are private, including scratch
conversion for read-only handlers. Generated type provenance is part of the current
POC interface. The API marker remains 1; markers can change freely
during development and do not select compatibility implementations.
Both original protobuf cases use host-only activity environments and are retained,
with native counterparts, compiled API/ownership checks, codecs and saved-history
replay. Gogo, dynamic messages, extensions, weak/opaque layouts and custom resolvers
remain separate scope.

Corrupt-signal handling is implemented for native typed channels, inbound payload
admission and byte-valued `NextSignal`. Undecodable values produce an error log
and increment `temporal_corrupted_signals`, then readers continue to the next
valid value. All five upstream receive/select/nonblocking/closed-channel tests
are covered in `example/signals/check`, including converter/interceptor adapters,
late registration, malformed envelopes and replay metric suppression. The live
server check verifies skipping and replays the recorded histories.

Native dynamic workflow/query/update handlers are implemented. Workers, replayers
and the native test facade register marked fallback workflows with
`converter.EncodedValues`. Named registrations take precedence; queries and
validators use the existing scratch owner, converters and inbound interceptor
chain. Dynamic updates retain acceptance ordering, failure conversion and
unfinished-handler policies. `example/dynamic/check/native_test.go` ports `TestDynamicWorkflows`
and covers arbitrary handler names, replacement and completed queries.
`example/dynamic/check` verifies compressed live histories and saved replay with
workflow cache sizes 1000 and 0. See [dynamic workflows and handlers](README.md#dynamic-workflows-and-handlers).

`workflow.GetRandomStream(ctx, name)` implements upstream's `WorkflowRandomStream`
(Source and Reader) with exact seed/byte/Uint64 goldens and stable unaligned
interleaving. Read-only snapshots include buffered bytes and captured handles.
A copied reset-aware identity reseeds handles in place before continuation;
late-created sources and child Continue-As-New runs use the proper identity.
All nine upstream unit cases are ported and mapped in TEST_PORTING.md. Live tests
cover reset and child/CAN with cache sizes 1000 and 0; ten saved histories verify
exact replay results at GOMAXPROCS 1/2/8. `samples-go-poc/random-streams`
demonstrates the Source/Reader API and rand.New facade. The math/rand/v2 library
is unchanged.
The isolate runtime adds ControlCall for input-service bookkeeping without
consuming default randomness; the compiler pins the additional host binding.

Native workflow streams are implemented in `contrib/workflowstreams`, including
its reserved handler namespace, batching, publisher deduplication, subscription
iterators, topic filtering, offset queries, response paging, truncation recovery
and continue-as-new state capture. All 28 associated upstream cases are mapped
to direct host ports and compiled native workflow tests. The live checker covers
compressed envelopes and subscriptions through rollover at cache sizes 1000/0;
saved histories replay at GOMAXPROCS 1/2/8. See
[workflow streams](contrib/workflowstreams/README.md).
