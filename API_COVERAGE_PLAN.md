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
The OpenTracing exporter needs a backend choice to preserve exact span IDs.
Backend lifecycle, transport behavior and supported hooks are documented in
[INTERCEPTORS.md](INTERCEPTORS.md).

Reference: https://typescript.temporal.io/api/namespaces/workflow#proxysinks
