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
in `example/apicoverage/testdata`. The observability work below remains planned.

## Observability follow-up

Printing and standard logging already use copied host log messages. Worker log
handlers receive execution identity and replay metadata; the default SDK logger
suppresses replay output according to SDK configuration. Operational visibility
is feature 10 in the runtime productization plan.

Add isolate-owned implementations of the regular SDK GetLogger and
GetMetricsHandler interfaces. They serialize records to host-owned logger/metric
backends. No backend object, network client or lock is shared with an isolate.

For tracing and application telemetry, consider the TypeScript SDK's sinks model:
worker-registered host handlers, serialized one-way messages, no application
return value, and replay suppression by default. Delivery is best effort and
must not change workflow decisions. Bound buffering and define overflow/error
handling before exposing a general sink API. Activities remain the mechanism
for durable external effects.

Reference: https://typescript.temporal.io/api/namespaces/workflow#proxysinks
