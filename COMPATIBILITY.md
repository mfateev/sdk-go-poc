# Supported native SDK scope

The current host dependency is `go.temporal.io/sdk v1.50.0`, replaced by the
matching `../temporal-go-sdk` fork from `toolchain/go-source.json`. The fork adds
binding version 2. Protobuf v1.36.11 and OTel trace v1.44.0 are pinned by the SDK
build driver. Runtime metadata/API/determinism markers are 1 and startup protocol
is 7. Matching markers alone do not identify source or prove replay compatibility.

| Implemented area | Current behavior |
| --- | --- |
| Native execution | context.Context, go/channels/select/sync, history time and deterministic supported randomness/maps |
| Durable operations | Activities, local activities, children, external signals/cancellation, sessions, Nexus, continuation and version markers |
| Handlers | Typed signals, queries, updates/validators, dynamic fallback, handler draining and completed queries |
| Conversion | Native converter/propagator/failure factories, per-context serialization, Google protobuf values and host payload codecs |
| Metadata | Typed/untyped search attributes, memo, workflow info, previous-run results/failures and reset-aware identity |
| Observation | Native interceptor chains, logging, metrics, sinks and private tracing providers with host exporters |
| Workflow streams | Native stream server/client protocol, batching, long polls, truncation and continuation state |
| Tests/plugins | Isolate test environment, mocks/replacements/test references, worker and replayer plugin registration |

Ordinary SDK workflows, clients, activities and host worker services retain the
host SDK path. Native workflow entries use standard contexts and marked concrete
functions; host clients start them by registered names. Activities may include
context.Context first or omit it. Method/generic/variadic workflow entries are
unsupported. Native concurrency APIs replace SDK Go/Channel/Selector/Await/timer
facades, and SideEffect/MutableSideEffect are excluded; use activities for external
work. [SDK_REFERENCE.md](SDK_REFERENCE.md) gives the exact per-operation behavior.

Current limits include external payload-storage references at the native wire
boundary, cardinality-changing codecs, variadic handlers, the default serializer's
legacy gogo values, complete hostile-code containment, hard kill of non-yielding
code, automatic worker-stop cache teardown and universal math/map-key portability.
Native timer Stop/Reset suppress local delivery without canceling already issued
durable host timers. Private protobuf state has material startup/retained cost.
The unported samples catalog describes application coverage independently of SDK
feature availability.

The executable test inventory covers a pinned v1.49.0 source-test set, not every
new v1.50.0 field/hook. Retained host tests and method-shape checks are evidence
for their specific paths, not proof of complete native behavioral parity. See
[TEST_COVERAGE.md](TEST_COVERAGE.md) and [TESTING.md](TESTING.md) for scope and gates.
The POC maintains one current implementation without old protocol shims.
