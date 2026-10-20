# Reimplementing the native Temporal SDK

This guide defines the SDK side of the POC. Read the Go fork's
[core implementation guide](https://github.com/mfateev/golang-go/blob/task/modify-go-runtime-for-isolates/doc/isolates/IMPLEMENTATION.md)
for package-state descriptors, owner checks, native scheduling and cleanup. This
SDK uses public `isolate` APIs and the generic build manifest; it cannot set raw
owners, register descriptors itself or borrow host objects through typed calls.

[SDK_REFERENCE.md](SDK_REFERENCE.md) specifies public behavior,
[TRANSPORT.md](TRANSPORT.md) specifies binary fields, and [TESTING.md](TESTING.md)
provides executable gates. This is the current design, without migration phases.

## 1. Build driver and graph policy

Create `cmd/tgo` as a small Go command backed by `internal/toolchain`. Resolve its
bundled `bin/go` from the executable path; permit an explicit `TGO_GOROOT` local
override. Ignore caller GOROOT, set `GOTOOLCHAIN=local` and select isolates before
graph loading. Reject explicit noisolates for build/test/run. Forward commands
without native discovery normally. Respect `-C`, graph-affecting flags, overlays,
build tags, module files and test identities in both discovery and compilation.

Use `go list -deps -json` (`-test` for tests). Parse build-selected files and find
exact `//temporal:workflow` directives on concrete top-level nonvariadic functions.
Reject methods/generic entries, cgo entries and SDK workflow.Context parameters.
Support zero results, error, or result+error. Discovery is generic enough to mark
support factories whose first argument is bytes rather than workflow context;
worker registration separately requires standard context.Context first.

Generate in-package adapters so unexported argument/result types work. Emit a
function-value accessor and invoker using typed locals and `isolate.Value` slots.
Decode every argument, call the original directly, return its error without
encoding a result on failure, and encode zero/one result on success. Keep
`//go:linkname` declarations limited to generated entry wiring. The Go core
validates these sources and owns registration and compiler descriptors.

Select ordinary workflow dependency state, the workflow support root, configured
factory graphs and supported standard-library state. Keep host worker/client/
activity services initialized on the host. Private graph traversal stops at those
services but includes workflow-reachable protobuf/generated packages, converter,
UUID and selected observability state. Do not initialize descriptors reachable
only through host SDK service imports. Mark standard/converter/UUID/private-proto/
observability library state for scratch rebuilding.

Validate owning modules and source versions: protobuf v1.36.11 without replacement,
OTel trace v1.44.0 without replacement, and Temporal SDK official owning module or
filesystem checkout with required bindings. Reject vendoring, nested-module
impersonation and replacement modules. Issue unsafe audits for private protobuf
implementation/protobufcopy packages and physical generated `.pb.go` files.
Other application source files in the same package get no generated-file audit.

Small library overlays pin protobuf ordering/default environment behavior, add
scratch snapshots before captured `proto.Clone` and replace OTel zero-ID sentinel
reads with literal comparisons. Preserve generated message/registry method bodies.
Use temporary module mirrors/workspaces; do not edit module cache or checked-in
sources. Nested tgo builds restore the original workspace before their own graph
selection. Hash effective content and honor Go's dependency cache reuse.

Forward build/test with a temporary generic manifest. For run, build the same
marked temporary executable, then execute it with program arguments. Install
forwards to ordinary Go without generating entry metadata. Marked coverage mode
is unsupported. Keep internal/external/test-only identities distinct and register
entries before TestMain.

Sources: `internal/toolchain/{driver,functions,selection,dependencies,adapters}.go`.

## 2. Host worker and binding layer

Wrap the host worker/replayer registration API. `isolate.LookupFunction` identifies
marked entries; unmarked functions and all activities go to the ordinary SDK.
Keep aliases/options on the host and resolve them when scheduling. For native
workflows require context.Context first and BlockWorkflow panic policy. A host
client starts by registered string name, not a native function reference.

Define a factory implementing host SDK WorkflowDefinition creation. Each execution
snapshots resources, data/failure converters, propagators and interceptors.
Compose compiler state through ProgramWithSupports, pass noncapturing dispatchers
and copied configuration, and instantiate everything privately. Host callbacks,
converters, exporters and locks must not enter the application owner.

Install wrapper registries for worker and replay plugins, including client-inherited
plugins. Effective worker configuration comes from WorkerConfigurationProvider;
custom wrappers need their construction options when no provider exists. Plugin
registration must pass through native detection, not bypass it on the underlying
worker. Replayer activity registrations retain alias metadata without running
activities during replay.

The matching Temporal SDK fork supplies binding version 2:

- WorkflowDefinition/WorkflowEnvironment hooks for callbacks and task starts.
- RegisterResetHandler with copied NewRunID/BaseRunID/EventID after host identity
  changes. Queue identity for the next private task boundary.
- Borrowed previous completion/failure metadata, serialized before owner crossing.
- Recorded memo converter policy, not an unconditional serializer choice.
- Canonical typed-search-attribute update entries, with copied list values and
  deletion markers, instead of reflecting into private SDK maps.
- Worker/replayer plugin registry overrides installed before concurrent use.

These bindings expose host operations/data; they do not authorize shared mutable
objects. `worker.GetIntegrationInfo` reports actual module/build/runtime contracts.
Source pins, rather than version numbers alone, identify the required implementation.

Sources: `worker`, `temporalbridge`, host SDK `internalbindings/workflow_integration.go`.

## 3. Per-execution protocol and task loop

Validate runtime metadata/API/determinism markers 1 and binding version 2 before
creating a native definition. Startup request/reply uses protocol 7's 20-byte
header, validated before invoking factories or decoding input. [Transport](TRANSPORT.md)
contains the current operation numbers, messages and scalar framing.

Execute installs host query/update/cancel/signal/reset routers immediately and
queues input. History callbacks append copied replies/events or retire cells;
they never directly run normal workflow continuations. Signals decode codec
wrappers on the host and retain opaque headers/payload values for the receiver.

At Workflow Task start:

```text
snapshot host task budget and current history time
if no instance:
    NewContext(program, deterministic, seed(original run), history time,
               timer opcode, runtime limits, startup logging)
    Start; perform startup handshake and private factory setup
else:
    advance history time; stage reset identity
repeat:
    service all emitted commands and observation writes
    suspend until token/eligible work is idle
    deliver ordered pending replies, cancellation, signals and handler input
    admit inbound delivery, then priority writable handlers, then normal work
until transport handshakes settle and no new command is produced
retain cache state or publish completion after the native cleanup fence
```

The host must service commands while waiting for suspension. Replies delivered
while fenced only queue members. Incoming services proactively fill registered
signal channels and future result channels before application continuations.
Registration drains previously queued signals before returning. Accepted update
handlers get required priority; validators run read-only and are skipped for
historically accepted replay updates. Phase loops must include immediate replies
and outstanding joins, not only durable server callbacks.

Maintain per-command retireable call cells. A callback captures its cell, and
close clears command/cancel references and marks it done. Late callbacks cannot
resurrect an instance or reply twice. Differentiate schedule acknowledgment from
operation completion. Incomplete futures park; completed results remain repeatable
and decode into fresh local values. Cancellation behavior follows each operation's
SDK policy, not blanket abandonment of its result future.

A blocked instance with no external pending calls is a native deadlock under this
contract. SDK deadlock timeout bounds a dispatch pass; explicit MaxTaskDuration
bounds the complete task including initialization. Capture TaskDiagnostics before
teardown. Drain Writes at startup, tasks, queries and final cleanup.

Sources: `temporalbridge/bridge.go`, operation files, `workflow/workflow.go`.

## 4. Payload, failure and factory ownership

Inside the workflow, create the default or marked DataConverter before argument
conversion. Generated invoker slots hold local typed values only. Serialize input,
activity/child arguments, results, signals, updates, queries, memo and failure
details under the active owner. The default supports null/bytes/JSON and Google
protobuf. A custom factory receives copied bytes; a per-context converter changes
the operation serializer locally.

Host codecs process opaque RawValue payloads once, including compressed/encrypted
failure attributes and cause chains. Host conversion must not decode application
values merely to transport them. Preserve message metadata, unknown binary fields,
precise integers, nil/absent values, headers and child initiation error tags.
Cardinality-changing codecs and external payload references are unsupported at
the native boundary. Legacy gogo is outside the default native serializer contract.

DataConverter and FailureConverter factories, context propagators and interceptor
factories are marked support handles. Failure conversion gets the operation's
selected converter. Query/validator calls construct fresh scratch instances of
all relevant chains. Host matching serializers/codecs/propagators remain necessary
for client and activity interoperability. Headers bypass application serializers;
observations bypass converters/codecs entirely.

Private protobuf initializes GlobalFiles/GlobalTypes, generated descriptor graphs,
MessageInfo and coder caches separately in each workflow/scratch service. Descriptor
literal strings are shared read-only. Custom ProtoReflect implementations, dynamic
messages, resolvers and local registrations are normal private code. Host-only
registrations/extensions are invisible; Any requires a linked private type.

Captured workflow messages can carry workflow-owned reflection caches. Before
read-only clone/conversion, `protobufcopy` snapshots fields, unknowns and extensions
into scratch representation and binds results to scratch descriptors. Preserve
read-only checks for custom callbacks; cloning is not a host service or privilege.

Sources: `workflow/{converter,context_propagator,failure_converter,protobuf}.go`,
`temporalbridge` counterparts, `internal/{payloadcodec,failurecodec,headerwire}`,
`protobufcopy` and toolchain overlays.

## 5. Native APIs and handler rules

Implement SDK operation families over binary Calls, with typed local futures:
activities/local activities, child scheduling/execution/results/signals, external
commands, Nexus execution/result tokens, sessions, versioning, Continue-As-New,
visibility/metadata and previous-run data. [SDK_REFERENCE.md](SDK_REFERENCE.md)
defines options, cancellation, error identities and examples.

Use ordinary Go context/go/channels/select/sync/time. Future.Get accepts a context
and decodes repeatably; ToChannel is the native select adapter. Signals have a
registered element type and get decoded proactively. Corrupt signals are skipped
with copied log/metric observations; they must not poison the task. Coordinate
signal goroutines separately from accepted update-handler draining.

Queries/validators use dedicated ReadOnlyCall services while normal continuations
keep FIFO positions. Handler writes reject before mutation; scratch caches and
scratch-owned nonblocking locks work. Child creation/waits/durable operations fail
within that request. Completed workflows retain query state behind FenceReadOnly
until eviction; publish retained completion without admitting normal continuations.

Dynamic registrations are fallbacks after named handlers. They receive local
EncodedValues and obey the same converters/interceptors/read-only rules. Reserved
handler names remain denied except the workflow-stream namespace. Native stream
state uses a mutex plus notifications, signal batch deduplication, update long polls,
query snapshots, pagination/truncation and explicit continuation state. Drain
accepted handlers and admitted signals before capturing a continuation snapshot.

Named random streams are independent from the runtime shared stream. Preserve
SDK ChaCha8/distribution golden behavior and namespace separation; seed with
SHA-256 of `temporal.sdk.random.v1`, NUL, reset-aware run ID, NUL and exact name; deliver reset
identity before creating subsequent streams/generated child IDs. No draw/identity
message adds a history event. SideEffect/MutableSideEffect are excluded.

Sources: `workflow`, `temporalbridge`, `contrib/workflowstreams`.

## 6. Failures, cache teardown and observation

Application returned errors/failures become execution results through failure
conversion. Runtime ownership/effect/quota faults and unrecovered native panics
revoke state and follow BlockWorkflow's Workflow Task failure path. They must not
be turned into execution completion by worker panic policy. Query/validator write
rejection fails only its request. Ordinary recovered application panic stays Go.

Definition close retires callbacks and pending queues, kills the group, waits for
cleanup or reports pending diagnostics, and releases state roots. Eviction does
not cancel server-side timers/activities. Workflow context cancellation is a
separate cooperative signal. Automatic worker-stop cache teardown and hard kill
of non-yielding code are outside the POC contract.

Resource observers receive copied usage/identity/failure snapshots. Maximum memory
and goroutines apply before init; active-task/progress watchdogs use host monotonic
time and exclude cached idle time. Permanent limit faults do not emit server work
cancellations. See the Go resource contract and SDK resource options.

Logs, metrics, custom sinks and tracing send best-effort one-way byte Writes.
Replay suppression is host/configuration policy; delivery/errors/drops never feed
workflow decisions. Format values inside the owner. Native span providers preserve
IDs/parent links; host exporters do I/O. [INTERCEPTORS.md](INTERCEPTORS.md) defines
the hook surface and supported tracing adapters.

## 7. Tests and distribution

Reuse the host SDK TestWorkflowEnvironment for virtual time, scheduling, mocks and
activities while executing marked workflows in real compiled isolates. Host mock
callbacks receive context.Context but cannot call native workflow APIs;
ReturnIsolate selects a marked replacement. Test references cross as copied IDs
and are enabled only by the host test environment. Complete root/child query state
survives until cleanup. Registration precedes mock controls needing a workflow.

Build compiler/runtime from SDK source pins, then compile the SDK-owned driver
beside `bin/go`. Record source/build metadata. Release archives contain the full
toolchain, checksums and relocatable launcher. Installer verification and smoke
compilation are packaging gates, separate from runtime/SDK/replay gates.

[TESTING.md](TESTING.md) covers package/race suites, compiled fixtures, saved
histories, opt-in live tests with cache sizes 1000/0 and resource soak. Compare
computed results/payloads separately when the SDK replayer compares only command
shape. Native Linux/macOS arm64/amd64 gates must pass for the tested revision.
