# Temporal isolate SDK POC

See [workflow interceptors and observability](INTERCEPTORS.md) for native SDK-style
interception, logging/metrics, OpenTelemetry v1/v2, Datadog and OpenTracing configuration.

## Determinism regression corpus

`example/determinism.DeterminismWorkflow` records observable ordering for native
goroutines, maps, selects, `sync.Map.Range`, `iter.Pull2`, top-level random streams,
floating-point distributions, standard context cancellation, and native timers.
Its activity input and result retain the full trace. The runtime's deterministic
default time zone is UTC. Named time-zone database lookups are rejected;
explicit fixed zones or supplied zone data remain valid.

### Deterministic randomness and security

Inside deterministic isolates, `crypto/rand.Read`, the default `crypto/rand.Reader`,
`crypto/rand.Int` and `crypto/rand.Text` use a replay-seeded ChaCha8 stream.
Top-level `math/rand`, `math/rand/v2` and native `select` shuffle decisions share
that stream. Calls consume it in deterministic dispatch order; a select can
therefore change the bytes returned by a later random call. Explicitly created
random generators keep their own supplied seeds. Map hashing keeps Go's internal
entropy, while supported map iteration uses canonical key order.

The Temporal host derives the seed from the original run ID recorded in history,
before package initialization. The same run reproduces its bytes after eviction
or replay; distinct run identities get different seeds.
UUID v4 generation, including `github.com/google/uuid`, can use the default reader
inside workflows. Queries and update validators use scratch random state and
cannot advance the workflow stream. Their generated IDs are not guaranteed unique
across requests. Outside isolates, `crypto/rand` retains normal OS randomness.

**Security:** isolate random bytes are predictable because the seed is derived
from public execution identifiers. They are suitable for replay-stable IDs, but
must not be used for encryption keys, authentication tokens, passwords, secret
material, or encryption nonces requiring freshness across replays. They provide
no cryptographic entropy or FIPS security guarantee, despite the `crypto/rand`
API names. Generate secrets and perform encryption in host activities or host
codecs, where normal cryptographic randomness remains available. Cryptographic
key generation and `crypto/rand.Prime` remain unsupported inside isolates.

Hosts using isolates directly can supply `isolate.Config.RandomSeed` (32 bytes).
Its default is all zeros: two instances with the same seed and call sequence
produce identical bytes, so hosts generating IDs must supply a distinct,
replay-stable execution seed. This POC's shared stream replaces its earlier
independent random/select sequences; earlier recorded sequences are incompatible.

`example/random.RandomWorkflow` exercises UUID v4, pooled UUID generation,
`crypto/rand`, both math/rand APIs and select before and after an activity wait.
Its checker validates the recorded completion as well as query isolation,
eviction and GOMAXPROCS 1/2/8 in fresh processes:

```sh
../golang-go/bin/go test ./example/random/check
../golang-go/bin/go build -o /tmp/isolate-random-check ./example/random/check
/tmp/isolate-random-check -history example/random/testdata/history.json
```

Build the checker with the custom toolchain and replay the saved history in
fresh processes from this repository:

```sh
../golang-go/bin/go build -o /tmp/isolate-determinism-replay ./example/determinism/check
GOMAXPROCS=1 /tmp/isolate-determinism-replay
GOMAXPROCS=2 /tmp/isolate-determinism-replay
GOMAXPROCS=8 GODEBUG=cpu.all=off TZ=America/Los_Angeles /tmp/isolate-determinism-replay
```

To record an additional history with the example worker and a running local
Temporal server:

```sh
temporal workflow execute --workflow-id isolate-poc-determinism --type DeterminismWorkflow --task-queue isolate-poc --input 8
temporal workflow show --workflow-id isolate-poc-determinism --output json > /tmp/isolate-determinism-history.json
/tmp/isolate-determinism-replay -history /tmp/isolate-determinism-history.json
```

The workflow compares its freshly computed trace against the recorded activity
result; the checker also compares every completion observation against history.
This is necessary because Temporal's SDK replay checks do not compare activity
input payloads. A negative test corrupts both recorded results and verifies that
replay still rejects the changed trace.
The current shared-stream baseline is `history-shared-random.json`. The earlier
`history.json` is retained, and a negative test confirms its independent-stream
observations are incompatible with this deliberate POC change.
The compiler repository's `isolate-determinism.yml` runs the
same fixture on native Linux and macOS arm64/amd64, alongside runtime/compiler,
SDK/sample, and repeated race tests. A fixture must be kept when runtime behavior
changes; replacing it would hide an incompatible replay change.

This module adapts statically linked Go isolates to the Temporal Go SDK's
`WorkflowDefinitionFactory` boundary, the same level used by the Go bridge for
Temporal's PHP SDK. It needs the compiler and runtime from the
[`mfateev/golang-go` fork](https://github.com/mfateev/golang-go/tree/task/modify-go-runtime-for-isolates).
Clone that fork and this repository as siblings, then build with the fork's
`bin/go`:

```sh
git clone --branch task/modify-go-runtime-for-isolates https://github.com/mfateev/golang-go.git
git clone --branch task/modify-go-runtime-for-isolates https://github.com/mfateev/sdk-go-poc.git
cd golang-go/src && ./make.bash && cd ../../sdk-go-poc
export GOCACHE="$(cd .. && pwd)/go-build-cache"
mkdir -p "$GOCACHE"
```

The Go fork needs a Go 1.26 or newer bootstrap toolchain. The
[`samples-go-poc` README](https://github.com/mfateev/samples-go-poc/tree/task/modify-go-runtime-for-isolates)
has complete setup instructions for a new Linux or macOS machine. The source
build manages its own bootstrap cache; `GOCACHE` keeps subsequent POC builds
separate from your usual Go cache for targeted cleanup.

Build the example from this directory:

```sh
../golang-go/bin/go build -o worker ./example/worker
```

Run `./worker` against a local Temporal server (or set `TEMPORAL_ADDRESS`). It
registers workflow types `IsolateOrder`, `IsolateEcho`, `IsolateTypedEcho`,
`IsolateSignal`, `IsolateClock`, and `PlainEcho` on task queue `isolate-poc`,
plus the host activity `echo`.
`IsolateOrder`, `IsolateEcho`, and `IsolateTypedEcho` are separate named
functions in the same `example/order` package. Each execution has its
own isolate state. The worker also registers an ordinary `PlainEcho` workflow
with Temporal's unchanged `worker.RegisterWorkflow` API.
`IsolateOrder` takes one `[]byte` argument and returns it after an activity and a
durable one-second timer.
`IsolateEcho` prefixes its input with `registered:`.
`IsolateTypedEcho` takes a struct and a string and returns a struct.
`IsolateSignal` returns the bytes from its next signal.
`IsolateOrder` uses native `time.Sleep` and checks elapsed isolate time after
the host delivers its durable timer event.
`IsolateClock` uses native `time.Now` and `time.NewTimer` and returns the exact
timestamps it observed. The clock replay command compares the replayed result
with the completion payload in history byte for byte.

The order, signal, and clock examples completed on Temporal CLI 1.9.1's local
development server, and their exported histories replayed in fresh processes.
The typed isolate workflow and ordinary `PlainEcho` workflow also completed on
that server together; the typed workflow's exported history replayed in a fresh
process.
To repeat the live check, start `temporal server start-dev --headless` and run
`./worker` in another terminal. Then run:

```sh
temporal workflow execute --workflow-id isolate-poc-order --type IsolateOrder --task-queue isolate-poc --input aGVsbG8= --input-base64 --input-meta encoding=binary/plain
temporal workflow execute --workflow-id isolate-poc-echo --type IsolateEcho --task-queue isolate-poc --input aGVsbG8= --input-base64 --input-meta encoding=binary/plain
temporal workflow execute --workflow-id isolate-poc-typed-echo --type IsolateTypedEcho --task-queue isolate-poc --input '{"Name":"world"}' --input '"!"'
temporal workflow execute --workflow-id isolate-poc-inferred-activity --type IsolateInferredActivity --task-queue isolate-poc --input 5
temporal workflow execute --workflow-id isolate-poc-typed-activity --type IsolateTypedActivity --task-queue isolate-poc --input '"world"'
temporal workflow execute --workflow-id isolate-poc-plain-echo --type PlainEcho --task-queue isolate-poc --input '"hello"'
temporal workflow start --workflow-id isolate-poc-signal --type IsolateSignal --task-queue isolate-poc
temporal workflow signal --workflow-id isolate-poc-signal --name ready --input c2lnbmFsLXJlc3VsdA== --input-base64 --input-meta encoding=binary/plain
temporal workflow result --workflow-id isolate-poc-signal
temporal workflow execute --workflow-id isolate-poc-clock --type IsolateClock --task-queue isolate-poc
temporal workflow execute --workflow-id isolate-poc-concurrent --type IsolateConcurrent --task-queue isolate-poc
```

Export and replay a completed history with a fresh isolate process:

```sh
../golang-go/bin/go build -o replay ./example/replay
temporal workflow show --workflow-id isolate-poc-order --output json > /tmp/isolate-order-history.json
./replay IsolateOrder /tmp/isolate-order-history.json
temporal workflow show --workflow-id isolate-poc-clock --output json > /tmp/isolate-clock-history.json
./replay IsolateClock /tmp/isolate-clock-history.json
```

Run the local bridge driver without a server:

```sh
../golang-go/bin/go build -o /tmp/isolate-temporal-driver ./example/driver
/tmp/isolate-temporal-driver
```

`workflow` is the API imported by isolate code. `temporalbridge` stays in the
host and depends on the pinned Temporal Go SDK. The adapter enables
`isolate.Config.Deterministic`: native goroutines share an execution token with
FIFO readiness order within each priority,
select polling is reproducible, and string/integer map range is canonical.
An exact suspend fence batches history replies and services all concurrent
commands before ending each workflow task. Unsupported map key kinds fail
closed; supported `sync.Map.Range` keys and `iter.Pull` are deterministic. General I/O
containment and native cross-architecture replay remain release work.

The driver includes two concurrent activity result channels, a native timer,
ordered callback batching, repeatability across GOMAXPROCS 1/2/8, and deadlock
reporting. `IsolateConcurrent` is registered by the example worker and replayer;
it completed on the local server and replayed in fresh processes. Ordinary
`PlainEcho` continued to work on the same worker.
See [the implementation plan](https://github.com/mfateev/golang-go/blob/task/modify-go-runtime-for-isolates/doc/isolates/TEMPORAL_POC.md).

Typed handlers receive protobuf-serialized Temporal `Payloads` through the
copied-byte boundary. An isolate-owned serializer decodes arguments and encodes
results. Temporal's default serializer is used unless the worker configures a
marked factory through `worker.SetIsolateDataConverter` (see below). Default
supported encodings are `binary/null`, `binary/plain`, `json/plain`,
`json/protobuf`, and `binary/protobuf`. Generated Google protobuf messages use
SDK value converters, including message-type metadata. External payload references
remain unsupported. See generated protobuf support below.

The host applies configured codecs using `converter.RawValue`, preserving the
plain serialized values without deserializing application types on the host.
Converter maps, ordered lists and mutable value converters are isolate-owned;
shared type metadata uses audited runtime services. The pinned converter/SDK
external dependency graph remains process-owned and subject to compulsory
ownership and effect checks. Ordinary SDK workflows retain their converter.

Mark each workflow function and register it through the POC worker package:

```go
// In an ordinary workflow package:
//go:isolate
func TypedEchoWorkflow(ctx context.Context, request TypedEchoRequest, suffix string) (TypedEchoResult, error) {
    return TypedEchoResult{Message: "hello " + request.Name + suffix}, nil
}

// In the host, using github.com/mfateev/sdk-go-poc/worker:
w := worker.New(client, taskQueue, worker.Options{})
w.RegisterWorkflow(order.TypedEchoWorkflow)
w.RegisterWorkflow(PlainEcho) // An ordinary Temporal workflow remains ordinary.
```

`go build` discovers marked functions in the host's import graph, retains their
original Go signatures, and generates direct typed invokers. No `isolate.json`,
workflow `main`, or `-isolate-dir` is needed. A package can contain several
marked functions and ordinary functions. Each execution gets fresh selected
package state. `isolate.LookupFunction(fn)` exposes the generated handle and
its original signature; the worker registers a `WorkflowDefinitionFactory`
for marked functions and forwards unmarked functions to the usual Go SDK.
`RegisterWorkflowWithOptions` preserves aliases and registration options.
Ordinary workflows may still use custom converters.
Register marked functions from the host's `main`, after startup; the generated
function table is populated after imported package initializers run.

Marked functions accept any number of ordinary Go arguments and return either
nothing, `error`, or `(result, error)`. They do not take a host-owned Temporal
`workflow.Context`. Generic or variadic functions, methods, cgo source, and dot
imports are rejected. Automatic entry generation currently applies to a
single executable `go build` and `go test` binaries, including entries declared
in internal or external `_test.go` files. `go run` and `go install` remain
future work. Initialization and state selection still operate at package level,
so selected package initializers run for every instance. Finer function-level
dependency selection remains TODO. The older directory-program and explicit
registration helpers remain available for runtime probes.

`workflow.GetSignalChannel[T](ctx, name)` returns a native `<-chan T` of signal
values decoded with the workflow's configured serializer. For example:

```go
type Approval struct {
    Approved bool
    Reason   string
}
approvals := workflow.GetSignalChannel[Approval](ctx, "approval")
select {
case approval, ok := <-approvals:
    if !ok {
        return ctx.Err()
    }
    // Use approval.Approved and approval.Reason directly.
case <-ctx.Done():
    return ctx.Err()
}
```

`GetSignalChannelWithOptions[T]` also accepts `SignalChannelOptions` for a
signal description. Repeated calls for the same name and type return the same
channel and retain the first description; a different type for that name panics.
Channel lifetime follows workflow cancellation, which closes the channel without
delivering a zero value. Canceling an activity/child context used for registration
does not close it. A first registration with `context.WithoutCancel(ctx)` keeps
the channel open after workflow cancellation, allowing cleanup/update handlers
to finish. Invalid payloads and other host errors panic and fail the
Workflow Task. `T` supports the same JSON/bytes/null values and configured
serializers as workflow arguments; generated Google protobuf values are supported.
Existing callers must supply a type argument and receive values directly,
e.g. `GetSignalChannel[[]byte]`, instead of accessing `SignalResult.Signal.Input`.
`NextSignal` retains its byte-input convention and receives the next signal of
any name. The host keeps other named signals queued for matching channels.

Signals are proactively delivered and decoded into registered native channels
before application goroutines resume. Registration also delivers signals already
queued for that name before returning. A `select` receive loop with a `default`
case can therefore drain the admitted batch before Continue-As-New, just as with
SDK signal channels. The native buffer holds up to 100,000 values; exceeding it
fails the Workflow Task. Undecodable signals are discarded, logged at error
level, and counted by `temporal_corrupted_signals`; the next valid signal remains
available. This also applies to the byte-valued `NextSignal` API. Application
goroutines processing signals still need
to finish before returning or continuing as new.

The bridge uses the runtime's SDK inbound-service admission phase to fill these
buffers while application channel receivers remain fenced. Inbound signal hooks
and goroutine-based outbound channel adapters participate in the same phase.
Only copied bytes cross from the host; decoding and channel sends happen on the
workflow's own heap. Runtime scheduling for ordinary goroutines is unchanged.

The compiled typed-signal checks cover nonblocking drains at startup, late
registration, signal/timer wakeups, inbound renaming/filtering, outbound native
channel adapters, struct payloads, precise integers,
ordered delivery, channel identity, cancellation, conflicting types, malformed
payloads and worker-configured converters at GOMAXPROCS 1/2/8. Corrupted-signal
ports cover blocking, select, nonblocking and closed-channel reads, late
registration, `NextSignal`, malformed envelopes, and replay counter suppression:

```sh
../golang-go/bin/go test ./example/signals/check
```

The local-server check also sends durable signal batches before worker polling,
verifies Continue-As-New carryover and corrupt-signal skipping, and replays the
recorded histories:

```sh
SIGNALS_DEV_SERVER=1 ../golang-go/bin/go test -count=1 ./example/signals/check
```

Activities use the current Go SDK API: `WithActivityOptions`,
`ExecuteActivity(ctx, activity, args...)`, and `Future.Get(ctx, &result)`.
The earlier typed activity helpers are commented out and retained in
`workflow/typed_activity.go` for future API work.

Every isolate workflow and every POC activity takes standard `context.Context`
as its first parameter. The SDK creates the workflow's context inside its
isolate and injects it before decoding the ordinary input payloads. Contexts
are never serialized or passed as host pointers. Existing ordinary Temporal
workflows retain the standard SDK's `workflow.Context` API.

```go
func FormatNumber(ctx context.Context, input int) (string, error) {
    return fmt.Sprintf("number:%d", input), nil // host activity
}

//go:isolate
func Example(ctx context.Context, input int) (string, error) {
    ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
        StartToCloseTimeout: time.Minute,
    })
    var result string
    err := workflow.ExecuteActivity(ctx, FormatNumber, input).Get(ctx, &result)
    return result, err
}
```

Activity references identify host work; they never run inside the isolate.
`ExecuteActivity` acknowledges scheduling before returning, including when its
future is ignored. Function references or literal activity names accept zero or
multiple arguments and activities returning only `error`; pass `nil` to `Get`
when no result is needed. `Get` is repeatable and `IsReady` checks completion.
For native `select`, use `Future.ToChannel()`, as in `example/concurrent`:

```go
resultCh := workflow.ExecuteActivity(ctx, MyActivity, input).ToChannel()
select {
case outcome := <-resultCh:
    if outcome.Err != nil {
        return outcome.Err
    }
    var result MyResult
    return outcome.Value.Get(&result)
case <-ctx.Done():
    return ctx.Err()
}
```

Each `ToChannel()` call returns a new buffered receive-only channel with one
`FutureResult{Value, Err}`, then closes it. On success, `Value` implements the
SDK's `converter.EncodedValue`: `Get(&typedResult)` extracts the chosen type and
`HasValue()` distinguishes an error-only activity with no result. On failure,
`Value` is nil and `Err` preserves the future's error. Extraction remains
repeatable, including through `Future.Get`, with the same converter and POC
type checks. Receiving, ignoring, or abandoning an adapter channel does not
cancel the activity; its execution context controls cancellation. A native
goroutine waits for completion and the one-item buffer lets it finish even when
the channel is ignored. Set a select arm's channel to nil after consuming its
result to avoid selecting repeatedly on the closed channel.

A Temporal workflow cancellation request cancels the context passed to the
workflow function. `<-ctx.Done()` wakes and `ctx.Err()` is `context.Canceled`.
Child contexts inherit cancellation normally. `context.WithCancel`,
`WithCancelCause`, `WithTimeout`, `WithDeadline`, `WithValue`, `WithoutCancel`,
and `AfterFunc` work inside an isolate. Deadlines use history time and durable
Temporal timers, and children/callbacks cancel in creation order. Custom
contexts with their own `AfterFunc` implementation remain outside the POC.

Canceling the context supplied to an activity call requests cancellation of
that activity on the host. With the default `WaitForCancellation: false`,
the SDK resolves the future immediately with cancellation. With
`WaitForCancellation: true`, the future waits for the activity’s actual terminal
result, which may still be successful. `Get` does not cancel or abandon the
operation when its waiting context is canceled; the execution context controls
the activity lifetime, matching the pinned SDK. The worker supplies
the activity's own standard Go context; a workflow context is never copied
into the host. As with standard Temporal activities, a running activity should
heartbeat to receive cancellation promptly and must cooperate with its context.
A child cancellation leaves its parent active, and `context.WithoutCancel(ctx)`
can be used for cleanup work after workflow cancellation. A workflow returning
`context.Canceled` completes as canceled; a workflow may handle cancellation
and return a normal result instead.

All pinned `ActivityOptions` fields are forwarded: task queue, activity ID,
schedule/start/heartbeat timeouts, retry policy, cancellation policy, eager
execution, versioning intent, summary, and priority. Option helpers copy context
state and mutable retry policy data. Empty task queues inherit existing context
options and ultimately the workflow task queue. Server retry defaults are
preserved; set `RetryPolicy.MaximumAttempts: 1` to disable retries.

The result boundary supports default-converter JSON/bytes/null and generated Google protobuf values.
Activities and workflow completion preserve the pinned SDK's structured error
types, causes, details, retry metadata, and heartbeat details across the byte
boundary. The default SDK failure converter runs under the receiving owner's
allocator. The compiler supplies a checked generated-value `proto.Clone` path;
it copies mutable values without exposing shared coder caches or invoking custom
message callbacks. Ordinary host protobuf operations retain their implementation.
The internal Failure wire codec supports the pinned schema and ordinary payloads;
unknown fields, extensions, and external payload references remain unsupported.
Worker-configured deterministic serializers, generated Google protobuf values,
custom failure converters and host codecs are supported below.

### Child workflows

`ExecuteChildWorkflow`, `ChildWorkflowOptions`, `WithChildWorkflowOptions`,
`GetChildWorkflowOptions` and `ChildWorkflowFuture` follow the pinned SDK API
with native contexts. Function references resolve registration aliases on the
host; strings are used as supplied. Children can be isolate workflows or
ordinary SDK workflows. Each isolate child and each retry/continued run receives
fresh isolate state.

```go
ctx = workflow.WithChildWorkflowOptions(ctx, workflow.ChildWorkflowOptions{
    WorkflowRunTimeout: time.Minute,
    WaitForCancellation: true,
})
child := workflow.ExecuteChildWorkflow(ctx, ChildWorkflow, input)
var execution workflow.Execution
if err := child.GetChildWorkflowExecution().Get(ctx, &execution); err != nil {
    return err
}
return child.Get(ctx, &result)
```

Scheduling finishes before the call returns, even when the future is ignored.
Results support repeatable `Get` and the same `ToChannel`/`EncodedValue` adapter
as activity futures. Canceling the execution context requests child cancellation
after initiation; `WaitForCancellation` keeps the SDK completion policy.
Canceling a context used only to wait does not abandon the future.
`SignalChildWorkflow` waits for initiation and signals the current child run,
including after continue-as-new. Signals to isolate workflows are decoded into
the registered channel type; use `GetSignalChannel[[]byte]` for existing byte-input histories.

The host SDK owns IDs, retry/cron behavior, timeouts, parent-close policy,
versioning, priority and summaries. Memo and untyped search-attribute values
cross as encoded payloads, preserving integers larger than JSON float precision.
Typed search attributes use the SDK's seven key/update types. Their audited
interface-key map iteration sorts by attribute name and value type, ignoring
process-specific `reflect.Type` addresses; custom key implementations remain
unsupported. Native context propagators inject child headers, including values
overridden on a derived context. Cache eviction detaches local callbacks without canceling server-side
children; parent completion follows the configured parent-close policy.

`example/child/check` tests native dispatch at GOMAXPROCS 1/2/8, cancellation
before/after initiation, signals, repeated future reads, option forwarding,
failed/duplicate starts and late callbacks after eviction. Checked-in histories
cover ordinary and isolate children, structured failures, retries and
continue-as-new. Build and run with the isolate toolchain:

```sh
../golang-go/bin/go test ./example/child/check
../golang-go/bin/go build -race -o /tmp/isolate-child-check ./example/child/check
GOGC=1 /tmp/isolate-child-check -history-dir example/child/testdata
# Optional: with a Temporal development server on localhost:7233
/tmp/isolate-child-check -address localhost:7233
```

### Queries

`SetQueryHandler(ctx, name, handler)` and `SetQueryHandlerWithOptions` follow
the SDK API with a native context. Handlers return `(result, error)` and may
read captured workflow state. Queries use a separate allocation owner while
workflow goroutines remain suspended. Writes to workflow fields, maps, slices,
globals and atomics panic before mutation. The query dispatcher recovers that
panic and returns a query error; subsequent queries and workflow tasks continue.
Handlers can allocate and change their own temporary values, but cannot spawn
goroutines, block on channels or locks, schedule workflow operations, or exit
the isolate. The same execution mode is intended for update validators.

Completed workflows with registered query handlers retain their state until
cache eviction. Eviction revokes the retained goroutines and releases both
allocator caches. This intentionally uses more cache memory than immediately
destroying completed workflows. Queries use the registration context's serializer
with scratch-owned caches, after extracting incoming headers through native context
propagators. Completed futures can be decoded without waiting; incomplete futures
return an error. Variadic handlers remain deferred. Generated Google protobuf values are supported. A non-yielding handler
can still exceed the task deadline; hard containment of CPU loops remains a
productization limitation.

`example/query/check` verifies rejected writes, continued workflow execution,
queries after completion and eviction. Its optional `-address localhost:7233`
mode records and replays a live Temporal execution.

### Updates

`SetUpdateHandler`, `SetUpdateHandlerWithOptions`, `UpdateHandlerOptions`,
`GetCurrentUpdateInfo`, `AllHandlersFinished` and unfinished-handler policies
follow the pinned SDK, using native contexts. Handlers require context first
and return `error` or `(result, error)`. Validators may omit context; their other
argument types must match the handler. They use the same read-only owner and
execution fence as queries. A state mutation or forbidden operation panics and
rejects that update as an SDK `PanicError`, without killing the workflow.
Accepted handlers run as high-priority native goroutines and may block on durable
operations. Their contexts follow workflow cancellation. Each workflow task
delivers signals into native channels, then runs ready handlers and admits queued
updates before releasing ordinary workflow goroutines. Updates are admitted one
at a time, allowing later validators to observe earlier handlers' state changes.
Workflow cancellation and SDK future results are also delivered before handlers
resume, including futures created by ordinary workflow goroutines and native
`ToChannel` adapters. Native timer waiters deliver values into their channels in
the same input phase; `time.AfterFunc` callbacks remain ordinary user goroutines.
This prevents an available result from remaining in a delivery goroutine while
the main goroutine runs first.

The runtime uses `isolate.GoWithPriority(isolate.PriorityHigh, fn)` for the update
delivery service and handlers. Whenever a goroutine blocks, yields, or finishes,
ready high-priority goroutines run before ordinary ones. Priority persists across
activity, timer, and channel wakeups; ordinary children started with `go` keep
normal priority. Priority does not preempt a running goroutine, bypass read-only
or signal-delivery fences, or prevent ordinary work from running when handlers
are blocked. Update priority is unconditional and does not depend on SDK flags.

The host SDK owns update acceptance and completion protocol messages. Results
stay encoded across the boundary; large JSON integer values do not pass through
an untyped host JSON decode. Rejected validators and handler failures preserve
SDK error types/details. Replay skips validators for historically accepted
updates, as in the SDK. Registration yields to queued updates for that handler.
Returning from the root abandons unfinished handlers according to the SDK policy;
it does not implicitly wait for them. `AllHandlersFinished` remains false until
their completion has reached the host SDK.

Use `WaitHandlersFinished(ctx)` to wait on accepted handlers with native Go
before returning or continuing as new. It returns `ctx.Err()` on cancellation;
use `context.WithoutCancel(ctx)` when cleanup must also await update completion.

Validation decodes arguments under the scratch owner; accepted handlers decode
again under the workflow owner. Custom decoder callbacks must be pure during
validation. Incoming headers are extracted before context-aware argument decoding,
and the registration context selects the serializer. Variadic handlers and rich
validator panic stack diagnostics remain outside this POC. See
`example/update/check` for compiled mutation/cancellation/concurrency checks,
registration and simultaneous wakeup ordering, ordinary-child dependencies, and
its checked-in live update history; `-address localhost:7233` records a fresh run.

Run the local-server update and recorded-history replay checks with:

```sh
UPDATES_DEV_SERVER=1 ../golang-go/bin/go test -count=1 ./example/update/check
```

### Dynamic workflows and handlers

`worker.RegisterDynamicWorkflow` and the replayer's matching method accept a
marked native function taking `(context.Context, converter.EncodedValues)` and
returning `error` or `(result, error)`. The fallback runs when the requested
workflow type has no named registration. Read the requested type through
`workflow.GetInfo(ctx).WorkflowType.Name`; `args.Get` decodes arbitrary arguments
inside the isolate. Register only one dynamic workflow per worker or replayer.
The host SDK still loads `DynamicRegisterOptions.LoadDynamicRuntimeOptions`.
The native test environment also supports `RegisterDynamicWorkflow`.

```go
//go:isolate
func DynamicWorkflow(ctx context.Context, args converter.EncodedValues) (string, error) {
    var first, second string
    if err := args.Get(&first, &second); err != nil { return "", err }
    return workflow.GetInfo(ctx).WorkflowType.Name + ":" + first + ":" + second, nil
}

w.RegisterDynamicWorkflow(DynamicWorkflow, workflow.DynamicRegisterOptions{})
```

Within any native workflow, register fallbacks with:

```go
workflow.SetDynamicQueryHandler(ctx,
    func(name string, args converter.EncodedValues) (string, error) {
        return name, nil
    }, workflow.QueryHandlerOptions{Description: "Fallback queries"})

workflow.SetDynamicUpdateHandler(ctx,
    func(ctx context.Context, name string, args converter.EncodedValues) (string, error) {
        return name, nil
    }, workflow.UpdateHandlerOptions{
        Validator: func(name string, args converter.EncodedValues) error { return nil },
        Description: "Fallback updates",
    })
```

Query handlers return `(result, error)`. Update handlers may return only `error`;
validators return `error` and may additionally take `context.Context` first.
Named query/update registrations take precedence, including an explicitly empty
name. Registering a fallback again replaces it. Names beginning with `__` remain
reserved and never reach a fallback. Query metadata describes fallback handlers
with an empty name. Update metadata uses the same convention.

Queries and validators retain the existing read-only ownership checks, scratch
converters and interceptor chains. Handler interceptor arguments contain the
requested name and `converter.EncodedValues`; workflow interceptor arguments
contain the encoded values. Payloads are copied into the isolate after host
codecs are removed. `Get` uses the registration context's converter, and rejects
unsupported protobuf values. Encoded values are local to the handler's owner;
queries and validators cannot save them into captured workflow state. Accepted
updates run with native priority and expose the actual name and ID through
`GetCurrentUpdateInfo`. Registration yields to matching queued updates within the
current task. Unregistered updates are rejected when that task reaches idle.
Completed workflows retain dynamic query state until eviction or test cleanup.

The adapted application sample lives in the sibling
`samples-go-poc/dynamic-workflows` checkout. `example/dynamic` contains SDK
regression fixtures; its checker covers custom
serialization, compression codecs, inbound interceptors, cache eviction and saved
history replay at GOMAXPROCS 1/2/8:

```sh
../golang-go/bin/go test ./example/dynamic/check -run 'TestDynamic|TestNativeDynamic'
../golang-go/bin/go test ./example/dynamic/check
DYNAMIC_DEV_SERVER=1 ../golang-go/bin/go test -count=1 ./example/dynamic/check
```

### Continue-as-new and workflow versioning

Use the SDK names and error type with a native context:

```go
if workflow.GetVersion(ctx, "change-id", workflow.DefaultVersion, 1) == 1 {
    // The new behavior, recorded by the host SDK's version marker.
}
ctx = workflow.WithWorkflowRunTimeout(ctx, time.Hour)
return workflow.NewContinueAsNewError(ctx, MyWorkflow, nextInput)
```

`NewContinueAsNewError` and `NewContinueAsNewErrorWithOptions` produce the
SDK's `ContinueAsNewError`. Returning it, including a wrapped error, delegates
the continuation command to the host SDK. Each new run starts a fresh isolate
with fresh globals; the old run and its pending local callbacks retire before
completion is published. Workflow function references resolve registration
aliases just as activity references do. Literal names are used verbatim.
Timeouts and the task queue inherit the current run unless overridden with the
SDK's workflow context option helpers. Retry policy, backoff and initial
versioning behavior use `ContinueAsNewErrorOptions`.

`GetVersion` delegates marker creation, recorded-version lookup and supported
range checks to the pinned SDK. An old history without the marker returns
`DefaultVersion`; repeated calls reuse the recorded version. An unsupported
recorded version fails the Workflow Task. `IsReplaying` exposes the SDK replay
flag for diagnostics; never use it to change workflow commands or results.
Continuations inject headers and encode inputs using the supplied native context,
including its context-aware or per-context converter.

`example/continuation/check` tests compiled fresh runs and replays saved live
histories, including the unsupported-version negative case:

```bash
../golang-go/bin/go test ./example/continuation/check
../golang-go/bin/go build -o /tmp/isolate-continuation-check ./example/continuation/check
/tmp/isolate-continuation-check -history-dir example/continuation/testdata
# Optional: record new fixtures against a local Temporal development server.
/tmp/isolate-continuation-check -live 127.0.0.1:7233 -history-dir /tmp/continuation-histories
```

`workflow.Sleep(ctx, d)` and `NextSignal(ctx)` return cancellation errors;
`GetSignalChannel[T](ctx, name)` closes on workflow cancellation. Native timers
can be used in `select` alongside `ctx.Done()`.

Function names and bound methods follow Temporal's short-name convention.
`activity.RegisterOptions{Name: ...}` aliases resolve on the host, including
registrations made after the workflow registration. Prefixed struct methods
use explicit names, matching the standard SDK. `worker.New` honors
`DisableRegistrationAliasing`; `worker.Wrap` assumes default SDK aliasing.
When replaying aliased function references, supply the same aliases with
`replayer.RegisterActivityWithOptions(fn, options)`. Replay registrations are
metadata only and never execute activities. Unaliased functions need no replay
activity registration. Registration rejects POC activities and marked workflows
that omit the required standard context parameter.

Arguments/results use the isolate-owned default or worker-configured serializer
and cross `Call` as protobuf-serialized `Payloads`. Default supported values are
JSON/bytes/null and generated Google protobuf messages. Adding context parameters does not add context data to workflow or
activity histories. Activities now use SDK-style futures with a mandatory standard Go context.

Cancellation examples are registered on the example worker: `ActivityWorkflow`
(waiting activity), `IdleWorkflow` (waiting on `ctx.Done()`), `DeadlineWorkflow`,
and `LocalCancelWorkflow`. `ContextStressWorkflow` verifies child callback order.

When the host configures an isolate clock and timer operation, native
`time.After`, `time.NewTimer`, and `time.Sleep` use durable host timers.

### Function entry ownership

The adapter uses `isolate.Handle.ProgramWithHandle` with a noncapturing dispatcher.
The trusted runtime entry copies compiler-created function metadata by value;
workflow dispatch does not read a host-owned closure containing that handle.
The compiler/runtime conformance workflow checks SDK application code with
level-two heap ownership diagnostics, including the race build. Marked workflow builds force ownership checks across the linked dependency
graph. Custom converters and hostile native-code containment retain separate
productization gates.

### Memory ownership failures

A detected ownership violation permanently terminates the workflow's isolate.
Workflow `recover` cannot handle it and application defers are discarded.
The adapter preserves the host's `*isolate.OwnershipError` (inspect it with
`errors.As`) when reporting failure. Trusted metadata services finish releasing
process locks before their goroutines are discarded; `Kill(ctx)` waits for
that cleanup. The metadata driver checks private registry and MessageInfo failures
and verifies that subsequent workflows still run in the same host.

Marked workflow builds make the compiler heap checks mandatory. The supported
default converter and native workflow operations have ownership regression
coverage; custom serializers are constructed under the isolate owner as described below.

## Workflow effects and logging

Isolate workflows reject environment/configuration reads, file and network I/O,
subprocesses, OS entropy, process runtime controls, and application unsafe/native
escape paths. Perform external work in activities and pass configuration as
workflow input. Forbidden operations permanently revoke the isolate; application
`recover` and deferred callbacks cannot resume it. The worker reports the
operation and isolate stack as a **Workflow Task failure**, allowing a corrected
workflow implementation to retry that task.

Isolate registrations require the SDK's default `BlockWorkflow` panic policy.
`worker.New` rejects marked workflow registration under `FailWorkflow`; ordinary
workflow registrations retain their configured behavior. When adapting an
existing worker, pass its original options to `worker.Wrap(existing, options)`.
Direct `temporalbridge.Factory` users must configure `BlockWorkflow` themselves.

`fmt.Print*`, standard `log`, `slog` and Go's `print`/`println` format inside the
isolate and send copied records through the reserved logging Write. Initializer
logs use the same transport. The default host handler uses the SDK logger and
honors `EnableLoggingInReplay`. Configure a worker or replayer with:

```go
w := worker.New(c, taskQueue, worker.Options{})
err := worker.SetIsolateLogHandler(w, func(event worker.LogEvent) {
    if event.Replay {
        return
    }
    hostLogger.Info(event.Message, "source", event.Source,
        "workflowID", event.WorkflowID, "runID", event.RunID)
})
if err != nil {
    panic(err)
}
w.RegisterWorkflow(MyWorkflow)
```

The handler stays on the host. Its errors/configuration are never returned to
workflow code; printing returns the formatted byte count and nil without waiting
for receipt or acknowledgment. Delivery is best effort: the runtime queues at
most 64 records, and drops records whose encoded payload exceeds 64 KiB or whose
queue is full. Startup, task/query fences and shutdown drain queued records.
The handler executes on the SDK host thread so replay filtering uses the correct
task metadata; remote handlers should enqueue to their own bounded exporter.
Handler panics produce host diagnostics and do not fail the workflow task.
A nil handler restores SDK logging. Workflow-local buffers and explicitly
constructed local loggers remain available. Host workflows and activities keep
ordinary Go printing, logging and I/O behavior.

### Interceptor sinks

An interceptor can serialize observations itself and emit bytes through a sink.
Give each sink a stable numeric operation code shared by workflow code and host
registration. Application codes range from `0x00010000` through `0xfffeffff`;
SDK calls and runtime operations, including printing, are reserved outside that
range. Registration order does not determine operation codes.

```go
const telemetryOp uint32 = 0x10000

// Host worker (the same API also accepts a workflow replayer):
err := worker.RegisterSink(w, telemetryOp, func(event worker.SinkEvent) {
    // Decode with the interceptor's own format. For remote IO, enqueue to a
    // bounded host exporter; this callback runs on the SDK host thread.
    exporter.Enqueue(event.WorkflowID, event.RunID, event.Payload)
}, worker.SinkOptions{Name: "telemetry"})
if err != nil {
    panic(err)
}

// Inside the interceptor, hide the sink behind its own Event method:
sink := workflow.NewSink(telemetryOp)
sink.Emit(encodedEvent)
```

`Sink.Emit` copies the bytes using one-way `isolate.Write`, with no acknowledgment
or result. Temporal's DataConverter, payload codecs and encryption pipeline are
not involved. The interceptor and host handler own their serialization format.
Names are optional diagnostic labels; routing uses the operation code alone.
Register handlers before starting a worker or replay. Workflow registration may
precede sink registration. Duplicate codes, reserved codes and nil handlers are
rejected; constructing a sink does not check whether the host has registered it.

Replay delivery is disabled by default. Set `SinkOptions.EnableReplay` to receive
replay observations, marked with `SinkEvent.Replay`. Events also contain the
operation, optional name, workflow ID, run ID and workflow type. Queued messages
are drained during tasks, queries, validators and shutdown, including final
observations after workflow completion. Sinks can emit from read-only handlers
but cannot emit during package initialization. Missing handlers and handler
panics produce host diagnostics without failing the workflow or returning data
to it. Delivery shares printing's 64-message queue and 64 KiB payload limit; full
queues and oversized messages are dropped. Sinks are for observations; external
decisions that affect workflow behavior belong in recorded activities.

### Effect conformance checks

The checker validates initializer and workflow logging, byte-sink routing and
replay policies, read-only and final sink delivery, worker configuration,
replay metadata, ordinary workflow registration, file and metadata-operation
violations, and recorded-history replay. Run it from this repository with the
fork's compiler:

```sh
../golang-go/bin/go build -o /tmp/isolate-effects-check ./example/effects/check
/tmp/isolate-effects-check
```

To also verify actual server history, start a temporary development server in
another terminal, then run the opt-in service check:

```sh
temporal server start-dev --ip 127.0.0.1 --port 7242 --headless
```

```sh
/tmp/isolate-effects-check live 127.0.0.1:7242
```

It checks that logging completes normally and that each forbidden operation
produces a Workflow Task failure containing its operation and stack, with no
Workflow Execution failure or completion. The checker uses unique workflow IDs
and terminates its own negative-test executions after checking their histories.


## Workflow lifecycle

Returning from a marked workflow terminates its remaining goroutines. Execution
completion is published only after the isolate's cleanup fence. Ordinary returned
errors remain Workflow Execution failures, and workflow-context cancellation stays
cooperative: workflow code can observe `ctx.Done()` and clean up before returning.

Unrecovered root or child panics, root `runtime.Goexit`, isolate `os.Exit`, and
memory ownership violations
produce **Workflow Task failures** under `BlockWorkflow`, leaving the execution
available to replay corrected code. Recovered panics and child `runtime.Goexit`
keep ordinary Go semantics. Panic diagnostics copy a bounded message and stack;
reporting never invokes an application's `Error` or `String` method.

Completion, cache eviction and workflow-definition close permanently revoke local execution.
Close retires outstanding command cells and callbacks; late activity, timer,
signal or cancellation callbacks cannot revive the workflow. Cache eviction does
not send server-side cancellation commands. Recreating a workflow replays its
history. Ordinary workflows and host activities retain their SDK behavior.

Automatic worker-shutdown integration is deferred by project decision (2026-10-07).
The pinned Go SDK's `Worker.Stop`
does not evict its shared sticky cache; closing a workflow definition and stopping
its worker are distinct operations. Individual-worker cache eviction needs an SDK
hook. The existing process-wide purge is valid only after all workers have stopped.

A stuck isolate can remain pending beyond the bounded close timeout. The adapter
logs the pending termination and keeps the handle for another cleanup attempt;
low-level hosts can inspect `CloseError()` and `StackTrace()`. A deadline never
undoes revocation. CPU loops without a supported execution fence and unsupported
native waits do not have an in-process hard-kill guarantee.

Run compiled lifecycle and replay conformance, optionally against a development
server (the checker terminates its own negative-test executions):

```sh
../golang-go/bin/go build -o /tmp/isolate-lifecycle-check ./example/lifecycle/check
/tmp/isolate-lifecycle-check
/tmp/isolate-lifecycle-check live 127.0.0.1:7242
```

## Isolate resource controls

Configure the isolate worker or replayer before executions are created:

```go
w := worker.New(client, taskQueue, worker.Options{})
w.RegisterWorkflow(MyWorkflow)
err := worker.SetIsolateResourceOptions(w, worker.ResourceOptions{
    Limits: isolate.ResourceLimits{
        MaxMemoryBytes: 64 << 20,
        MaxGoroutines:  128,
    },
    MaxTaskDuration:       5 * time.Second,
    MaxNoProgressDuration: time.Second,
    Observer: func(event worker.ResourceEvent) {
        // Host metrics/logging. Inspect event.Replay when aggregating.
        log.Printf("isolate %s: %+v %s", event.Kind, event.Stats, event.Error)
    },
})
if err != nil {
    return err
}
```

Options are copied once per execution; changing worker configuration affects new
executions. Zero values disable the corresponding limit. Ordinary workflows keep
their SDK behavior. The low-level `temporalbridge.Factory.ResolveResourceOptions`
provides the same policy; its definition exposes host-only `Resources()`.

Memory accounting charges owned allocator slots until GC sweep, attached stacks,
and attributable runtime metadata. Selected package globals are owned heap
allocations. `ReservedHeapBytes` reports full owned span capacity, including
allocated slots; unused capacity is not charged again. Shared process services,
SDK/transport objects and process-wide GC infrastructure are outside this budget.
It is **not an RSS cap**. Completed handles retain diagnostics without retaining
private heaps. Concurrent counter snapshots are independently sampled.

Goroutine limits include initializer, workflow and SDK helper goroutines attached
to the instance, including iterator runners. Oversized application allocations are rejected
before storage is requested. Runtime preparation under a lock may briefly exceed
a budget while finishing the cleanup needed for safe revocation. A resource violation permanently revokes the
instance and raises a typed `isolate.ResourceLimitError` as a **Workflow Task
failure**. Recovery cannot convert it into a workflow result. Eviction retires
late callbacks without canceling Temporal activities or timers.

Task duration includes startup and active task processing. The no-progress
watchdog starts after startup and detects absence of supported park/yield
progress; asynchronous preemption does not count as progress. Cached idle time
uses neither budget, and cached instances have no watchdog timers. These are
host monotonic watchdogs, not workflow clocks or exact CPU quotas.
`RunningNanoseconds` counts completed scheduled intervals, including metadata
work and syscalls. Uninterrupted loops or native execution can leave termination
pending; diagnostics and a later `Close()` report/retry cleanup, without forcibly
killing arbitrary execution.

`Observer` receives copied `created`, `task`, `limit`, `closed` and
`termination-pending` events on the host. Use it to integrate the worker's metrics
backend. Its values and failures are never supplied as deterministic workflow
inputs. Keep host callbacks short. An observer panic follows the existing task
panic path and disables that observer during cleanup.

Run the compiled resource integration checks with the fork:

```bash
../golang-go/bin/go test ./example/resources/check
```

## Worker-configured isolate data converters

Create the deterministic value serializer inside each workflow isolate:

```go
// This function can live in a separate package from the workflows.
//go:isolate
func NewWorkflowConverter(config []byte) (converter.DataConverter, error) {
    // Parse copied configuration here. Construct new converter/cache objects.
    return converter.NewCompositeDataConverter(
        converter.NewNilPayloadConverter(),
        converter.NewByteSlicePayloadConverter(),
        converter.NewJSONPayloadConverter(),
    ), nil
}

w := worker.New(client, taskQueue, worker.Options{})
w.RegisterWorkflow(MyWorkflow)
err := worker.SetIsolateDataConverter(w, worker.DataConverterOptions{
    Factory: NewWorkflowConverter,
    Config:  []byte(`{"schema":1}`),
})
if err != nil { return err }
```

The factory must be a top-level `//go:isolate` function with this exact signature.
Closures and host-owned converter instances cannot be passed into an isolate.
The compiler adds the factory's package/dependencies to the workflow's selected
state; common dependencies initialize once. The factory runs inside the isolate
before workflow arguments are decoded. It receives a private copy of `Config`.
A factory can allocate mutable caches and use deterministic Go operations; all
ownership and external-effect checks still apply to it and its callbacks.

The setting works on workers and replayers and can follow workflow registration.
Each new execution snapshots the factory/configuration; cached executions keep
their existing serializer. An empty `DataConverterOptions{}` restores the default
for new executions. Replay must use the same serializer format/configuration as
historical executions. Ordinary SDK workflows and host activities retain their
usual configured converter.

Read-only queries and update validators construct fresh scratch-owned converters
for their conversions using the same factory/configuration. Converter caches can
be updated there without changing cached workflow state. Writes from factories,
marshal callbacks or error methods to retained workflow state still panic before
mutation. A rejected query does not terminate its isolate.

### Encryption, compression and remote codecs

Configure the host as in the regular SDK:

```go
hostSerializer, err := NewWorkflowConverter(config) // Separate host-owned object.
if err != nil { return err }
hostDC := converter.NewCodecDataConverter(hostSerializer, payloadCodec)
c, err := client.Dial(client.Options{DataConverter: hostDC})
```

The host serializer must match the isolate factory's plain wire format and honor
Temporal's `converter.RawValue` contract. Plain payloads cross the copied-byte
boundary. The host data converter receives `RawValue`, applying codecs while
skipping application value conversion. Encryption keys, random nonces, HTTP codec
clients and external I/O stay on the host. Signals decode their Go byte values
under the receiving isolate owner too. Failure details and encrypted common
failure attributes are transformed across the entire cause chain.

Workflow, activity, child and external-signal codec contexts are supplied on the
host. Codecs must be self-describing on decode: SDK failure conversion may omit
context, and standalone SDK replay supplies synthetic namespace/execution IDs.
Auto-generated child IDs are determined before encryption using the pinned
SDK's current run ID and sequence, preserving its usual ID convention. The audited
SDK fork exposes a reset-aware ID reservation hook; the bridge does not read
private SDK fields. SDK search attributes
use their default serialization and bypass application codecs.

Current restrictions and follow-ups:

- Host codecs must preserve payload count and support encoding/decoding individual
  payloads. Batch codecs that combine multiple payloads are deferred and rejected
  when they violate the single-payload contract. Codec failures fail the Workflow
  Task. Remote codecs therefore currently make one request per payload.
- Gogo/dynamic/extension protobuf values and external payload references at the
  isolate wire boundary remain unsupported. SDK external-storage retrieval stays
  on the host; end-to-end storage integration has not been validated.

Run the compiled converter checks, optionally with a Temporal development server:

```sh
../golang-go/bin/go build -o /tmp/isolate-converter-check ./example/converter/check
/tmp/isolate-converter-check -history-dir ./example/converter/testdata
/tmp/isolate-converter-check -address 127.0.0.1:7233
/tmp/isolate-converter-check -history /tmp/feature8-Workflow-history.json
/tmp/isolate-converter-check -history /tmp/feature8-Parent-history.json
```

### Context propagation and per-context serialization

Native workflows use `context.Context`, so their `workflow.ContextPropagator`
interface has two methods: `Inject(context.Context, HeaderWriter) error` and
`Extract(context.Context, HeaderReader) (context.Context, error)`. Construct the
ordered propagators inside each isolate with a marked factory:

```go
//go:isolate
func NewPropagators(config []byte) ([]workflow.ContextPropagator, error) {
    return []workflow.ContextPropagator{newMyPropagator(config)}, nil
}

err := worker.SetIsolateContextPropagators(w, worker.ContextPropagatorOptions{
    Factory: NewPropagators,
    Config:  []byte(`{"keys":["tenant"]}`),
})
```

The setting works on workers and replayers, including after workflow registration.
Configuration is copied and snapshotted for each new execution; cached executions
keep their setting. Empty options disable propagation for future executions.
Configure matching ordinary SDK propagators on `client.Options.ContextPropagators`
to inject client headers and extract host activity contexts. The host and native
implementations must use the same header keys and payload formats. Context values
and propagator objects stay with their owners; copied headers cross the boundary.

Extraction precedes workflow argument decoding and query/update conversion.
Activities, local activities, child workflows, outbound signals, and continuations
inject headers from the context used to schedule them. Derived context values do
not change parent headers. Factories and callbacks must be deterministic and avoid
I/O. Queries and update validators construct fresh scratch-owned propagators.

`workflow.ContextAware` uses native contexts for both `WithWorkflowContext` and
`WithContext`, returning a `converter.DataConverter`. `workflow.GetDataConverter(ctx)`
binds the selected serializer to that context. Converters with only a native
`WithContext` method also work. Use `workflow.WithDataConverter(ctx, dc)` for a
derived context's serializer override; construct `dc` inside the workflow isolate.
It applies to scheduled operations and registered signal/query/update handlers.
Futures retain their scheduling converter even when `Get` uses another context.
The root context controls workflow results. Host serializers must understand every
plain payload format used by a native override; host codecs remain on the host.

Converters must support concurrent use by native goroutines. Read-only handlers
cannot mutate captured converter state; a context-aware method can construct fresh
scratch-owned caches without mutating its parent. Worker factories already provide
fresh scratch converters. Generated Google protobuf values use scratch clones
for read-only conversion. External-storage payloads remain unsupported.

The compiled port tests cover header inheritance and overrides, host activity and
child mocks, local activities, context-aware masking, per-context formats, repeated
queries/updates, and saved live-history replay at GOMAXPROCS 1/2/8:

```sh
../golang-go/bin/go test ./example/contextpropagation/check ./workflow ./worker
../golang-go/bin/go build -o /tmp/native-context-check ./example/contextpropagation/check
/tmp/native-context-check -address 127.0.0.1:7233 -cache-size 1000
/tmp/native-context-check -address 127.0.0.1:7233 -cache-size 0
```

### Custom failure converters

Native converters implement the existing Temporal `converter.FailureConverter`
interface: `ErrorToFailure(error) *failurepb.Failure` and
`FailureToError(*failurepb.Failure) error`. The failure schema, retry flags and
payload codec pipeline are unchanged. Use an isolate factory for the native side:

```go
//go:isolate
func NewFailureConverter(config []byte, dc converter.DataConverter) (converter.FailureConverter, error) {
    return &MyFailureConverter{
        DataConverter: dc,
        Fallback: temporal.NewDefaultFailureConverter(temporal.DefaultFailureConverterOptions{
            DataConverter: dc,
        }),
    }, nil
}

err := worker.SetIsolateFailureConverter(w, worker.FailureConverterOptions{
    Factory: NewFailureConverter,
    Config:  []byte(`{"schema":1}`),
})
```

The factory receives copied configuration and the operation's selected, bound data
converter, including native context-aware and per-context overrides. It runs once
for startup validation and constructs fresh private state for each conversion.
This also allows mutable converter caches in read-only update validators and
previous-run error queries. Factories and callbacks must be deterministic, avoid
I/O and leave captured workflow state unchanged. Factory errors and typed nil
results are rejected. Workers and replayers snapshot settings for new executions;
empty options restore the default, and cached executions retain their configuration.

Configure a compatible host implementation through `client.Options.FailureConverter`
(or `TestWorkflowEnvironment.SetFailureConverter`). The host handles activity/local
activity failures, retries, client decoding and its usual serialization-context
hooks. The native converter reconstructs incoming operation/previous-run errors and
serializes workflow failures, update handler failures and validator rejections.
Only copied failure bytes cross the boundary. Custom conversion may replace the
whole failure tree; preserving causes, operation wrappers and retry metadata is
the converter's policy. Host and native converters must agree on that format and
understand the plain payload encodings they exchange. The existing failure-wire
restrictions on extensions, unknown fields and external payload references remain.

`example/failureconversion/check` ports the upstream custom error/retry assertions
and covers precise details, fallback errors, read-only conversions, per-context
serializers, copied configuration, factory failures and saved compressed-history
replay at GOMAXPROCS 1/2/8:

```sh
../golang-go/bin/go test ./example/failureconversion/check
../golang-go/bin/go build -o /tmp/custom-failure-check ./example/failureconversion/check
/tmp/custom-failure-check -address 127.0.0.1:7233 -cache-size 1000
/tmp/custom-failure-check -address 127.0.0.1:7233 -cache-size 0
```

## Additional workflow APIs

### Native workflow tests

Import `github.com/mfateev/sdk-go-poc/testsuite` and use the custom toolchain's
`go test`. Marked functions in ordinary source and internal/external `_test.go`
files receive generated entries before `TestMain`. Ordinary SDK workflows retain
their existing path. Marked tests currently require the default gc mode without
`-cover`.

```go
//go:isolate
func TestedWorkflow(ctx context.Context, observed testref.Ref[int]) (int, error) {
    if err := observed.Store(42); err != nil {
        return 0, err
    }
    return observed.Load()
}

func TestWorkflow(t *testing.T) {
    var suite testsuite.WorkflowTestSuite
    env := suite.NewTestWorkflowEnvironment(t)
    observed, err := testsuite.NewRef(env, 0)
    require.NoError(t, err)
    env.ExecuteWorkflow(TestedWorkflow, observed)
    require.NoError(t, env.GetWorkflowError())
    value, err := observed.Load()
    require.NoError(t, err)
    require.Equal(t, 42, value)
}
```

Passing `t` registers automatic cleanup. Otherwise call `env.Close()` explicitly.
Completed workflows retain queries until cleanup. Cleanup drains root and child
isolates and invalidates references. Register children with the familiar
`RegisterWorkflow` APIs. Activity mocks/listeners and virtual time use the host
test engine. Native tests default to a 15-second task deadline to accommodate
instrumented package initialization; `worker.Options.DeadlockDetectionTimeout`
overrides it. That deadline is independent of virtual workflow time.

`OnWorkflow(...).Return(values...)` and function-valued returns execute on the
host, where callbacks may capture test assertions and variables. A replacement
that uses workflow APIs must be a marked function with the original signature:

```go
env.OnWorkflow(MyChild, mock.Anything, "input").
    ReturnIsolate(MyReplacementChild).Once().After(time.Minute)
```

Mock delays use virtual time. Mock call counts, `Run`, `Never`, `Maybe`, `Panic`,
ordering and expectation assertions remain available. Child-start listeners
receive copied workflow metadata, host `context.Context` and encoded arguments.
Native converter, failure-converter, propagator and interceptor factories use
the environment's `SetIsolate...` methods.

Update callbacks receive copied payloads for native results. Use
`testsuite.DecodeResult[T](value)` to recover the expected result type on the
host; pass the configured data converter as a second argument when customized.
Ordinary SDK callback values are returned directly. Development-server helpers
such as `StartDevServer`, `DevServerOptions` and `CachedDownload` retain their
host SDK behavior.

`testref.Ref[T]`, also `testsuite.TestRef[T]`, is a typed handle to environment-owned
JSON storage. Each side holds its own copy of the handle. `Load` returns an
independent snapshot; `Store`, `Swap` and `CompareAndSwap` publish changes through
the copied-byte host transport. Comparison uses canonical JSON value equality,
rather than pointer identity. Children in the same environment can use a handle;
other environments are rejected. Queries/validators may load references but
cannot write them, including through raw transport requests. Handles contain no
host pointers. Workflow-local goroutines and query closures may capture handles;
workflow entry definitions remain top-level marked functions receiving them as
arguments. References are test inputs outside Temporal history.

### Stack trace queries

The standard `client.QueryTypeStackTrace` (`__stack_trace`) query returns a Go
stack dump for the requested workflow instance. It includes the workflow entry,
its live child goroutines and accepted update handlers. SDK query/validation,
input-delivery, update-dispatch and session-service goroutines are excluded,
along with host goroutines and other workflow instances.
No custom query registration is needed. The host captures the stacks at the
workflow task's suspension fence without resuming workflow code or advancing
workflow time or randomness. A retained completed instance with no live workflow
goroutines returns an empty string.

```go
value, err := c.QueryWorkflow(ctx, workflowID, runID, client.QueryTypeStackTrace)
if err != nil {
    return err
}
var stacks string
if err := value.Get(&stacks); err != nil {
    return err
}
fmt.Print(stacks)
```

### External workflows

`SignalExternalWorkflow(ctx, workflowID, runID, name, arg)` and
`RequestCancelExternalWorkflow(ctx, workflowID, runID)` return repeatable Futures.
They schedule before returning and resolve when the server acknowledges the
request. An empty run ID targets the current run. `WithWorkflowNamespace` selects
the target namespace. Context cancellation does not retract these commands,
matching the SDK. Missing executions retain
`temporal.UnknownExternalWorkflowExecutionError` identity.

### Local activities

```go
ctx = workflow.WithLocalActivityOptions(ctx, workflow.LocalActivityOptions{
    StartToCloseTimeout: time.Minute,
})
var result Result
err := workflow.ExecuteLocalActivity(ctx, MyActivity, input).Get(ctx, &result)
```

Local activities execute outside the isolate through the SDK local worker. The
SDK owns local result markers, in-task retries and timeouts; longer retry backoff
uses durable workflow timers. Futures support repeatable Get and ToChannel.
Arguments are serialized inside the isolate and decoded into the registered
activity's exact Go types on the host. Codecs receive a local-activity context.

Register local activity implementations on the worker, including function
references: host-owned activity closures cannot cross the isolate boundary.
Offline replay does not require implementations and never executes them.
Activities and workflows must take context first, as elsewhere in this POC.

### Sessions

`CreateSession`, `RecreateSession`, `CompleteSession`, `GetSessionInfo`,
`SessionOptions`, `SessionState` and `ErrSessionFailed` follow the SDK contracts
with native contexts. Configure `worker.Options.EnableSessionWorker` as usual.
Session activities use the SDK creation/completion activities and response signal
protocol, then route user activities to the session worker's resource queue.
Failure cancels the session context and further activities return ErrSessionFailed.
Open session metadata is available through the SDK's built-in open-session query.
CompleteSession is idempotent and recreate tokens retain the SDK JSON format.

Session IDs use the original run ID and deterministic sequence, without
SideEffect or random UUID generation. They are stable in standalone replay and
after reset. Ordinary SDK workflows keep their existing session behavior; legacy
SDK session histories containing UUID SideEffect markers are not migrated by this
adapter. Session state/context objects remain isolate-owned.

### Nexus

`NewNexusClient(endpoint, service).ExecuteOperation(ctx, operation, input, options)`
accepts an operation name or typed Nexus operation reference and returns a
NexusOperationFuture. It supports Get, ToChannel and a separate
GetNexusOperationExecution future exposing the operation token. The host SDK
owns schedule/start/completion events and all four cancellation policies:
abandon, try cancel, wait requested and wait completed (the default).
Host codecs receive a Nexus serialization context. Services and handlers use
the ordinary worker's RegisterNexusService API and run outside isolates.

SideEffect and MutableSideEffect are intentionally omitted. External work belongs
in activities or local activities. Native context propagation and per-context
serialization also apply to these APIs, with the documented converter restrictions.

### Validation and observability

`example/apicoverage/check` covers ownership, native dispatch at GOMAXPROCS 1/2/8,
precise integer values, aliases, durable local retry, structured errors, session
recreation/failure, Nexus execution/result separation, cancellation and late
callbacks after eviction. Saved histories include ordinary SDK interoperability.

```sh
../golang-go/bin/go build -o /tmp/isolate-api-check ./example/apicoverage/check
/tmp/isolate-api-check -history-dir ./example/apicoverage/testdata
# With a development server; creates/deletes a temporary Nexus endpoint.
/tmp/isolate-api-check -address 127.0.0.1:7233 -history-dir /tmp/isolate-api-histories
```

Replay-aware printing, SDK logger/metrics interfaces and tracing use copied host
messages. The [API and observability plan](API_COVERAGE_PLAN.md) describes the
one-way sinks abstraction and its replay/delivery rules.

## Workflow interceptor API parity

The isolate chain covers all exported workflow inbound hooks and the non-concurrency,
non-SideEffect outbound hooks in Go SDK v1.49.0. New metadata APIs include
`GetTypedSearchAttributes`, `UpsertSearchAttributes`, `UpsertTypedSearchAttributes`,
`UpsertMemo`, `GetSignalChannelWithOptions`, `HasLastCompletionResult`,
`GetLastCompletionResult` and `GetLastError`. Queries and validators can use the
read getters through captured workflow contexts without mutating the cached chain.
Ordinary SDK workflows and host interceptors retain their upstream path.

See [the hook parity table and metadata contracts](INTERCEPTORS.md#metadata-and-previous-runs)
for converter behavior, explicit exclusions, the supported previous-failure binding,
and verification instructions.

## Versioned integration contracts

The POC implements one current isolate API, including protobuf support.
`isolate.CurrentContract()` exposes numeric sanity-check markers for metadata,
the API and determinism; all three are currently **1**. These markers can change
freely during development. They do not identify Git revisions or select older
API implementations. Build the SDK and application with the current toolchain.
Generated function/program registrations and package descriptors carry the
producer's numeric metadata version; an unsupported version fails before the
runtime reads descriptor fields or publishes an entry. The compiler's isolate
build report includes these versions.

The POC SDK separately expects those three versions and uses **host protocol 5**.
Protocol 5 adds native failure converter factories to startup, retaining context
propagator factories, priority update admission/execution and signal-channel
admission barriers from protocols 4, 3 and 2. Rebuild host and workflow
SDK together when upgrading.
Startup requests and replies have a fixed `ISOL` header carrying the four version
numbers. Both sides validate it before decoding startup data or running converter
factories and workflow code. An incompatibility fails the Workflow Task; it does
not complete the Workflow Execution. Version zero is invalid. The header is
internal transport metadata and does not add history events or alter Payloads.
Host and workflow code must be rebuilt together; mixing an old unversioned bridge
with a versioned workflow is rejected.

The SDK dependency is upstream `go.temporal.io/sdk v1.49.0`, replaced by the
audited [`mfateev/temporal-go-sdk`](https://github.com/mfateev/temporal-go-sdk/tree/task/modify-go-runtime-for-isolates)
tag **`v1.49.0-isolates.1`**. Go downloads it automatically; a fourth local checkout
is unnecessary. The compiler accepts only this exact replacement or the original
upstream baseline. Other versions, local replacements, nested modules and vendor
sources require a source audit before receiving metadata privileges. The fork's
binding contract is **1** and exposes previous failures, reset-aware child IDs,
memo converter policy and copied typed search-attribute updates, including unset
markers. Ordinary SDK workflows keep their existing execution path.

`worker.GetIntegrationInfo()` reports these contracts, the toolchain, actual SDK
module/version/checksum and application VCS revision when embedded by Go.
`isolate.Handle.Name()` identifies the statically linked workflow function.
These diagnostics are deployment information; they are not replay inputs or
automatic compatibility proofs.

### Compiled integration test budgets

Run the complete SDK suite with the custom toolchain:

```sh
export GOCACHE="$(cd .. && pwd)/go-build-cache"
../golang-go/bin/go test -timeout=20m ./...
```

Compiled workflow fixtures use `internal/checktest`: each checker build has a
10-minute budget, and each checker process gets a fresh 2-minute execution
budget. Compilation cannot consume a subsequent replay's execution allowance.
Build failures report elapsed time and context status. The outer 20-minute limit
is per test package; GitHub's native platform jobs have a 90-minute limit.

The interceptor, tracing and workflow-metadata synthetic hosts default to a
30-second workflow-task deadline, configurable with `-task-timeout`. This allows
race and ownership instrumentation in CI. Dedicated resource-limit checks retain
their short enforcement deadlines. Worker task and cleanup policies are separate
from these fixture budgets. Native CI saves full command output as artifacts.

### Upgrade and rollback validation

Keep supported histories when changing the compiler, runtime or SDK. Replay with
the candidate binary before deploying, and replay candidate histories with the
rollback binary before declaring rollback supported. Workflow versioning still
controls application command changes. Increment determinism versions when
observable execution behavior changes; never replace a failing fixture to hide
a compatibility break. Determinism 1 is the current shared random/select stream,
not the earlier independent-stream POC behavior.

From this repo, with the custom compiler built:

```sh
export GOCACHE="$(cd .. && pwd)/go-build-cache"
./example/compatibility/check.sh
```

The script builds the previous immutable SDK revision and current source, then
replays histories recorded before and after the protocol change at GOMAXPROCS
1/8. It compares the complete workflow trace, including recorded activity input
and completion. Logs and binaries remain in a printed temporary directory; set
`COMPATIBILITY_LOG_DIR` to choose it. This verifies SDK upgrade/rollback within
determinism 1 using the current compiler. It does not promise arbitrary old
toolchain or workflow-code compatibility. Native CI runs the same check on
Linux/macOS arm64/amd64.

To record an additional candidate history against a local Temporal server:

```sh
../golang-go/bin/go build -o /tmp/isolate-compatibility-record ./example/compatibility/record
/tmp/isolate-compatibility-record -history /tmp/isolate-candidate-history.json
```

The recorder prints and validates the binary's integration identity. Keep the
committed baseline unchanged when recording additional histories.

The Temporal SDK fork's canonical quality checks use standard Go 1.26.7:
`cd internal/cmd/build && go run . check`. Isolate builds and replay tests use
the custom toolchain. External analysis tools currently do not understand its
experimental export-data format; standard-Go checks validate the portable SDK
patch without disabling those checks.


## Generated protobuf values

Native workflow APIs accept generated `google.golang.org/protobuf` messages using
the open struct API, as pointers or values. The pinned SDK converters retain their
`json/protobuf` and `binary/protobuf` encodings, message-type metadata, typed nil
behavior and precise integer representation. Default conversion chooses protobuf
JSON; a native converter factory can select protobuf binary. Nested messages,
repeated fields, maps, oneofs, enums, proto2 scalar defaults and well-known types
(including `Any` with the default registry) are covered. Binary conversion preserves
unknown message fields; protobuf JSON follows the SDK's ordinary JSON semantics.

This applies to workflow inputs/results, activities, local activities, children,
continue-as-new, signals, queries, updates, memo and failure details. Per-context
converters keep selecting the serializer for their operation. Host payload codecs
continue to wrap the wire payloads, and saved histories replay with the same
configuration.

Descriptors remain canonical process metadata. Generated accessors resolve their
metadata by the Go type recorded by the audited process builder. Mutable reflection
and coder tables, codec pools, and value allocations belong to the active workflow
or query owner. Descriptor names and scalar defaults are copied into that owner.
Queries and validators serialize captured messages through scratch clones, so
protobuf's size caches cannot write workflow memory. Ordinary read-only checks still
reject direct mutation and reflection setters. Host protobuf behavior is unchanged.

Gogo/legacy messages, `dynamicpb`, extension-bearing messages, weak/opaque layouts,
custom resolvers and registry mutation are outside this implementation. Their types
or operations are rejected rather than gaining shared metadata privileges.

[Compiled API checks](example/protobufvalues/check/main_test.go) run both encodings
at GOMAXPROCS 1/2/8, including read-only conversions and mutation rejection. The
original two activity-environment cases retain host coverage in
[upstream_test.go](example/protobufvalues/check/upstream_test.go), with native
counterparts and compressed live histories for replay. Build host and workflow
binaries together with the current development toolchain.
