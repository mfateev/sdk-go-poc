# Temporal isolate SDK POC

## Determinism regression corpus

`example/determinism.DeterminismWorkflow` records observable ordering for native
goroutines, maps, selects, `sync.Map.Range`, `iter.Pull2`, top-level random streams,
floating-point distributions, standard context cancellation, and native timers.
Its activity input and result retain the full trace. The runtime's deterministic
default time zone is UTC. Named time-zone database lookups are rejected;
explicit fixed zones or supplied zone data remain valid.

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
`isolate.Config.Deterministic`: native goroutines share a FIFO execution token,
select polling is reproducible, and string/integer map range is canonical.
An exact suspend fence batches history replies and services all concurrent
commands before ending each workflow task. Unsupported map key kinds,
`sync.Map.Range`, and `iter.Pull` fail closed in this mode. General I/O
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
supported encodings are `binary/null`, `binary/plain`, and `json/plain`; a custom
factory can supply additional value encodings. Arbitrary protobuf values and
external payload references at this boundary remain unsupported.

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
single executable `go build`; `go run`, `go install`, and test binaries remain
future work. Initialization and state selection still operate at package level,
so selected package initializers run for every instance. Finer function-level
dependency selection remains TODO. The older directory-program and explicit
registration helpers remain available for runtime probes.

`workflow.GetSignalChannel(ctx, name)` provides a channel for signals with that
name. It wraps the existing host `Call` in an isolate-owned goroutine and
returns `workflow.SignalResult` values, including any host error. `NextSignal`
still receives the next signal of any name. The host keeps other named signals
queued for their matching channels.
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

The result boundary still supports default-converter JSON/bytes/null values.
Activities and workflow completion preserve the pinned SDK's structured error
types, causes, details, retry metadata, and heartbeat details across the byte
boundary. The default SDK failure converter runs under the receiving owner's
allocator. The compiler supplies a checked generated-value `proto.Clone` path;
it copies mutable values without exposing shared coder caches or invoking custom
message callbacks. Ordinary host protobuf operations retain their implementation.
The internal Failure wire codec supports the pinned schema and ordinary payloads;
unknown fields, extensions, and external payload references remain unsupported.
This does not enable general protobuf workflow arguments/results. Worker-configured deterministic serializers and host codecs are supported below;
custom failure converter implementations remain deferred.

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
including after continue-as-new. Signals to isolate workflows currently use
the native signal channel's byte-slice input convention.

The host SDK owns IDs, retry/cron behavior, timeouts, parent-close policy,
versioning, priority and summaries. Memo and untyped search-attribute values
cross as encoded payloads, preserving integers larger than JSON float precision.
Nonempty **typed search attributes are explicitly unsupported** in this POC:
the SDK stores their keys in an interface-key map, which needs a deterministic
iteration audit. Custom headers and context propagators remain
deferred. Cache eviction detaches local callbacks without canceling server-side
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
destroying completed workflows. Queries use the workflow's configured serializer with scratch-owned caches. Custom query headers, context propagation, variadic
handlers and arbitrary protobuf values are deferred. A non-yielding handler
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
Accepted handlers run as ordinary native goroutines and may block on durable
operations. Their contexts follow workflow cancellation.

The host SDK owns update acceptance and completion protocol messages. Results
stay encoded across the boundary; large JSON integer values do not pass through
an untyped host JSON decode. Rejected validators and handler failures preserve
SDK error types/details. Replay skips validators for historically accepted
updates, as in the SDK. Registration yields to queued updates for that handler.
Returning from the root abandons unfinished handlers according to the SDK policy;
it does not implicitly wait for them. `AllHandlersFinished` remains false until
their completion has reached the host SDK.

Validation decodes arguments under the scratch owner; accepted handlers decode
again under the workflow owner. Custom decoder callbacks must be pure during
validation. Variadic handlers, context propagation and rich
validator panic stack diagnostics remain outside this POC. See
`example/update/check` for compiled mutation/cancellation/concurrency checks and
its checked-in live update history; `-address localhost:7233` records a fresh run.

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
Custom context propagators and converters remain outside this checkpoint.

`example/continuation/check` tests compiled fresh runs and replays saved live
histories, including the unsupported-version negative case:

```bash
../golang-go/bin/go test ./example/continuation/check
../golang-go/bin/go build -o /tmp/isolate-continuation-check ./example/continuation/check
/tmp/isolate-continuation-check -history-dir example/continuation/testdata
# Optional: record new fixtures against a local Temporal development server.
/tmp/isolate-continuation-check -live 127.0.0.1:7233 -history-dir /tmp/continuation-histories
```

`workflow.Sleep(ctx, d)`, `NextSignal(ctx)`, and `GetSignalChannel(ctx, name)`
also honor cancellation. Native timers can be used in `select` alongside
`ctx.Done()`.

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
JSON/bytes/null. Arbitrary protobuf message arguments/results remain deferred. Adding context parameters does not add context data to workflow or
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

### Effect conformance checks

The checker validates initializer and workflow logging, worker configuration,
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
SDK's current run ID and sequence, preserving its usual ID convention. The POC
reads that private string field on the host and rejects an incompatible SDK layout;
an upstream bindings hook remains a productization task. SDK search attributes
use their default serialization and bypass application codecs.

Current restrictions and follow-ups:

- Host codecs must preserve payload count and support encoding/decoding individual
  payloads. Batch codecs that combine multiple payloads are deferred and rejected
  when they violate the single-payload contract. Codec failures fail the Workflow
  Task. Remote codecs therefore currently make one request per payload.
- Isolate value serializers must use context-independent formats. Per-operation
  serializer context, `workflow.WithDataConverter`, and custom failure converter
  implementations need further integration.
- General protobuf argument/result values and external payload references at the
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

## Additional workflow APIs

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
in activities or local activities. Custom headers/context propagation and the
documented converter restrictions still apply to these APIs.

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

Replay-aware printing/logging already goes to worker-configured host handlers.
Metrics, SDK logger interfaces and tracing are planned as copied host messages;
the [API and observability plan](API_COVERAGE_PLAN.md) describes the proposed
one-way sinks abstraction and its replay/delivery rules.
