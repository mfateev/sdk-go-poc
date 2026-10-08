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

The isolate adapter requires Temporal's default data converter on the worker
and rejects a custom converter when an isolate workflow task starts. Byte handlers
remain supported. Typed handlers receive protobuf-serialized Temporal
`Payloads` through the isolate byte boundary, decode their arguments with
a fresh default converter inside the isolate, and encode their
result there. The host forwards typed result payloads without converting them.
The isolate's small protobuf wire codec accepts ordinary payload metadata and
data. Its supported encodings are `binary/null`, `binary/plain`, and
`json/plain`; protobuf message encodings and external payload references fail
with a workflow error. Custom worker converters, payload codecs, serialization
context, and a full boundary type check remain TODOs. The workflow package
creates a separate default converter inside each isolate, including its converter
map, ordered list and mutable converter values. Shared type metadata uses the audited
runtime services. The host worker retains its standard default converter. The build
still treats the converter's external dependency graph as process-owned for
this trusted POC; its mutable caches and effects need an ownership audit.

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
Structured Temporal failures across the byte boundary remain feature 7 work:
non-cancellation failures currently arrive as text. The SDK default failure
converter's protobuf clone needs further ownership support before it can run
inside an isolate. Custom converters remain feature 8 work.

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

Arguments/results still use Temporal's default converter inside the isolate
and cross `Call` as protobuf-serialized `Payloads`. The supported values remain
JSON/bytes/null; custom converters and protobuf message arguments/results are
TODOs. Adding context parameters does not add context data to workflow or
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
coverage; custom converter support remains deferred.

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
isolate and send copied records through the reserved logging Call. Initializer
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
workflow code; printing returns the formatted byte count and nil after delivery.
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
