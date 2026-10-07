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
`workflow.ExecuteActivityAsync` wraps `ExecuteActivity` the same way and returns
a channel with one `workflow.ActivityResult[R]` before closing.

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
    return workflow.ExecuteActivity(ctx, FormatNumber, time.Minute, input)
}
```

Go infers both input and result types. Async calls use the same context:

```go
result := <-workflow.ExecuteActivityAsync(ctx, FormatNumber, time.Minute, 5)
// result.Result is string; result.Err is error.
```

A Temporal workflow cancellation request cancels the context passed to the
workflow function. `<-ctx.Done()` wakes and `ctx.Err()` is `context.Canceled`.
Child contexts inherit cancellation normally. `context.WithCancel`,
`WithCancelCause`, `WithTimeout`, `WithDeadline`, `WithValue`, `WithoutCancel`,
and `AfterFunc` work inside an isolate. Deadlines use history time and durable
Temporal timers, and children/callbacks cancel in creation order. Custom
contexts with their own `AfterFunc` implementation remain outside the POC.

Canceling the context supplied to an activity call requests cancellation of
that activity on the host and wakes its workflow caller. The worker supplies
the activity's own standard Go context; a workflow context is never copied
into the host. As with standard Temporal activities, a running activity should
heartbeat to receive cancellation promptly and must cooperate with its context.
A child cancellation leaves its parent active, and `context.WithoutCancel(ctx)`
can be used for cleanup work after workflow cancellation. A workflow returning
`context.Canceled` completes as canceled; a workflow may handle cancellation
and return a normal result instead.

Function references also support zero-input and error-only activities:

```go
choice, err := workflow.ExecuteActivityNoInput(ctx, orders.GetOrder, timeout)
err = workflow.ExecuteActivityError(ctx, orders.OrderApple, timeout, choice)
completion := <-workflow.ExecuteActivityAsyncError(ctx, orders.OrderApple, timeout, choice)
err = completion.Err
```

The async error-only variant returns `<-chan ActivityResult[struct{}]>`: one
completion followed by channel closure. Its `Result` is empty; `Err` contains
the activity failure or cancellation.

These infer types from the actual signatures and preserve the original argument
counts. A nil receiver can identify a method; the registered host object supplies
its receiver state. Prefixed struct registrations still require explicit names.

Name-based calls support zero or multiple inputs and error-only activities:

```go
text, err := workflow.ExecuteActivityByName[string](ctx, "Greet", time.Minute, name)
result := <-workflow.ExecuteActivityAsyncByName[MyResult](ctx, "Compute", time.Minute, input, options)
_, err = workflow.ExecuteActivityByName[struct{}](ctx, "SendEmail", time.Minute, message)
```

Here the result type is explicit. Use `struct{}` for an activity returning only
an error. Name-based calls are passed through exactly. `workflow.Sleep(ctx, d)`,
`NextSignal(ctx)`, and `GetSignalChannel(ctx, name)` also honor cancellation.
Native timers can be used in `select` alongside `ctx.Done()`.

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
activity histories. Previous `ExecuteActivityWithContext` variants are replaced
by the mandatory-context `ExecuteActivity`/`ExecuteActivityAsync` APIs.

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
