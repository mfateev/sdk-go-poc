# Temporal isolate SDK POC

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
`converter.GetDefaultDataConverter()` inside the isolate, and encode their
result there. The host forwards typed result payloads without converting them.
The isolate's small protobuf wire codec accepts ordinary payload metadata and
data. Its supported encodings are `binary/null`, `binary/plain`, and
`json/plain`; protobuf message encodings and external payload references fail
with a workflow error. Custom worker converters, payload codecs, serialization
context, and a full boundary type check remain TODOs. The compiler currently
treats the default converter's external dependency graph as process-owned for
this trusted POC; its mutable caches and effects need an ownership audit.

Mark each workflow function and register it through the POC worker package:

```go
// In an ordinary workflow package:
//go:isolate
func TypedEchoWorkflow(request TypedEchoRequest, suffix string) (TypedEchoResult, error) {
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

`workflow.GetSignalChannel(name)` provides a channel for signals with that
name. It wraps the existing host `Call` in an isolate-owned goroutine and
returns `workflow.SignalResult` values, including any host error. `NextSignal`
still receives the next signal of any name. The host keeps other named signals
queued for their matching channels.
`workflow.ExecuteActivityAsync` wraps `ExecuteActivity` the same way and returns
a channel with one `workflow.ActivityResult[R]` before closing.

Activity calls infer input and result types from a function with one input and
one result plus an error:

```go
func FormatNumber(input int) (string, error) {
    return fmt.Sprintf("number:%d", input), nil
}

text, err := workflow.ExecuteActivity(FormatNumber, time.Minute, 5) // text is string
result := <-workflow.ExecuteActivityAsync(FormatNumber, time.Minute, 5)
// result.Result is string; result.Err is error.
```

For an activity with a leading `context.Context`, use
`ExecuteActivityWithContext` or `ExecuteActivityAsyncWithContext`. The host
worker supplies the context; the isolate sends only the input value. These
APIs identify the function without executing it inside the isolate. Function
names and bound methods follow Temporal's short-name convention. Function
registrations with `activity.RegisterOptions{Name: ...}` are resolved by the
host, including registrations made after the workflow was registered.
`worker.New` honors `DisableRegistrationAliasing`; `worker.Wrap` assumes the
wrapped SDK worker uses the default aliasing setting.

Name-based calls support zero or multiple inputs and error-only activities:

```go
text, err := workflow.ExecuteActivityByName[string]("Greet", time.Minute, name)
result := <-workflow.ExecuteActivityAsyncByName[MyResult]("Compute", time.Minute, input, options)
_, err = workflow.ExecuteActivityByName[struct{}]("SendEmail", time.Minute, message)
```

For these calls the result type is explicit. Use `struct{}` for an activity
returning only an error. Prefixed struct registrations should be called by
name, matching the standard SDK. Name-based calls are passed through exactly
and are not rewritten by the function alias table.

When replaying a workflow that references aliased activity functions, call
`replayer.RegisterActivityWithOptions(fn, options)` with the same aliases as
the worker. These registrations supply metadata only; replay never executes
activities. Unaliased functions need no activity registration on the replayer.

Arguments and results use the default Temporal converter inside the isolate
and cross `Call` as protobuf-serialized `Payloads`. The host forwards them to
native activity functions without byte/string wrappers. Activity and converter
errors return the zero result with an error. Values remain restricted to the
same JSON/bytes/null subset as workflow arguments; custom converters and
protobuf message values remain TODOs. Previous `ExecuteActivity[R](name, ...)`
callers now use `ExecuteActivityByName[R](name, ...)`, and the previous async
name API becomes `ExecuteActivityAsyncByName[R]`.

When the host configures an isolate clock and timer operation, native
`time.After`, `time.NewTimer`, and `time.Sleep` use durable host timers.
