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
../golang-go/bin/go build -isolate-dir=./example/order -isolate-dir=./example/signal -isolate-dir=./example/clock -o worker ./example/worker
```

Run `./worker` against a local Temporal server (or set `TEMPORAL_ADDRESS`). It
registers workflow types `IsolateOrder`, `IsolateEcho`, `IsolateTypedEcho`,
`IsolateSignal`, `IsolateClock`, and `PlainEcho` on task queue `isolate-poc`,
plus the host activity `echo`.
`IsolateOrder`, `IsolateEcho`, and `IsolateTypedEcho` are separate named
functions in the same `example/order` isolate program. Each execution has its
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
temporal workflow execute --workflow-id isolate-poc-plain-echo --type PlainEcho --task-queue isolate-poc --input '"hello"'
temporal workflow start --workflow-id isolate-poc-signal --type IsolateSignal --task-queue isolate-poc
temporal workflow signal --workflow-id isolate-poc-signal --name ready --input c2lnbmFsLXJlc3VsdA== --input-base64 --input-meta encoding=binary/plain
temporal workflow result --workflow-id isolate-poc-signal
temporal workflow execute --workflow-id isolate-poc-clock --type IsolateClock --task-queue isolate-poc
```

Export and replay a completed history with a fresh isolate process:

```sh
../golang-go/bin/go build -isolate-dir=./example/order -isolate-dir=./example/signal -isolate-dir=./example/clock -o replay ./example/replay
temporal workflow show --workflow-id isolate-poc-order --output json > /tmp/isolate-order-history.json
./replay temporal-order IsolateOrder /tmp/isolate-order-history.json
temporal workflow show --workflow-id isolate-poc-clock --output json > /tmp/isolate-clock-history.json
./replay temporal-clock IsolateClock /tmp/isolate-clock-history.json
```

Run the local bridge driver without a server:

```sh
../golang-go/bin/go build -isolate-dir=./example/order -isolate-dir=./example/signal -isolate-dir=./example/clock -o /tmp/isolate-temporal-driver ./example/driver
/tmp/isolate-temporal-driver
```

`workflow` is the API imported by isolate code. `temporalbridge` stays in the
host and depends on the pinned Temporal Go SDK. This initial adapter accepts
serial workflow programs only. The fork still needs a native quiescence barrier
and deterministic scheduling for concurrent workflow goroutines and replay.
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

An isolate directory can register several named workflow functions from `init`:

```go
func init() {
    workflow.Register("IsolateOrder", OrderWorkflow)
    workflow.Register("IsolateEcho", EchoWorkflow)
    workflow.RegisterTyped2("IsolateTypedEcho", TypedEchoWorkflow)
}

func main() {
    if err := workflow.Run(); err != nil { panic(err) }
}
```

Byte handlers have the signature `func([]byte) ([]byte, error)`. `Run` obtains the
workflow name and input from the host, calls the registered handler, and sends
its result or error back. `temporalbridge.Register` uses its Temporal workflow
name as the handler name; `temporalbridge.RegisterEntry` allows the two names to
differ. Registration only fills isolate-owned state and must not call the host.
The static build still requires a small `main` dispatcher in each isolate
directory; workflow logic belongs in named functions.

Typed handlers return `(R, error)` and use `RegisterTyped0`, `RegisterTyped`, or
`RegisterTyped2` for zero, one, or two arguments. These helpers invoke the Go
function directly. Compiler-generated registration for arbitrary signatures
and automatic `worker.RegisterWorkflow(fn)` routing are future work; ordinary
non-isolate registrations already use that API unchanged.

`workflow.GetSignalChannel(name)` provides a channel for signals with that
name. It wraps the existing host `Call` in an isolate-owned goroutine and
returns `workflow.SignalResult` values, including any host error. `NextSignal`
still receives the next signal of any name. The host keeps other named signals
queued for their matching channels.
`workflow.ExecuteActivityAsync` wraps `ExecuteActivity` the same way and returns
a channel with one `workflow.ActivityResult` before closing.
When the host configures an isolate clock and timer operation, native
`time.After`, `time.NewTimer`, and `time.Sleep` use durable host timers.
