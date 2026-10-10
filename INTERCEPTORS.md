# Workflow interceptors and observability

Use `github.com/mfateev/sdk-go-poc/interceptor` for isolate workflow interception.
Its inbound/outbound chain follows the Go SDK's `WorkerInterceptor` pattern, with
standard `context.Context` and native signal channels. Embed the provided base
types and forward operations through `Next`.

## Configure an isolate-owned chain

```go
// In a workflow package:
//go:isolate
func NewInterceptors(config []byte) ([]interceptor.WorkerInterceptor, error) {
    return []interceptor.WorkerInterceptor{&MyInterceptor{}}, nil
}

// In the host, before starting the worker:
err := worker.SetIsolateInterceptors(w, worker.InterceptorOptions{
    Factory: workflows.NewInterceptors,
    Config:  []byte("settings"),
})
```

The factory runs inside each execution and creates private state. Configuration
bytes are copied; no host interceptor, converter, exporter, network client, or
lock is passed into an isolate. The compiler combines the workflow, interceptor
factory and optional converter factory package layouts, initializing each
selected package once. Their normal Go function signatures are preserved.

The first returned interceptor is outermost for inbound calls. Its `Init` can
wrap the outbound chain before forwarding `Next.Init`, as in the Go SDK. New
executions snapshot configuration; cached workflows keep their original chain.
Configure replayers through the same `SetIsolateInterceptors` API.

`worker.Options.Interceptors` retains its existing meaning for ordinary SDK
workflows and host activities. Configure those with the **upstream** SDK
interceptors; configure isolate workflows with the factory above. Ordinary
workflows registered on the same worker continue to use the original SDK path.

Supported inbound hooks are workflow execution, signals, queries, update
validation and update execution. Signals traverse hooks when admitted from
history, including signals without a channel receiver. A hook can modify,
rename or filter a signal before native channel delivery. Buffered signal hooks
are drained before application startup and completion.

Supported outbound hooks cover activities, local activities, child workflows,
external/child signals, external cancellation, Nexus, Continue-As-New, versioning,
signal/query/update registration, workflow metadata, logger, metrics, replay,
`workflow.Now` and `workflow.Sleep`. Native `go`, channel operations, `select`,
`time.Now` and native timers are runtime primitives, not SDK interceptor hooks.
SideEffect and MutableSideEffect remain intentionally excluded.

Queries and update validators build fresh scratch-owned chains from the same
factory/configuration. They can mutate their own temporary interceptor state,
but cannot change the cached workflow or interceptor state. Invalid mutations
panic within the query/validation request without killing the workflow isolate.
Completed workflows with query handlers retain their state until eviction.

## Logging and metrics

`workflow.GetLogger(ctx)` implements the SDK logger interface.
`workflow.GetMetricsHandler(ctx)` implements the SDK metrics interface, including
counter, gauge, timer and tagged handlers. Dedicated copied byte encodings feed
the host's configured SDK logger and metrics backend. Log field values are
formatted to strings inside the isolate; this POC does not preserve arbitrary
structured field types. Metric integers and floating point bits retain precision.

Custom telemetry/interceptors can use `workflow.NewSink(op).Emit(bytes)` and a
matching `worker.RegisterSink` handler. Numeric operations identify sinks;
optional names label diagnostics. Interceptors own their byte serialization.
**Observations do not pass through DataConverter, payload codecs or encryption.**
Headers likewise preserve copied protobuf payload bytes independently of the
application converter; tracing headers use the SDK-compatible JSON carrier.

Writes are one-way observations. They do not create history events or wait for
acknowledgments. The runtime queue holds at most 64 messages of 64 KiB each;
overflow drops messages. Backend errors/panics are host diagnostics. Replay
metrics and custom sinks are suppressed by default; logging follows the SDK's
replay setting or a custom log handler. Activities provide durable external work.

## OpenTelemetry

`contrib/opentelemetry` adapts the SDK v1 tracing interceptor. Its default private
provider generates IDs using deterministic isolate randomness and emits completed
spans to the configured sink:

```go
//go:isolate
func NewInterceptors(config []byte) ([]interceptor.WorkerInterceptor, error) {
    tracing, err := opentelemetry.NewTracingInterceptor(
        opentelemetry.TracerOptions{SinkOp: 0x10200},
    )
    if err != nil { return nil, err }
    return []interceptor.WorkerInterceptor{tracing}, nil
}
```

Register `contrib/opentelemetry/exporter.RegisterProcessor(w, op, processor,
resource)` on the host to share an existing SDK span processor. Alternatively,
`exporter.Register(w, op, spanExporter, resource, batchOptions...)` creates a
bounded host batch processor and returns its shutdown function. Stop workers
before flushing/shutting it down. Exporter resources, credentials, samplers and
network connections belong on the host. Retain one owner for shared shutdown.

`contrib/opentelemetry-v2` adapts the SDK's v2 tracing contract. Its default is
propagation without additional Temporal spans. To add those spans, set
`TracerOptions: interceptor/tracing.TracerOptions{AddTemporalSpans: true}` in its
options. It uses the same private provider and host exporter bridge. Use the
upstream v2 plugin/provider separately for host client and activity tracing.

Workflow code can use `trace.SpanFromContext(ctx)`, `span.TracerProvider().Tracer`
and standard span attributes/events/links. The default propagator carries W3C
Trace Context and baggage. New root traces are sampled; children inherit parent
sampling and trace state. This POC does not implement a configurable in-isolate
sampler. Optional private tracers/propagators must obey isolate ownership and
effect rules; a normal global OTel SDK provider with exporters is unsuitable.

The host reconstructs span data with the exact trace ID, span ID, parent ID,
clock timestamps, status and typed attributes. Client, workflow and activity
parent links therefore survive the byte boundary. Span IDs consume randomness
on replay too, while the host suppresses export, preserving downstream headers.
Original run identity supplies the replay-stable SDK idempotency key. Stable
execution metadata is copied when the chain is built and reused by tracing;
per-span metadata Calls would yield and could reorder cancellation commands.

Queries/validators receive a host-generated observation token used only to make
their tracing span IDs distinct. It does not seed or advance the writable
workflow's random stream, change query results or enter workflow commands.
Application random APIs in scratch scopes retain their documented behavior.

**Security:** workflow tracing IDs are predictable, like other deterministic
isolate random values. They are not secrets, authorization tokens or cryptographic
entropy. See [randomness and security](README.md#deterministic-randomness-and-security).
Observations bypass application payload encryption: avoid putting secrets in
names, attributes, baggage, log fields or metric tags.

Spans are observations and may be dropped. Unfinished spans are not flushed when
an isolate is killed/evicted. Fine-grained span limits and comprehensive OTel API
support remain productization work. The audited OTel API version is 1.44.0.

## Datadog

`contrib/datadog/tracing.NewTracingInterceptor(TracerOptions{SinkOp: op})` creates
a private workflow tracer with SDK-compatible Datadog header keys, operation
names, resource names and log correlation fields. It supports W3C and legacy
Datadog carriers, 128-bit trace IDs, sampling priority, origin and propagated tags.

On the host, `contrib/datadog/exporter.Register(w, op, Options{Service: service})`
uses the configured Datadog tracer. `OnFinish` and optional `SpanStarter` run
exclusively on the host. Use the upstream Datadog interceptor for client and
activity tracing. The host preserves deterministic span IDs and parent links;
normal Datadog sampler/backend lifecycle stays outside isolates.

## OpenTracing

`contrib/opentracing.NewInterceptor(TracerOptions{SinkOp: op})` creates a
private OpenTracing tracer backed by the same native span provider and copied-byte
sink as OpenTelemetry. It never uses or installs a process-global tracer.

```go
//go:isolate
func NewInterceptors(config []byte) ([]interceptor.WorkerInterceptor, error) {
    tracing, err := opentracing.NewInterceptor(opentracing.TracerOptions{
        SinkOp: 0x10200,
    })
    if err != nil { return nil, err }
    return []interceptor.WorkerInterceptor{tracing}, nil
}
```

Register `contrib/opentelemetry/exporter.RegisterProcessor` or `Register` on the
host using that operation. The host exports completed spans with their original
IDs, parents, timestamps, tags and logs. No arbitrary OpenTracing backend is
asked to generate replacement IDs. Replay constructs the same spans/headers and
suppresses export. The same predictable-ID and unencrypted-observation security
restrictions described above apply.

Workflow instrumentation uses standard OpenTracing APIs with the private tracer:

```go
parent := opentracing.SpanFromContext(ctx)
span, ctx := opentracing.StartSpanFromContextWithTracer(ctx, parent.Tracer(), "work")
defer span.Finish()
span.SetTag("customer", "example")
span.LogKV("event", "started")
```

Here `opentracing` is `github.com/opentracing/opentracing-go`; use a distinct alias
for our contrib adapter. Context hooks also expose the underlying OTel span for
mixed API instrumentation. Baggage contexts are immutable snapshots and propagate
to future children. Queries can read cached contexts; mutating cached spans or
baggage is rejected without killing the workflow.

TextMap and HTTPHeaders carry **W3C Trace Context and baggage** in the SDK's
`_tracer-data` header. Binary uses a bounded length-prefixed JSON carrier specific
to this bridge. For host clients/activities, configure the upstream Temporal
OpenTracing interceptor with `NewBridgeTracer(hostOTelProvider, nil)` from our
contrib package, or use a W3C-compatible OTel integration. Existing Jaeger/B3
headers are not automatically translated. An optional private propagator supplied
to `NewBridgeTracer` can provide other formats with explicit compatibility.

ChildOf references choose the primary parent; FollowsFrom retains the causal
trace/parent and is also represented as a typed link. Additional references are
links. Tags preserve signed integers and floating point bits; unsigned integers
above MaxInt64 become exact decimal strings. Unsupported tag types are ignored.
Logs become span events; arbitrary log objects are formatted immediately, and
lazy log callbacks execute inside the isolate when logged. FinishWithOptions and
deprecated logging methods are supported. New roots are sampled, with child
sampling inherited from the parent; configurable sampling remains future work.
The audited OpenTracing API version is 1.2.0.

## Verification

From this repository, using the custom compiler/runtime:

```sh
../golang-go/bin/go test ./example/interceptors/check ./example/tracing/check
../golang-go/bin/go build -o /tmp/isolate-tracing-check ./example/tracing/check
/tmp/isolate-tracing-check -history example/tracing/testdata/v1.json
/tmp/isolate-tracing-check -history example/tracing/testdata/v2.json
/tmp/isolate-tracing-check -history example/tracing/testdata/datadog.json
/tmp/isolate-tracing-check -history example/tracing/testdata/opentracing.json
```

The checks run fresh processes at GOMAXPROCS 1/2/8, exercise chain ordering,
configuration copying, factory/converter coexistence, signal filtering/renaming,
read-only failures, query span uniqueness and replay-stable activity headers.
The API coverage checker also replays the existing external/local/session/Nexus
history corpus with all four adapters, including histories without trace headers.
Saved tracing histories cover real-server runs interoperating with upstream client and
activity integrations for all four adapters. To repeat live checks, start a
Temporal development server, then run:

```sh
/tmp/isolate-tracing-check -live localhost:7233 -histories /tmp/isolate-tracing-histories
```

Live checks also cover ordinary SDK workflows on the same configured worker,
completed queries, update hooks, exact cross-worker parent links and replay export
suppression. Native-context workflows are started through the unmodified SDK
client by registered workflow **name**, as in the examples.
