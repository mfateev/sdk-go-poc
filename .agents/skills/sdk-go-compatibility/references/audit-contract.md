# Compatibility audit contract

## Evidence and decisions

Compatibility has three independent requirements: complete inventory, equivalent observable behavior, and evidence from the current source/configuration. Report each separately. A compiling alias, populated struct, getter round trip, ported-test count, or passing replay alone cannot establish all three.

Use these statuses:

| Status | Meaning | Strict gate |
| --- | --- | --- |
| `pending` | Not assessed | Fail |
| `unverified` | Implementation exists; evidence is insufficient | Fail |
| `needs_decision` | Reconciliation requires user judgment | Fail |
| `unsupported` | A reproducible required behavior is absent | Fail |
| `compatible` | Same observable contract demonstrated | Eligible |
| `adapted` | Native API mapping preserves the agreed contract | Eligible with mapping rationale |
| `delegated` | Retained host behavior plus proof the POC preserves its wiring | Eligible with boundary evidence |
| `excluded` | Explicit user decision limits the claim | Fail for full parity; scoped gate only |

When source, tests, or documentation disagree and do not mechanically determine a single answer, record `needs_decision` and ask the user. Present the conflicting locations, affected behavior, alternatives, and consequences. Do not infer the intended contract from test quantity, implementation convenience, a POC design preference, or an older ledger classification. Continue independent checks while awaiting the answer. An approved adaptation or exclusion must retain the user's exact decision reference and relevant quote.

Example: a plugin mutates `WorkflowPanicPolicy` after the wrapper captures it. Demonstrating the different outcomes is mechanical; choosing whether the POC should honor that mutation, reject the configuration, or document an exception requires user input unless an existing explicit contract settles it.

## Complete source inventory

Inventory all baseline public packages and independently versioned contrib modules. Generate candidate inventories with the actual replaced host SDK, not a convenient sibling revision. Include:

- Exported functions, methods, constants, variables, named types and aliases, constructor/setter functions, struct fields, embedded members, interface methods and required plugin base embeddings.
- Every options/configuration field, including anonymous structs, nested structs, pointer members, map/slice elements, callback parameters, retry and priority policies, tuning and poller behavior. Do not restrict discovery to names ending in `Options`.
- Configuration from environment variables, flags, worker/global setters, functional options, runtime resources, and factory/configuration registration APIs. Include all supported source build variants.
- Client/start/signal-with-start/update/schedule options; workflow, activity, local activity, child, Continue-As-New, Nexus, session, signal/query/update handler and registration options; worker/replayer/test environment configuration; converters/codecs/failures/context propagation; plugin and contrib configuration.

The supplied AST helper includes all exported struct fields, including result/metadata structs, to avoid relying on a curated options list. It expands aliases and known nested types from supplied source roots. IDs include the exposed API path and declaration file, so separate build variants remain visible. Alias targets in `internal` are inspected even though internal packages are not independent public roots. Maps expand values; inspect struct-valued keys as well if configuration uses them.

Limitations requiring review: it does not type-check, calculate promoted Go method sets or generic substitutions, evaluate constant values, discover environment key strings, or traverse callback argument types. Unknown external named fields are listed in `external_references`; named external aliases/embeddings and parser failures are `issues`. Add pinned dependency source roots for SDK configuration types. For standard library/protobuf transport types, document why the exposed type can remain a leaf or supplement its configurable members. Resolve default import package names with supplied sources and verify remaining external names through type checking. Unrecognized syntax and dot imports require a better parser or a type-checked inventory, not an empty waiver.

Use the AST inventory as the floor. Cross-check exported method sets, build tags, generic APIs, external configuration shapes, and functional options with `go/types`/`go/packages` or compile probes under the proper toolchain. Add missed contracts as supplemental inventory items with stable IDs/fingerprints, source references, and reproducible generation. Generate these consistently in CI; do not append them by hand to make a single run pass. Supplemental discovery can use a separate deterministic inventory merged before initialization/checking.

## Per-field validation

Record the baseline type, default and validation rules, candidate mapping, source trace, and assertions for each field. Trace:

`caller -> plugins/defaults/validation -> host SDK -> copied bridge envelope -> isolate options -> operation -> request/command/history/result`

Not all steps apply to host-only options; prove where the value is consumed and why it does not need to cross the boundary. An alias can preserve the type while an adapter drops the value.

For each field, cover:

| Case | Required observation |
| --- | --- |
| Default | Actual default behavior when omitted/zero, including inheritance |
| Explicit | A distinguishable valid value reaches the operation and affects the result |
| Invalid | Rejection, error/panic type and timing, or a reason there is no invalid domain |
| Unset | Nil/empty/false/zero versus omission; pointer presence, copying, clearing and inheritance |
| Interaction | Documented precedence, dependent fields, conflicts, or a reason no interaction exists |

Use more than one explicit value when a boolean, enum, union, or range has different branches. Test zero as an intentional value where supported. Use nested retry/priority members with distinct values. For callbacks and interfaces, use instrumented fakes that record arguments, calls, ordering, ownership and lifetime; verify errors/cancellation/panics where the contract specifies them. Mirror upstream validation, including contracts that defer invalidity to a server.

Keep reflection/all-fields-populated tests as drift detectors. Add semantic assertions at request/command boundaries and in compiled native execution. Include defensive-copy tests only where the upstream contract promises copying or where crossing isolate ownership requires it. Never impose a new copying/lifetime contract without evidence or a user decision.

## Worker configurations

Enumerate every `worker.Options`, replayer option, tuner/poller/deployment option and nested member from the selected baseline. Derive exact names and rules from that revision. Cover these behavior families without treating this list as exhaustive:

- Workflow/activity/local-activity/Nexus concurrency, per-worker versus task-queue rates, fixed versus adaptive polling, slot suppliers/tuners, sticky scheduling, eager execution and capability-dependent defaults. Assert actual limits/poll requests using controlled barriers and instrumented services, not elapsed sleeps.
- Workflow-only/activity-only combinations, session workers and activity-only session queues, dynamic/named registrations, aliases and duplicate checks, Nexus service registration, marked native and ordinary SDK workflows on the same worker.
- Worker versioning/build/deployment routing, identity, capability negotiation, namespace/task queue inheritance, background activity context, interceptors, converters, propagators, logging/metrics, replay logging, payload limits, panic policy and fatal callbacks.
- `Start`, `Run`, `Stop`, startup errors, interrupt/cancellation, in-flight tasks, poll cancellation/no repoll, graceful timeout, local activities, sessions and Nexus shutdown, partial startup failure, cleanup and documented non-idempotency. Do not assume fatal paths invoke every normal shutdown callback.
- Global cache/setter behavior, native resource limits and bridge admission, factory configuration, `Wrap` with default/explicit options, replayer creation, and registration through plugin-provided registries.

For relevant cases run default cache and cache zero, ordinary replay and eviction/reconstruction, at least two independent executions to detect shared mutable state, and concurrency/race checks supported by the runtime. Use existing GOMAXPROCS 1/2/8 fixture checks when they cover the changed path. Record unsupported race/toolchain combinations explicitly.

## Plugins and extensions

Distinguish the SDK's plugin interfaces, interceptor interfaces, and optional contrib integrations. Inventory all three; they have different lifecycles. At discovery time, find plugins across every nested module, including unshipped optional modules. A module's absence from the POC becomes a visible gap or an explicit user scope decision.

For the currently observed plugin shape, verify:

- `client.Plugin`: `Name`, base embedding, `ConfigureClient` before validation, `NewClient`, lazy and existing-client cases, plugin propagation from clients to workers.
- `worker.Plugin`: `ConfigureWorker`, registry callbacks, `StartWorker`, `StopWorker`, `ConfigureWorkflowReplayer`, and `ReplayWorkflow`, including complete option structs and immutable instance-key/task-queue contracts.
- `temporal.SimplePlugin`: every option, converter/failure-converter replacement, propagator/interceptor append order, user-option precedence, before/after callbacks and per-replay lifecycle. Determine chain nesting and duplicate/name rules from baseline source/tests; do not invent them.
- Registry callbacks for normal/dynamic workflow and activity registrations and Nexus services. Register a marked native workflow from a plugin start/before callback and through any transformed registry callbacks. Confirm it reaches the isolate adapter and receives converters, aliasing and native configuration. Exercise replay registration too.
- Empty/single/multiple plugins, option mutation order, short-circuit errors, next-called/next-not-called semantics required by the baseline, context propagation and cancellation, partial startup failure, fatal errors, cleanup order, re-use versus per-instance state, and concurrent workers/replays.
- Contrib integrations' actual behavior, including available tracing generations, Datadog, OpenTracing, metrics, envconfig, sysinfo, payload storage, cloud workers, workflow streams and other modules discovered in the selected revision. Test supported transport/exporter lifecycle as well as native span/event relationships. Do not infer native coverage from host telemetry alone.

Host interfaces that carry clients, locks, closures, contexts or backend objects must not be assumed safe to copy into isolates. Verify native factory/byte-message adapters and read-only query/validator ownership. If an upstream plugin contract cannot be represented by the adapter, reproduce the discrepancy and ask the user how it should be reconciled.

## Scenario layers and regression sensitivity

1. Compile/type probes establish available types, fields and method sets under supported build configurations.
2. Unit/contract tests establish validation, defaulting, copying, callback and bridge/request behavior.
3. Paired upstream/native scenarios establish equivalent user-visible behavior, including negative cases.
4. Compiled isolate plus live server, cache eviction and saved-history replay establish actual worker execution and reconstruction. Use real server capability/configuration records for server-dependent behavior.

Validate that representative tests are sensitive: temporarily drop an option during bridge serialization, bypass native plugin registration, or omit a nested member in an isolated copy and confirm the corresponding tests fail. Do not mutate the shared working tree for this experiment. Choose a few meaningful negative controls; do not write tests that only reproduce the implementation.

Normalize incidental IDs/times only through an explicit list. Compare semantically important command payloads, headers, task queues, timeouts, retry policies, cancellation semantics and errors. Replay may not compare command inputs; use boundary assertions and recorded outcomes to cover those. Current POC fixture validation is distinct from a promise to replay historical POC versions.

## Matrix and gate schema

`gate.py init` creates pending entries. `check` requires a fresh `--candidate-inventory` as well as the baseline inventory and reviewed matrix. Inventory identities hash declarations, source digests and revisions of all supplied roots; absolute paths and git status are informational. Keep generated matrix/report/log artifacts under a hidden `.compatibility/` directory so regenerating them does not change source snapshots. The scanner hashes Go files (including tests), module metadata, Markdown and JSON, including saved JSON histories, under non-hidden directories; record noncovered runtime/binary/configuration inputs in run environment evidence too.

Top-level matrix fields:

- `baseline_identity`, `candidate_identity`: compute with `gate.identity()` on fresh inventories. Update only after drift review and rerunning affected scenarios.
- `reviews`: `inventory_completeness`, `build_variants`, `configuration_paths` each contain an evidence reference explaining the manual/type-checked supplement.
- `external_reviews`: one evidence string for every external type listed in either inventory, explaining its pinned expansion or leaf treatment.
- `entries`: one per baseline item, with its current `fingerprint`, `status`, `target`, `trace`, `reason`, and executed `evidence`. Adaptations include `mechanical_basis` or an explicit `decision` with `reference` and `quote`.
- `runs`: immutable evidence records from real test/CI execution. Each has `baseline_identity`, `candidate_identity`, `command`, `cwd`, `toolchain`, `environment`, `exit_code`, `result`, `log`, `log_sha256`, and `tests` mapping exact test IDs to `passed`/`failed`/`skipped`. Log paths resolve relative to the matrix. Record exact commands with shell arguments/env, server capabilities, compiler/runtime source identities, OS/architecture, build tags, cache mode and fixture revisions. Preserve actual logs and hash their bytes.

An evidence reference is `{"run":"native-options-cache0", "test":"TestActivityOptions/TaskQueue/explicit"}`. Each field's `cases` contains `default`, `explicit`, `invalid`, `unset`, and `interaction`; each case has `{"evidence":[...]}` or, except default/explicit, `{"not_applicable":"specific source-backed reason"}`. Multiple cases/fields may share a test only when its assertions distinguish each claimed behavior. A trace/reference is not a substitute for a passing semantic test.

For a decision, retain e.g. `{"status":"needs_decision", "question":"...", "conflicting_evidence":["source:line", "test:line"], "alternatives":["...", "..."]}` alongside the normal entry fields. The gate always rejects that state. Do not change it until the user responds or newly found evidence resolves the discrepancy mechanically.

The gate checks ID coverage, fingerprints, source identities, unresolved source issues, review records, per-field case evidence, passing named tests and log integrity. It cannot establish truthful assertions or executed-test lists from arbitrary prose. Generate run records from actual test results (prefer verbose/JSON output) and inspect logs for skipped subtests, absent matches and swallowed errors. A command returning zero with no matching tests is not evidence.

CI regenerates inventories and supplements, checks drift, runs baseline/native tests, creates current run evidence and evaluates the gate. Preserve full regression results as artifacts. Run fast inventory/compile/unit gates on relevant PRs and live/cache/replay gates before release or when affected behavior changes; install scheduling only if requested. Any approved scoped exclusions must be displayed in the release/report claim. A green structural gate is only one prerequisite for a compatibility conclusion.

When changing the bundled helpers, run `python3 scripts/test_helpers.py` from the skill directory. These self-contained synthetic checks exercise inventory discovery and gate rejection behavior; their logs are never SDK compatibility evidence.
