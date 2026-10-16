# sdk-go compatibility audit — 2026-10-16

Full compatibility with official sdk-go v1.49.0 is **not established**. The strict evidence gate fails. Two plugin integration discrepancies were reproduced, context-free activity registration differs, and native SideEffect APIs are absent. No SDK behavior, dependency pin, CI configuration, or published artifact was changed.

This is a negative compatibility assessment with a complete row set for the generated inventory, not an exhaustive behavioral certification. Most rows remain unverified. Test-port dispositions and API aliases were not promoted to compatibility evidence.

## Comparison and freshness

| Component | Audited source |
| --- | --- |
| Official feature baseline | sdk-go **v1.49.0**, `58115b581391037b93451f50ac3628611ea0493b`, clean isolated worktree `/tmp/sdk-go-official-audit-v1.49.0` |
| Actual host fork | `github.com/mfateev/temporal-go-sdk v1.49.0-isolates.1.0.20261012175541-b87b0a1d5dda`, revision `b87b0a1d5dda22d68ddb97237fdc42cd2492043f` |
| POC | `c91e13324d62489202663d50be38354795fdec1f`; no initial tracked changes; untracked `.idea/` left untouched |
| Runtime/compiler checkout | `04ee2113dc0805b35188dba1e064c0035d7e9366`; untracked `.idea/` and `go-build-cache/` |
| Custom compiler binary | reports `go1.28-devel_dba1c0fd35`, linux/arm64; binary hash and environment in `metadata.json` and `toolchain-env.json` |
| Standard compiler | Go 1.26.7, linux/arm64 |
| Sibling SDK checkout | `fbc4738d28663daf230d32456ff8f082d9531c5a`; deliberately not substituted for the actual dependency |
| Live server | CLI 1.9.1 / Server 1.32.0, in-memory, namespace default; `frontend.enableServerVersionCheck=false` |

`host-module.json` records the actual `tgo list -m -json go.temporal.io/sdk` result and checksums. `modules.json` captures all selected module versions, including tracing integrations. Inventories include nested contrib modules and all parsed source variants. The pinned fork differs from the official tag in seven files, adding isolate bindings, integration tests/docs and CI. The host activity registry implementation involved in the context-free probe is unchanged in that diff.

These artifacts describe the named source revision. An audit-only commit changes Git HEAD; the recorded evidence must not be described as a run against that later revision. A future audit must review drift and regenerate evidence rather than rewrite these identities. The compiler binary's reported revision differs from the checkout HEAD; both are recorded, and binary/source build reproducibility was not established.

## Reproduced findings

### P1 — High: plugin registries bypass native registration

The public POC wrapper converts a marked `context.Context` workflow into a `temporalbridge.Factory` (`worker/worker.go:135–151`). `TestPluginRegistrationControl` confirms that conversion reaches the host callback. In contrast, a plugin's `StartWorker` registry exposes the underlying host worker. Registering the same marked workflow there panics:

```text
expected first argument to be workflow.Context but found context.Context
```

The same failure occurs through `ReplayWorkflow`'s replay registry. Both probes stop before polling/history interpretation, so the failure is registration wiring, not a server or history mismatch. Official `internal/plugin.go:216–232,285–300` supplies these registration hooks; the POC inherits the underlying start/replay path without wrapping the registries. Converter, resource and interceptor setup inside the POC registration path is consequently bypassed too; downstream effects were not separately exercised after the registration panic.

Evidence: `probes/plugin_test.go`, `logs/plugin-control.jsonl`, `logs/plugin-start.jsonl`, `logs/plugin-replay.jsonl`. The two negative probes exit 1. The direct-registration control passes.

### P2 — Medium: plugin-mutated panic policy escapes the wrapper check

Official plugins may mutate worker options before validation (`internal/plugin.go:108–116,172–186`). `worker.New` calls the host constructor with an options value, then captures `WorkflowPanicPolicy` from its own original copy (`worker/worker.go:36`). A plugin setting the effective host policy to `FailWorkflow` therefore leaves the wrapper's `failWorkflowOnPanic` false. Native registration succeeds even though explicit `FailWorkflow` is rejected by `worker/worker.go:136–138` and existing native tests.

Evidence: `TestPluginPanicPolicyMutation`, `logs/plugin-policy.jsonl`, exit 1. This demonstrates inconsistent registration-time validation. It does **not** establish the eventual workflow panic outcome under the mutated policy.

For P1/P2 the reconciliation remains **needs_decision**. Alternatives are native-aware plugin registries with effective post-plugin options, or explicit rejection of these configurations. The user's answer in this audit was: **“Leave the behavior decision open.”** No adaptation or exclusion was inferred. Registry activity/Nexus/dynamic methods share the affected registry object but were not each tested; their rows remain blocked by that unresolved contract rather than being reported as individually reproduced failures.

`DisableRegistrationAliasing` is also captured from the pre-plugin options in worker and replayer constructors. This is a source-level audit hypothesis; an aliasing behavior mismatch was not reproduced.

### A1 — Medium: context-free activities accepted by sdk-go are rejected

`TestActivityContextOptionalBaselineComparison` registers the same `func() error` on the pinned host worker and on the POC wrapper. Host registration succeeds; POC registration panics with `activity must take context.Context first`. `internal/activityref/name.go:31` imposes that additional restriction on worker registration. The repository documents and tests this restriction; it still prevents unqualified sdk-go feature compatibility without an explicit scope decision.

Evidence: `logs/activity-context.jsonl`, exit 0, containing assertions for both different outcomes. This is a passing discrepancy probe, not a compatibility pass. Dynamic activity and replayer paths also call the validator; only normal worker registration was directly compared.

### A2 — Feature gap: native SideEffect and MutableSideEffect are omitted

The official baseline exports `SideEffect`, `SideEffectWithOptions`, `MutableSideEffect`, `MutableSideEffectWithOptions` and their options. The native API omits them, explicitly documented in `API_COVERAGE_PLAN.md` and `SDK_REFERENCE.md`. Ordinary host SDK workflows still expose them. Native source/API absence is established; no synthetic implementation or migration shim was added. These entries are unsupported for a full native feature claim. A repository statement of omission is not a user-approved audit exclusion.

Other documented restrictions—variadic handlers, selected protobuf layouts/resolvers and external-storage payload references—need contract-specific follow-up. The test ledger's `user_deferred`, `obsolete` or `out_of_scope` labels were not used to remove baseline rows.

## Inventory and evidence

The Go 1.26 parser initially rejected the Go 1.27 generic-method variant. Final inventories use `tgo`'s newer parser. Exact dependency sources resolve testify/OpenTelemetry embeds. Minimal copies of `io`, `math/rand/v2` and `isolate` source files, with package module names, resolve standard/runtime aliases without incorrectly naming them `std/io`. Their source provenance is in `stdlib-sources/manifest.json`. Initial inventories and broad standard-library experiments are retained locally but are not the final gate inputs.

| Inventory | Count |
| --- | ---: |
| Baseline AST items | 7,812 |
| Baseline literal configuration sites added | 14 |
| Final baseline/matrix entries | 7,826 |
| Candidate AST items | 2,308 |
| Candidate literal configuration sites added | 81 |
| Final candidate items | 2,389 |
| Unresolved parser/alias issues in final inventories | 0 |
| Distinct external type references needing review | 61 |

One matrix row exists for every baseline item, including fields, callbacks, expanded interfaces, deprecated APIs and nested modules. `assemble.py` deterministically adds literal environment reads and flag sites and records source-based mapping candidates. It refuses to replace an existing matrix. Counts are declaration paths, including repeated exposures through aliases; they are not counts of independent features. Method sets, callback argument graphs, external configuration types such as protobuf retry/priority and TLS/Nexus options, dynamically computed environment keys, and all supported build combinations still require deeper inventory/type review. The three completeness review gates are intentionally unsatisfied.

| Status | Rows |
| --- | ---: |
| unverified | 6,985 |
| pending/unmapped | 817 |
| unsupported | 12 |
| needs_decision | 12 |
| compatible / adapted / delegated | 0 |
| approved exclusions | 0 |

| Overlapping family | Rows | Evidence limitation |
| --- | ---: | --- |
| Options/configuration/policies/tuners/pollers/limits | 2,750 | Name-based reporting group; per-field default/explicit/invalid/unset/interaction cases not fully demonstrated |
| Worker | 534 | Host aliases and boundary tests do not cover every concurrency, lifecycle, session, tuner or deployment setting |
| Plugins | 531 | Includes nested option expansions; registration and policy discrepancies above; client lifecycle/chain behavior not exhaustively paired |
| Contrib | 791 | 681 pending, 110 unverified; independently versioned module versions and native integration need separate review |

Inventory completeness, behavior and freshness are separate: generated ID coverage is complete for these inputs; semantic inventory completeness and behavioral compatibility are incomplete; recorded logs identify the audited snapshots. Native tests and host delegation are kept distinct. No successful row status was assigned merely because a similarly named source declaration or getter test exists.

## Executed validation

| Command / scenario | Observed result |
| --- | --- |
| `../golang-go/bin/tgo test -json ./...` | Exit 0; 494 passed test cases, five skipped live cases; 44 passed packages, 44 packages without tests; no cached results |
| `tgo run ./internal/cmd/testports -upstream /tmp/sdk-go-host-audit-b87b0a1` | Exit 0; 1,497 entries: 243 covered, 857 retained, 39 obsolete, 332 out_of_scope, 26 suite. This is ledger integrity, not full feature parity |
| Official canonical runner, focused options/plugin regex | Exit 0; 24 actual tests passed across the root/nested modules selected by the runner, standard Go 1.26.7 with race enabled; modules with no regex matches provide no behavioral evidence |
| Official root `tgo test -json -run '^$' ./...` | Exit 0; compile/type check including Go 1.27 source variant; zero executed tests, recorded separately as a check |
| Compiled dynamic checker, `-live`, cache 1000 and 0 | Exit 0; dynamic workflow/update/query, converter/interceptor paths, completed query, fresh recorded histories and replay; histories retained under `live-histories/` |
| Direct marked native plugin registration control | Passed |
| Plugin startup native registration | Failed/panic reproduced |
| Plugin replay native registration | Failed/panic reproduced |
| Plugin panic-policy mutation consistency | Failed discrepancy assertion |
| Context-free activity comparison | Passed discrepancy assertion |

The native suite includes compiled isolate fixtures and saved-history replay; API coverage checks exercise GOMAXPROCS 1/2/8. The five skipped tests are named in the JSON log. Only dynamic handlers received an additional live run here. The full server capability response was not captured. Native race/ownership variants, exhaustive eviction/lifecycle interactions, negative-control bridge field drops, all OS/build tags and paired official/native command-attribute comparisons were not completed. Replay does not by itself verify every payload or command attribute.

`matrix.json` holds source identities, mappings and real run records with exact test outcomes and log hashes. Failed and skipped outcomes remain visible. Raw successful runs are supporting evidence, not field-by-field compatibility certificates. The strict gate fails; `logs/gate.log` includes the failures and summary. See `REPRODUCE.md` for commands.
