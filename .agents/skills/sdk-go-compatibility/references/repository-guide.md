# Repository discovery guide

Observed on 2026-10-16; rediscover pins and source state on every audit.

The local task workspace is `/Users/maxim/workarea/workspaces/projects/tasks/modify-go-runtime-for-isolates/` with siblings `sdk-go-poc`, `temporal-go-sdk`, `golang-go`, and `samples-go-poc`. Locate equivalent checkouts rather than requiring these absolute paths elsewhere.

## Baselines and existing evidence

The POC module is `github.com/mfateev/sdk-go-poc`. Its observed `go.mod` requires `go.temporal.io/sdk v1.49.0`, replaced by `github.com/mfateev/temporal-go-sdk v1.49.0-isolates.1.0.20261012175541-b87b0a1d5dda`. The sibling Temporal checkout was ahead of that pin when this skill was written. Resolve the dependency's `Dir`/`Replace` using the selected toolchain, and distinguish the official v1.49.0 baseline from this modified fork. Never assert that a fork checkout proves compatibility with official upstream.

Read POC `README.md`, `SDK_REFERENCE.md`, `INTERCEPTORS.md`, `API_COVERAGE_PLAN.md`, and the introduction/feature decisions of `TEST_PORTING.md`. Search the latter for relevant names instead of loading the whole generated table. The table and `internal/testports/inventory.json` are maintained by `internal/cmd/testports`; preserve them and reuse their exact source pin. Existing `covered`, `retained`, `obsolete`, or `out_of_scope` statuses do not automatically settle a feature compatibility discrepancy.

The task's `AGENTS.md` says previous POC versions do not need compatibility shims before release. This is distinct from feature compatibility with official sdk-go. Do not silently expand or narrow either contract; ask the user if reconciliation requires a decision.

## Source hotspots

| Area | POC | Host SDK |
| --- | --- | --- |
| Worker | `worker/worker.go` and adjacent configuration files | `worker/worker.go`, `internal/worker.go`, worker construction/lifecycle files |
| Options | `workflow/*options*`, operation implementations | `workflow/*options*`, public aliases and their `internal` definitions |
| Copied boundary | `temporalbridge/`, `workflow/` startup/options/operation envelopes | Workflow definition factory and operation bindings |
| Plugins | Worker wrapper, factory setters, contrib adapters | `client/plugin.go`, `worker/plugin.go`, `temporal/plugin.go`, `internal/plugin.go` and plugin call sites |
| Native interceptors | `workflow/interceptor*`, `worker/`, `contrib/`, `INTERCEPTORS.md` | `interceptor/`, worker/client interceptors and contrib plugins |
| Tests | `workflow/upstream_options_test.go`, worker tests, `example/*/check`, fixtures, test-port ledger | Original unit/contract tests, `test/` module, nested contrib modules |

The POC's `worker.Options` and replayer options are host aliases. `worker.New` passes options to the host and retains selected registration/panic settings. `Wrap` adds native registration to an existing worker. Verify options after plugin mutation, every registry path, and marked/ordinary registrations; these observations identify places to test, not confirmed defects.

Client/activity/common Temporal APIs continue to come from `go.temporal.io/sdk`; workflow/worker/testsuite APIs are adapted by the POC. Native contexts/goroutines/channels/time are deliberate API mappings. Record their equivalence basis; do not recreate the old workflow concurrency facade. Tracing adapters are separate from upstream host plugins; verify each configured layer.

## Commands

Use the local POC/runtime instructions to select `tgo`. A source build may expose it at sibling `golang-go/bin/tgo`. Standard Go can run the AST helper because it does not import the POC; native workflow tests/builds need the custom toolchain. An older parser may flag future build variants as unresolved; do not silently discard them.

From the POC root, with a valid `tgo`:

```sh
tgo list -m -json go.temporal.io/sdk
tgo version
tgo run ./internal/cmd/testports -upstream /path/to/exact-test-ledger-source
tgo test ./workflow ./worker ./temporalbridge ./testsuite
```

Select existing checker packages and live-test environment variables by reading their tests/docs. Do not guess a universal live-test flag. Build runnable native checkers with `tgo build` before execution; the README distinguishes worker builds from `tgo run`. Confirm tests actually executed and did not skip because a server environment flag was absent.

For the observed host SDK checkout, `AGENTS.md` prefers these commands from `internal/cmd/build/`:

```sh
go run . check
go run . unit-test -run 'RelevantTest'
go run . integration-test -dev-server -run 'RelevantSuite/RelevantTest'
```

Its current build tool has custom-toolchain support; read its flags and the exact pinned checkout's instructions before selecting `go` versus `tgo`. `test/` and many `contrib/*` directories are separate modules and are not covered by a root `./...`. For native lifecycle/execution changes, use the documented cache-zero mode (commonly `WORKFLOW_CACHE_SIZE=0`) and existing GOMAXPROCS 1/2/8 replay checks.

The existing option getter tests deliberately populate every exported field. Keep them as drift alarms; additionally inspect bridge serialization, host scheduling arguments, command attributes and live results. The repository documents that replay does not compare every activity input, so a replay pass cannot substitute for those assertions.

Keep temporary demos under `/tmp`; a full audit can use `.compatibility/` under the POC for matrix, report and run logs, following any established repository convention. Integrating a checked-in compatibility harness or changing SDK behavior is separate from creating this reusable skill.
