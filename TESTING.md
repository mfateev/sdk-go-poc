# Testing the native SDK

Use the matching SDK-built `tgo`; source setup is in [README.md](README.md).
Run commands from the SDK root unless stated otherwise.

## Routine checks

```sh
tgo test -timeout=20m ./...
tgo test -race -p=2 -timeout=20m ./...
tgo build -race -o /tmp/isolate-driver ./example/driver
/tmp/isolate-driver
```

The driver executes native serial/concurrent scenarios. Compiled checker packages
use `internal/checktest` from TestMain: build once per package with a 10-minute
budget, run scenarios in fresh processes with separate 2-minute budgets, remove
the executable when the package exits. A slow build does not consume the replay
execution budget. The outer 20-minute timeout is per test package.

Native marking includes dependency, internal and external `_test.go` declarations,
registration before TestMain and `test -c`. Marked builds do not support `-cover`.
Unit tests through the testsuite facade run native workflows, while activities,
mocks and virtual-time engine remain host SDK work. Host mock callbacks cannot
call workflow APIs; use ReturnIsolate for a marked replacement. Supply `t`/`s.T()`
to NewTestWorkflowEnvironment for cleanup.

## Focused compiled behavior

| Package/fixture | Contract |
| --- | --- |
| `internal/toolchain` | Discovery, test graphs, dependency provenance, module mirrors and private protobuf state selection |
| `workflow`, `temporalbridge`, `worker`, `testsuite` | APIs, forwarding, task ordering, cancellation, retirement and test facade |
| `example/determinism/check`, `example/random/check` | Full observation traces, distributions, named streams, reset and replay |
| `example/protobufvalues/check` | Binary/JSON values, independent registries/caches, scratch cloning, custom messages/resolvers |
| `example/query/check`, `example/update/check`, `example/child/check` | Read-only handlers, admission, children and structured errors |
| `example/converter/check`, `example/contextpropagation/check`, `example/failureconversion/check` | Native factories, per-context serialization, headers, host codecs and errors |
| `example/dynamic/check`, `example/workflowstreams/check` | Dynamic fallback and stream state/continuation |
| `example/tracing/check`, `example/workflowmetadata/check` | Observations, trace identity, headers and previous-run metadata |
| `example/resources/check`, `example/lifecycle/check`, `example/effects/check` | Limits, failures, cleanup and denied effects |
| `example/plugins/check`, `example/contextfree/check` | Plugin registration and optional activity contexts |

For example, `tgo test -race -count=1 ./example/protobufvalues/check` builds and
runs its native tests with compulsory ownership instrumentation.

## Replay

```sh
tgo build -o /tmp/isolate-replay ./example/determinism/check
GOMAXPROCS=1 /tmp/isolate-replay
GOMAXPROCS=2 /tmp/isolate-replay
GOMAXPROCS=8 GODEBUG=cpu.all=off TZ=America/Los_Angeles /tmp/isolate-replay
```

Run normal and CPU-feature-disabled variants at all three processor counts when
changing runtime numerical/scheduling behavior. Saved histories are runnable
fixtures, not documentation logs. Checkers compare freshly computed observations
against recorded activity results/completion and reject deliberately corrupted
outcomes. Temporal replay alone may not compare activity input payloads.

For a changed integration, record representative histories with a live server and
replay in fresh processes. Test default sticky cache and `WORKFLOW_CACHE_SIZE=0`
where the fixture supports it; SDK live checkers may run both internally. Verify
payload/header/command attributes separately where replay does not. Source pins
and exact build settings are needed to assess an upgrade/rollback comparison.
The POC does not maintain old protocol implementations merely because a numeric
marker matches; application workflow versioning remains necessary.

## Live and soak tests

Ordinary tests do not automatically start every server-dependent fixture. Existing
opt-in commands include:

```sh
PLUGIN_DEV_SERVER=1 tgo test ./example/plugins/check -run TestLivePluginRegistrations -count=1
CONTEXT_FREE_DEV_SERVER=1 tgo test -timeout=10m -count=1 ./example/contextfree/check
STREAMS_DEV_SERVER=1 tgo test -count=1 ./example/workflowstreams/check -run TestLiveWorkflowStreams
ISOLATE_STRESS=1 tgo test ./example/resources/check -run '^TestIsolateStress$' -count=1 -v -timeout=23m
```

Use fixture flags for checkers targeting an existing server. These scenarios
create executions and may create/delete temporary Nexus endpoints. Their commands
belong in an explicitly selected local test run. [Stress details](example/resources/STRESS.md)
explain the soak duration, leak thresholds and shorter smoke invocation.

From `samples-go-poc`, test and build the complete workspace:

```bash
tgo test -p=2 ./... ./codec-server/... ./grpc-proxy/...
sample_build_dir=$(mktemp -d)
while IFS= read -r sample_package; do
    if [ -n "$sample_package" ]; then
        tgo build -p=2 -o "$sample_build_dir/program" "$sample_package" || exit
    fi
done < <(tgo list -f '{{if eq .Name "main"}}{{.ImportPath}}{{end}}' ./... ./codec-server/... ./grpc-proxy/...)
```

Cloud deployment, authentication servers and telemetry collectors need their own
infrastructure. Compilation and unit tests do not prove those deployments work.

## Test inventory gate

[TEST_COVERAGE.md](TEST_COVERAGE.md) is generated from
`internal/testports/inventory.json`. The inventory keeps source fingerprints,
dispositions and native target references used by the gate. The concise document
explains its scope; it does not duplicate every test row.

```sh
tgo run ./internal/cmd/testports
tgo run ./internal/cmd/testports -write-checklist
```

For a source comparison, pass `-upstream /path/to/the-inventory-source-checkout`.
The inventory's source commit differs from the current host SDK pin, so using the
newer sibling checkout is expected to detect source drift. Do not describe a
successful internal inventory check as full v1.50.0 compatibility. A complete
current SDK field/hook/behavior audit is a separate task.

Run host SDK checks with its canonical runner and ordinary Go:
`cd ../temporal-go-sdk/internal/cmd/build && go run . check` and `go run . unit-test`.
Its integration runner supports `integration-test -dev-server`. Experimental
compiler export data is not understood by every external analysis tool.

## Generated transport and CI

Regenerate schema, bindings and static conversions together with protoc 3.21+
and a built isolate Go compiler:

```sh
GO=/absolute/path/to/golang-go/bin/go python3 internal/cmd/transportgen/generate.py
```

Review [TRANSPORT.md](TRANSPORT.md) alongside schema/operation changes. No Temporal
proto checkout is needed; generator imports come from pinned modules.

`.github/workflows/isolate-integration.yml` builds pinned Go/host-SDK/sample
sources on native Linux/macOS arm64/amd64 and runs core, SDK, compiled race, sample
and replay gates. Go core CI separately covers isolates/noisolates. Release CI
checks archive installation and relocation; that is not a runtime correctness
gate. Inspect results for the intended revision before asserting platform success.
