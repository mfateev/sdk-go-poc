# Reproduce the audit

Run from the POC root at the revision in `metadata.json`, with the recorded runtime/toolchain. Keep this audit immutable: send new logs and inventories to a new audit directory. The commands below show the original inputs and output locations; do not overwrite reviewed evidence on a subsequent run.

## Exact sources

```sh
../golang-go/bin/tgo list -m -json go.temporal.io/sdk
git -C ../temporal-go-sdk worktree add --detach /tmp/sdk-go-official-audit-v1.49.0 v1.49.0
git -C ../temporal-go-sdk worktree add --detach /tmp/sdk-go-host-audit-b87b0a1 b87b0a1d5dda22d68ddb97237fdc42cd2492043f
```

Worktrees already exist after this audit. Inspect/reuse them if clean; do not reset shared worktrees. Verify the official tag resolves to `58115b581391037b93451f50ac3628611ea0493b`.

## Inventories

The checked-in `stdlib-sources` files are exact non-test Go source copies from `../golang-go/src/io`, `src/math/rand/v2`, and `src/isolate` at the recorded runtime revision. Their minimal `go.mod` files use module paths `io`, `math/rand/v2`, and `isolate`, respectively, so the source-only scanner resolves standard-library names. They are not replacement modules used by the POC build. `stdlib-sources/manifest.json` records original file paths; compare their bytes before regenerating.

Original final AST commands:

```sh
../golang-go/bin/tgo run /Users/maxim/.codex/skills/sdk-go-compatibility/scripts/inventory.go \
  -root /tmp/sdk-go-official-audit-v1.49.0 \
  -dependency .compatibility/stdlib-sources/io \
  -dependency /Users/maxim/go/pkg/mod/github.com/stretchr/testify@v1.10.0 \
  -dependency /Users/maxim/go/pkg/mod/go.opentelemetry.io/otel/sdk@v1.44.0 \
  -dependency /Users/maxim/go/pkg/mod/go.opentelemetry.io/otel/trace@v1.44.0 \
  -out .compatibility/baseline-resolved.json

../golang-go/bin/tgo run /Users/maxim/.codex/skills/sdk-go-compatibility/scripts/inventory.go \
  -root . \
  -dependency /Users/maxim/go/pkg/mod/github.com/mfateev/temporal-go-sdk@v1.49.0-isolates.1.0.20261012175541-b87b0a1d5dda \
  -dependency .compatibility/stdlib-sources/io \
  -dependency .compatibility/stdlib-sources/math/rand/v2 \
  -dependency .compatibility/stdlib-sources/isolate \
  -out .compatibility/candidate-final.json

../golang-go/bin/tgo run /Users/maxim/.codex/skills/sdk-go-compatibility/scripts/inventory.go \
  -root /Users/maxim/go/pkg/mod/github.com/mfateev/temporal-go-sdk@v1.49.0-isolates.1.0.20261012175541-b87b0a1d5dda \
  -dependency .compatibility/stdlib-sources/io \
  -dependency /Users/maxim/go/pkg/mod/github.com/stretchr/testify@v1.11.1 \
  -out .compatibility/host-inventory.json
```

`assemble.py` adds the reproducible literal configuration supplement, initializes one row per baseline ID, maps declarations, records logs, and annotates observed gaps. Its input paths describe this run. It refuses to replace `matrix.json`; later audits must preserve old evidence, merge new IDs as pending, and review changed/removed IDs and identities. It is an audit assembly script, not a semantic parity checker.

## Tests and discrepancy probes

```sh
../golang-go/bin/tgo test -json ./...
../golang-go/bin/tgo run ./internal/cmd/testports -upstream /tmp/sdk-go-host-audit-b87b0a1

../golang-go/bin/tgo test -json ./.compatibility/probes -run TestPluginRegistrationControl
../golang-go/bin/tgo test -json ./.compatibility/probes -run TestPluginStartNativeRegistration
../golang-go/bin/tgo test -json ./.compatibility/probes -run TestPluginReplayNativeRegistration
../golang-go/bin/tgo test -json ./.compatibility/probes -run TestPluginPanicPolicyMutation
../golang-go/bin/tgo test -json ./.compatibility/probes -run TestActivityContextOptionalBaselineComparison

../golang-go/bin/tgo build -o .compatibility/dynamic-check ./example/dynamic/check
.compatibility/dynamic-check -live -history-dir .compatibility/live-histories
```

The three plugin discrepancy tests intentionally fail on the audited implementation. Execute them separately because the first unrecovered registration panic ends the test process. The direct-registration control and activity discrepancy test pass. The hidden probes are outside the normal `./...` package walk and do not alter the product's test suite.

From `/tmp/sdk-go-official-audit-v1.49.0/internal/cmd/build`:

```sh
go run . unit-test -run 'Test(GetActivityOptions|GetLocalActivityOptions|GetChildWorkflowOptions|PayloadLimitOptionsToLimits|.*Plugin.*)$'
```

The canonical runner executes nested modules with race enabled and preserves JSON logs in `.build/test-logs/unit-test.json`. Its aggregate JSON was copied to `logs/official-focused.jsonl`. Modules with no matching tests are not behavioral evidence.

From the official root:

```sh
/Users/maxim/workarea/workspaces/projects/tasks/modify-go-runtime-for-isolates/golang-go/bin/tgo test -json -run '^$' ./...
```

This last command only type-checks/builds the root module. It deliberately executes no tests.

## Strict gate

```sh
python3 /Users/maxim/.codex/skills/sdk-go-compatibility/scripts/gate.py check \
  --inventory .compatibility/baseline.json \
  --candidate-inventory .compatibility/candidate.json \
  --matrix .compatibility/matrix.json
```

Expected exit: **1**. The gate rejects unresolved rows, missing completeness/external reviews, and failed/skipped regression evidence. No exclusions were approved and `--allow-exclusions` was not used. Recheck with newly generated inventories when auditing later source; passing frozen inputs would only validate their recorded structure.
