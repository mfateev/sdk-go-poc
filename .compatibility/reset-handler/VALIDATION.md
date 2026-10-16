# Reset callback and plugin registry validation

The SDK task branch includes official upstream main `e43cbba8d80f064ca93e4a8ebd9cc2e6dd15d024` (v1.50.0), merged as `3a43770`. Registry routing is committed as `b5c1d2c`, reset callbacks as `d9ed586`, and compiler source selection as `10678846d0`. Nothing was pushed or published. The POC uses the sibling development SDK checkout; the proposed integration APIs are not yet in unmodified upstream.

## Resulting behavior

`WorkflowEnvironment.RegisterResetHandler(func(WorkflowResetInfo))` follows the existing signal/cancellation registration pattern. `internalbindings.WorkflowResetInfo` carries copied `NewRunID`, `BaseRunID`, and `EventID` values. Nil disables the handler, and registration replaces an earlier handler. Register before the workflow definition's Execute returns. Callbacks run during history processing, including replay, after the SDK updates its identity and reseeds held random streams, before the next workflow task starts. Callbacks may queue host state changes; workflow execution remains fenced to OnWorkflowTaskStarted.

The POC initializes identity from OriginalRunID and queues reset identities until that task boundary. Cancellation and reset in the same history batch produce a control reply with both final values. Named streams and context-aware child serialization use the tracked identity. This removes the SDK-specific GetCurrentRunID and GenerateChildWorkflowID helpers. The SDK test compares explicit IDs made from the callback identity and GenerateSequence with actual SDK-generated child command attributes before and after reset.

Plugin startup and replay registries now use the POC wrapper. Explicit plugins and plugins inherited from clients register named/dynamic native workflows through the same factory/converter/interceptor/resource configuration path. Host registration callbacks fire once. Custom worker implementations retain their own plugin wiring, as documented by Wrap. Plugin-mutated panic policy remains the earlier open behavior decision; this change does not resolve it.

The compiler supports official SDK module revisions and filesystem SDK checkouts, retaining owning-module and separate protobuf/API/telemetry source checks. It no longer requires a hardcoded fork revision. The dependency is the checked-out upstream-main-based SDK with local integration changes, not a published fork version.

## Executed checks

All positive commands exited zero; full logs and SHA-256 hashes are recorded alongside metadata.json.

| Check | Command |
| --- | --- |
| Full POC suite | `../golang-go/bin/tgo test -timeout=20m ./...` |
| Bridge, workflow, worker, testsuite and ledger | `../golang-go/bin/tgo test ./temporalbridge ./workflow ./worker ./testsuite ./internal/testports` |
| Compiled native tests, live reset/replay and plugins | `RANDOM_STREAMS_DEV_SERVER=1 PLUGIN_DEV_SERVER=1 ../golang-go/bin/tgo test -count=1 -timeout=10m ./example/random/check ./example/plugins/check ./example/child/check` |
| SDK canonical checks | From SDK internal/cmd/build: `go run . check` |
| SDK canonical race tests | Focused reset, integration binding and plugin registry tests; command below |
| Additional multiple-reset race test | From SDK root: `go test -race -count=1 ./internal -run '^TestWorkflowReset'` |
| SDK live reset regression, default cache | Canonical runner: `integration-test -dev-server -run '^TestWorkflowRandomTestSuite/TestReset'` |
| SDK live reset regression, zero cache | Same canonical runner with `WORKFLOW_CACHE_SIZE=0` |
| Compiler policy and build-script test | Source-policy unit and metadata script tests; command below |

Exact focused commands, from SDK internal/cmd/build and runtime root respectively:

```sh
go run . unit-test -run 'TestWorkflowReset|TestWorkflowIntegration|Test(Worker|Replayer)PluginRegistryOverride'
./bin/tgo test cmd/internal/isolatepolicy cmd/go -run '^TestTemporalSDKSource$|^TestScript/build_isolate_metadata$' -count=1
```

The two SDK integration runs were executed sequentially from fresh working directories with a binary built from the canonical runner. This preserves its test flags, race detector and server dynamic configuration while giving each run a fresh relative database path. The initial parallel attempts collided on the runner's fixed port/database/workflow IDs and failed with terminated executions; those logs are preserved separately. The successful fresh runs each executed both reset scenarios. SDK dev server: configured CLI `v1.8.3-server-1.32.0-162.0`; native POC checker: CLI 1.9.1 / Server 1.32.0.

Native checks cover cache sizes 1000 and 0, existing held streams and streams created after the reset point, child/Continue-As-New cases, and recorded history replay at GOMAXPROCS 1/2/8. The default full suite still skips opt-in live cases; the relevant named-stream and plugin live cases above ran explicitly. Other unrelated live features were not retested.

As a negative control, an isolated Go source overlay removed only SDK reset-callback delivery. With `GOFLAGS=-overlay=/tmp/reset-handler-negative-control/overlay.json`, the saved named-stream history replay test exited 1 on `reset-late-false-cache-0.json`: `reset or continue stream outputs changed on replay`. The production working tree was not modified. This confirms the native replay assertions detect a dropped notification.

## Compatibility scope and inventory freshness

This validates the affected behavior, not full sdk-go v1.50.0 parity. Fresh AST inventories contain 7,771 baseline and 2,318 candidate items, with zero unresolved parser/alias issues. The compressed matrix preserves the earlier audit assessments, carries changed/new declarations as pending, and retains removed rows for review. Registry-routing rows move to unverified rather than being marked fully compatible from a few scenarios. The literal configuration supplement and external configuration expansion still need review for the new baseline. No exclusions were approved.

The strict gate continues to fail on unverified/pending/unsupported contracts and missing completeness reviews. Its failure is preserved in logs/strict-gate.log. The original v1.49.0 audit remains unchanged. The source-test migration ledger also remains explicitly a v1.49.0 assertion baseline; its runtime dependency metadata now identifies v1.50.0 and the local SDK replacement. It does not claim to enumerate every new v1.50.0 test. In particular, the newly introduced child VersioningOverride has a populated getter drift check here, but native command serialization/behavior is not certified by that check.

To inspect the fresh structural gate, decompress baseline.json.gz, candidate.json.gz and matrix.json.gz into a temporary directory, then run the skill's `gate.py check` with those three files. Expected exit is 1. `refresh.py` records how this review seed was derived and refuses to overwrite its outputs. Source revisions and working-tree state at validation are recorded in metadata and the inventories; the subsequent POC commit only records this tested code and hidden evidence artifacts.
