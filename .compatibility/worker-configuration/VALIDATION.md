# Effective worker configuration

The SDK adds the standalone optional `worker.WorkerConfigurationProvider`
interface, with `GetConfiguration() worker.WorkerConfiguration`. It embeds no
worker interface and leaves `worker.Worker` and `worker.New` signatures intact.
The configuration has the fields of worker options, after plugin configuration,
defaulting and validation, including inherited identity, interceptors and plugins.
It is a construction-time snapshot, not runtime poller-scaling state. Slices and
the eager-reservation scalar pointer are copied at construction and on retrieval;
referenced components remain shared and must not be mutated through the snapshot.

POC New/Wrap reads that configuration before registration, and forwards the
capability. Existing custom workers without it continue to use Wrap's supplied
options (or defaults); the POC getter reports those supplied values in that case.
The existing SDK registry override continues to route startup registrations to
the POC wrapper, which delegates to the host worker once.

The reproduced P2 stale panic-policy validation is fixed: inherited and explicit
plugins selecting FailWorkflow cause both named and dynamic native registration
to reject the configuration, through direct and startup-plugin paths. Plugins
changing FailWorkflow back to BlockWorkflow allow registration. Host workflows
retain SDK behavior. Worker alias bookkeeping also uses the final plugin setting.

The public SDK test checks final plugin ordering, defaults, inherited settings,
and snapshot isolation. A doclink scanner incorrectly treated an underlying type
as a new declaration when processing `type WorkerConfiguration WorkerOptions`;
its declaration-name matching is corrected and regression-tested.

## Validation

All commands below passed; log hashes and exit codes are in logs.json.

- POC: `../golang-go/bin/tgo test -timeout=20m ./...`
- POC live native plugins: `PLUGIN_DEV_SERVER=1 ../golang-go/bin/tgo test -count=1 -timeout=10m ./example/plugins/check` (cache 1000 and 0; execution and replay).
- SDK canonical runner: `go run . check`
- SDK canonical runner: `go run . unit-test -run 'TestWorkerConfiguration|Test(Worker|Replayer)PluginRegistryOverride'` (race enabled).
- SDK: `go test ./internal/cmd/tools/doclink`
- SDK canonical runner: `go run . integration-test -dev-server -run '^TestIntegrationSuite/Test(WorkerPlugin|SimplePlugin)$'`, once normally and once with `WORKFLOW_CACHE_SIZE=0` (race enabled). Both scenario methods passed in each run.

This is focused implementation evidence, not a refreshed full compatibility
audit or a claim of complete SDK parity. The original audit and reset validation
remain historical evidence. No replayer configuration provider was added; the
separate post-plugin replayer aliasing hypothesis remains open. No code was
pushed or published.
