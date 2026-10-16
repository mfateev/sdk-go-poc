# Activities with optional contexts

Activities may declare `context.Context` as their first parameter or omit it,
matching Temporal sdk-go. Both remote and local activities execute on the host.
Native workflows still require a context parameter.

`workflow_test.go` runs the same successful calls through SDK and native workflows
with real activities and SDK activity mocks. It covers zero inputs, ordinary
inputs, optional contexts, aliases, bound methods, prefixed struct methods,
nil pointers, large integers, error-only returns and activity failures. Native
calls also check incorrect argument counts and types before scheduling.

Run the compiled live comparison from the repository root:

```sh
CONTEXT_FREE_DEV_SERVER=1 ../golang-go/bin/tgo test -count=1 -timeout=10m ./example/contextfree/check
```

The checker starts an embedded Temporal server and compares SDK/native results
and remote activity type/input payloads. For zero inputs, absent and empty
Payloads are treated equivalently; other payload messages are compared exactly.
It runs with worker cache sizes 1000 and 0 and replays each native history at
GOMAXPROCS 1, 2 and 8. Local activity marker replay uses the SDK replayer and does
not execute local activities again.
