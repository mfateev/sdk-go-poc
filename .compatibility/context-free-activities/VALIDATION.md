# A1: activities without context parameters

The mandatory-context registration restriction is removed. The SDK validates
registered function and struct activity signatures and continues to execute
remote and local activities. No SDK, compiler or wire-protocol change is needed.

Native remote/local function references use the SDK's optional-context detection
and argument validation, adapted from v1.50.0 internal/internal_activity.go.
The local bridge decodes all parameters for context-free functions and skips
only an optional leading context. The adapted helper retains the SDK's MIT
license in internal/activityref/LICENSE. The SDK activity helpers and decoder
have no source differences from the official upstream-main reference recorded
in metadata.json.

Testsuite activity mocks already delegate to the SDK; the custom mock decoder
only handles native workflow mocks and is unchanged. SDK and native workflow
fixtures test remote/local calls, real activities and context-free mock return
callbacks. A worker plugin also registers a context-free activity through the
startup registry, including inherited and explicit plugin paths.

## Verified cases

- Zero-input and ordinary-input functions, with and without contexts.
- Multiple inputs, nil pointer inputs, and int64 precision beyond 2^53.
- Function aliases, bound-method aliases and prefixed struct registration.
- Error-only success and nonretryable failure.
- SDK-supplied context on activities declaring it.
- Native argument-count/type rejection and SDK invalid-signature rejection at worker registration.
- Context-free activity alias registration for replay.
- Live SDK/native result comparison and remote activity type/input payload comparison.
- Native history replay with cache sizes 1000/0 and GOMAXPROCS 1/2/8.

For zero arguments the SDK omits Input whereas the bridge sends empty Payloads;
the checker normalizes only that representation. Remaining input payloads are
compared exactly. Replay alone is not used as proof of input-payload parity.

The SDK public remote-activity path resolves function references to names before
argument validation, so its rejection of malformed calls differs from the
POC's pre-existing stricter reference checks. Paired valid-call comparisons do
not assert parity for malformed-call timing; the native invalid-call checks
remain regression coverage. This change does not choose new malformed-call
semantics or claim full activity or SDK compatibility.

## Commands and results

All commands exited zero; exact log hashes and source revisions are recorded
in metadata.json. The full suite skips opt-in live cases; the new live case was
run explicitly below.

```sh
../golang-go/bin/tgo test -count=1 ./example/contextfree ./internal/activityref ./worker ./workflow ./temporalbridge ./testsuite
CONTEXT_FREE_DEV_SERVER=1 ../golang-go/bin/tgo test -count=1 -timeout=10m ./example/contextfree/check
../golang-go/bin/tgo test -timeout=20m ./...
```

The original audit and later configuration/reset evidence are preserved as
historical snapshots. This closes A1's demonstrated mandatory-context gap for
the tested paths; no complete inventory refresh or strict compatibility claim
is made by this focused fix. No code was pushed or published.
