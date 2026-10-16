# Temporal Go SDK with native Go workflows

Write Temporal workflows using Go's `context.Context`, goroutines, channels,
`select`, and timers. Activities, clients, and worker registration follow the
familiar Temporal Go SDK APIs.

This SDK is a proof of concept. APIs and compatibility can change between
versions.

## Setup

### Install tgo

Use `tgo` to build, test, and run applications that use this SDK. The installer
supports macOS and Linux on arm64 and amd64 and does not require an existing
Go installation.

The download commands require a published
[tgo release](https://github.com/mfateev/golang-go/releases). Until the first
release is available, follow the
[source setup guide](https://github.com/mfateev/samples-go-poc/blob/task/modify-go-runtime-for-isolates/README.md#1-install-tgo-on-linux-or-macos).

```sh
curl -fsSL https://github.com/mfateev/golang-go/releases/latest/download/install.sh -o install-tgo.sh
sh install-tgo.sh
export PATH="$HOME/.local/bin:$PATH"
tgo version
```

Add the PATH line to your shell startup file so new terminals can find `tgo`.
For a specific version, a custom installation directory, or Homebrew, see the
[installation guide](https://github.com/mfateev/golang-go/blob/task/modify-go-runtime-for-isolates/misc/tgo/README.md).

### Get the SDK

```sh
git clone --branch task/modify-go-runtime-for-isolates https://github.com/mfateev/sdk-go-poc.git
cd sdk-go-poc
```

This development branch uses a sibling `../temporal-go-sdk` checkout synchronized
with official upstream main and containing the bridge integration changes.
Unmodified upstream does not yet contain those additions. The repository's local
module replacement selects that checkout.

Choose a tgo release compatible with the SDK version you use. For changes on
the development branch that are newer than the latest release, use the source
setup guide above.

### Run your first workflow

Install the [Temporal CLI](https://github.com/temporalio/cli) and start a local
server in another terminal:

```sh
temporal server start-dev --headless
```

From the SDK directory, build and start the example worker:

```sh
mkdir -p bin
tgo build -o bin/example-worker ./example/worker
./bin/example-worker
```

In a third terminal, start a workflow:

```sh
temporal workflow execute \
  --workflow-id tgo-hello \
  --type IsolateTypedEcho \
  --task-queue isolate-poc \
  --input '{"Name":"world"}' \
  --input '"!"'
```

The result contains `hello world!`. Use a new workflow ID when trying another
run. To connect the example worker to a different server, set
`TEMPORAL_ADDRESS` before starting it.

For more examples, follow the
[samples setup guide](https://github.com/mfateev/samples-go-poc/blob/task/modify-go-runtime-for-isolates/README.md).

### Add the SDK to your application

From your application's Go module, use these dependency versions together:

```sh
tgo mod edit -replace=go.temporal.io/sdk=/absolute/path/to/temporal-go-sdk
tgo get github.com/mfateev/sdk-go-poc@task/modify-go-runtime-for-isolates
```

Use this SDK's packages for workflows, workers, and workflow tests:

```go
import (
    "github.com/mfateev/sdk-go-poc/workflow"
    "github.com/mfateev/sdk-go-poc/worker"
    "github.com/mfateev/sdk-go-poc/testsuite"
)
```

Choose the imports your application needs. Continue using `go.temporal.io/sdk`
packages for clients, activities, and common Temporal options and errors. After
updating imports, run `tgo mod tidy`.

Build workers with `tgo build` and run tests with `tgo test`. You can continue
using standard `go` for other projects and development tools.

## Differences from sdk-go

Compared with the standard [Temporal Go SDK](https://github.com/temporalio/sdk-go),
this SDK lets workflow code use ordinary Go concurrency and time APIs.

| Area | Standard Temporal Go SDK | This SDK |
| --- | --- | --- |
| Build and test | `go build`, `go test` | `tgo build`, `tgo test` |
| Workflow context | `workflow.Context` | `context.Context` |
| Workflow declaration | Register a workflow function | Add `//temporal:workflow` above the function, then register it |
| Goroutines | `workflow.Go` | `go` |
| Channels | `workflow.NewChannel` and SDK channel methods | `make(chan T)`, sends, and receives |
| Selecting events | `workflow.NewSelector` | Go's `select` |
| Time | `workflow.Now`, `workflow.Sleep`, `workflow.NewTimer` | `time.Now`, `time.Sleep`, `time.NewTimer`; `workflow.Sleep` is also available |
| Cancellation | SDK workflow contexts | Standard `context` cancellation and `ctx.Done()` |
| Signals | SDK signal channels | `workflow.GetSignalChannel[T]` returns a Go receive channel |
| Workflow tests | `go.temporal.io/sdk/testsuite` | `github.com/mfateev/sdk-go-poc/testsuite`, using `tgo test` |

### What stays familiar

Keep using activities for external work, activity options and futures, child
workflows, signals, queries, updates, retries, and Continue-As-New. Worker
registration still uses `RegisterWorkflow` and `RegisterActivity`.

Use this SDK's `worker` package to register native Go workflows. A worker can
also register existing standard SDK workflows, which keep their original
`workflow.Context` signatures. When starting a native workflow through the
standard Temporal client, pass its registered name as a string.

### Rules for workflow code

- Mark each workflow entry function with `//temporal:workflow`. Use a top-level
  function; workflow entries cannot be methods, generic functions, or variadic
  functions.
- Use `context.Context` as the first argument when the workflow needs a context.
- Keep file access, network calls, environment reads, and other external work
  in activities. Pass configuration as workflow input.
- Each workflow execution has its own package variables. Use activities,
  signals, or workflow inputs to exchange data with other executions or worker
  code.
- Use `tgo build` to create worker executables. Use `tgo run` for clients and
  starters; workflow workers need the build step.
- Continue following Temporal's workflow versioning practices when changing
  existing workflows. Migrating from the standard SDK requires review and
  replay checks; changing imports alone is not sufficient.

### Deterministic randomness and security

Workflow code can use `math/rand`, `math/rand/v2`, supported `crypto/rand`
functions, and UUID v4 helpers. Values repeat during replay. They are
predictable and must not be used for passwords, encryption keys, or other
secrets; generate secrets in activities.

For more usage examples, browse [the examples](example/). For advanced
configuration and detailed behavior, see [the SDK reference](SDK_REFERENCE.md)
and [interceptors and observability](INTERCEPTORS.md).
