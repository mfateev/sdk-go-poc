# Temporal workflows with native Go isolates

Write Temporal workflows using standard `context.Context`, goroutines, channels,
select, synchronization and timers. The modified Go runtime makes supported
operations replay deterministic and enforces private state/ownership. Activities,
clients and worker services run in the ordinary host Temporal SDK.

This is a statically linked proof of concept for reviewed code. APIs can change;
there is one current implementation and no process security sandbox or hard-kill
guarantee for arbitrary non-yielding code. [Supported scope](COMPATIBILITY.md)
records the concrete API limits.

## Install tgo

`tgo` is this SDK's build driver paired with the matching Go fork. It discovers
marked functions and supplies the generic compiler manifest. Stock Go cannot
build native workflow executables. Source setup needs Git, Python 3.11+, a C
compiler and bootstrap Go 1.26.7 or newer.

Create sibling SDK, Go, host SDK and sample checkouts. From their parent directory:

```sh
git clone --branch task/modify-go-runtime-for-isolates https://github.com/mfateev/sdk-go-poc.git
git clone --no-checkout https://github.com/mfateev/golang-go.git
git clone --no-checkout https://github.com/mfateev/temporal-go-sdk.git
git clone --no-checkout https://github.com/mfateev/samples-go-poc.git
python3 - <<'PYSETUP'
import json, subprocess
from pathlib import Path
pin = json.loads(Path('sdk-go-poc/toolchain/go-source.json').read_text())
for repo, key in [('golang-go', 'revision'), ('temporal-go-sdk', 'temporal_sdk_revision'), ('samples-go-poc', 'samples_revision')]:
    subprocess.run(['git', '-C', repo, 'fetch', 'origin', pin[key]], check=True)
    subprocess.run(['git', '-C', repo, 'checkout', '--detach', pin[key]], check=True)
PYSETUP
cd sdk-go-poc
export GOROOT_BOOTSTRAP=/absolute/path/to/bootstrap/go
python3 toolchain/build.py --source ../golang-go --local
export PATH="$(cd ../golang-go/bin && pwd):$PATH"
tgo version
```

The source pins in [toolchain/go-source.json](toolchain/go-source.json) select
matching compiler/runtime, host SDK and samples. The host SDK checkout adds
binding version 2 and is selected by this repository's module replacement;
unmodified upstream is insufficient for this configuration. For a pinned
snapshot instead of in-place development, see [toolchain/README.md](toolchain/README.md).

For a published precompiled release, the installer supports Linux/macOS
arm64/amd64 without an existing Go installation:

```sh
curl -fsSL https://github.com/mfateev/sdk-go-poc/releases/latest/download/install.sh -o install-tgo.sh
sh install-tgo.sh
export PATH="$HOME/.local/bin:$PATH"
tgo version
```

That command requires the corresponding published release assets. Source setup
works independently of release publication. [Distribution](toolchain/DISTRIBUTION.md)
covers exact tags, offline install, packaging and the conditional Homebrew tap.

## A workflow

In a workflow package:

```go
package jobs

import (
    "context"
    "time"

    "github.com/mfateev/sdk-go-poc/workflow"
)

//temporal:workflow
func Hello(ctx context.Context, name string) (string, error) {
    ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
        StartToCloseTimeout: time.Minute,
    })
    var greeting string
    err := workflow.ExecuteActivity(ctx, "Greet", name).Get(ctx, &greeting)
    return greeting, err
}
```

Use the POC `worker.New` and register `jobs.Hello` plus host activity `Greet`.
Clients use `go.temporal.io/sdk/client` and start native workflows by registered
string name. Native worker entries require context.Context first; it is injected,
never serialized as a workflow input. Activities may include a context first or
omit it. Unmarked SDK workflows keep workflow.Context and their normal path.

`tgo build`, `tgo test` and `tgo run` discover marked entries. Run builds a
marked temporary executable. `tgo install` does not generate entries. Concrete
ordinary arguments and no returns/error/result+error are supported; method,
generic and variadic workflow entries are rejected. Tests discover marked
`_test.go` entries too; marked coverage mode is unsupported.

## Run the example

Install Temporal CLI separately. In one terminal start a local server:

```sh
temporal server start-dev --headless
```

From this SDK checkout, in another terminal:

```sh
mkdir -p bin
tgo build -o bin/example-worker ./example/worker
./bin/example-worker
```

Start an execution from a third terminal:

```sh
temporal workflow execute --workflow-id tgo-hello --type IsolateTypedEcho \
  --task-queue isolate-poc --input '{"Name":"world"}' --input '"!"'
```

The result contains `hello world!`. Choose a fresh workflow ID for another run.
`TEMPORAL_ADDRESS` changes the example worker's server address.

## Use from another module

Keep a matching host SDK checkout and replace its module in the application:

```sh
tgo mod edit -replace=go.temporal.io/sdk=/absolute/path/to/temporal-go-sdk
tgo get github.com/mfateev/sdk-go-poc@task/modify-go-runtime-for-isolates
tgo mod tidy
```

Import this module's `workflow`, `worker`, `testsuite`, `interceptor` and native
contrib packages as needed. Use ordinary Temporal SDK packages for clients,
activities, shared options, converters and errors. In the samples workspace,
`go.work` selects the sibling SDK and host SDK directly.

## Execution rules

| Concern | Native POC |
| --- | --- |
| Concurrency | `go`, native channels/select and sync primitives |
| Time | `time.Now`, Sleep/NewTimer/After/AfterFunc and context deadlines use history time/durable waits |
| Signals | `workflow.GetSignalChannel[T]` returns a native receive channel |
| Future selection | `Future.Get(ctx, &value)` or `ToChannel()` |
| State | Fresh selected package globals and owned allocation per execution |
| External effects | Host activities or explicit durable SDK operations |
| Queries/validators | Read-only workflow state plus writable scratch allocations |
| Serialization | Local default/custom converter; host codecs; private Google protobuf |
| Testing | Native `testsuite.TestWorkflowEnvironment` through `tgo test` |

Do not share mutable application objects with host/peers. Pass configuration as
input/copied factory bytes; environment/files/network belong on the host.
Ownership/effect/resource faults permanently revoke an instance and fail its
Workflow Task. Ordinary returned application errors remain workflow results.
Timers suppress stale local delivery on Stop/Reset but do not cancel an already
emitted host timer. Supported map iteration is canonical; unsupported key kinds
reject. Random output is replay seeded and predictable, including crypto-named
APIs: generate secrets in host code.

## Documentation

- [SDK reference](SDK_REFERENCE.md): exact APIs, cancellation, options and examples.
- [Implementation guide](IMPLEMENTATION.md): construction order, graph policy,
  owner boundaries, task phases, handlers and host SDK bindings.
- [Transport](TRANSPORT.md): operation numbers, startup header and full binary schema.
- [Interceptors](INTERCEPTORS.md): private chains, headers, metrics and tracing.
- [Compatibility scope](COMPATIBILITY.md): implemented behavior and explicit limits.
- [Testing](TESTING.md): package/race, compiled fixtures, replay and live checks.
- [Go core design](https://github.com/mfateev/golang-go/blob/task/modify-go-runtime-for-isolates/doc/isolates/README.md):
  compiler/runtime reimplementation, ownership, determinism and lifecycle.
- [Samples](https://github.com/mfateev/samples-go-poc/blob/task/modify-go-runtime-for-isolates/README.md):
  complete workspace setup and runnable applications.
