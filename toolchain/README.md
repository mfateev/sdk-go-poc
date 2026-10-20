# SDK-owned isolate toolchain

`cmd/tgo` owns workflow discovery, typed entry adapters, package selection and
external dependency validation. The Go fork accepts an explicit generic build
manifest. It retains descriptor emission, state factories, instrumentation,
cache identity and native scheduling enforcement.

Build the driver with an ordinary Go 1.26.7 or newer toolchain:

```sh
go build -o bin/tgo ./cmd/tgo
TGO_GOROOT=/absolute/path/to/golang-go bin/tgo test ./...
```

`TGO_GOROOT` is explicit local checkout support. A distribution places the
SDK driver beside its bundled `bin/go`; caller `GOROOT` cannot redirect it.
`go-source.json` records the matching compiler/runtime source revision.

Protobuf and OpenTelemetry policy lives in this SDK. Protobuf packages and
workflow-reachable generated messages use private instance state; host-only SDK
service descriptors are excluded from private startup. Query services rebuild
selected libraries in scratch state. Generated message methods and registry
bindings keep their original bodies, with no host mediation or process registry.

The generic Go manifest supports audited packages and physical source files for
unsafe implementation code while retaining ownership and forbidden-effect
checks. The SDK audits the pinned protobuf implementation, its generated files,
and `protobufcopy`; ordinary application files get no generated-file audit.

Small dependency overlays pin protobuf's ordering seed/functions, use default
protobuf environment settings during private execution, snapshot captured values
before read-only `proto.Clone`, and replace OpenTelemetry zero-ID sentinel
comparisons with literal values. Host environment settings and ordinary host
Clone behavior remain unchanged. The driver mirrors cached modules temporarily;
it never edits the module cache or checked-in generated files. It applies no
application-generated message rewrites. Adapted builds use `-trimpath` and
preserve workspace modules/replacements in a temporary `go.work`; nested builds
restore the original workspace before selecting dependencies.

Structured transport has a shared generated schema at
`internal/transportpb/transport.proto` and uses payload, header and failure
submessages. Scalar identifiers use fixed-width
binary framing; timer waits retain the runtime's decimal-nanosecond contract.
The public typed host-function/construction/waiter APIs have been removed.

Regenerate the schema, protobuf types and static SDK conversions with a built
isolate Go toolchain and `protoc` (3.21 or later) on PATH:

```sh
GO=/absolute/path/to/golang-go/bin/go python3 internal/cmd/transportgen/generate.py
```

The generator inventories the shared SDK envelope/options types, builds the
pinned `protoc-gen-go`, and obtains imported descriptor definitions from the
pinned Go modules. No separate Temporal proto checkout is needed. Review the
schema and regenerate both sides together when changing an operation.

The driver preserves selected Go source/build constraints, ordinary command
forwarding, internal/external test variants and dependency cache reuse. The
`internal/toolchain` tests cover directive diagnostics, graph flags, package
identities and negative dependency provenance. Set an absolute `TGO_GOROOT`
when running those integration tests with an ordinary Go command.

Build the pinned source and driver together:

```sh
GOROOT_BOOTSTRAP=/path/to/go python3 toolchain/build.py --output /path/to/new-toolchain
```

The pin is a full Go commit. `--source /path/to/git-checkout` supplies that commit
offline; no fetch occurs in this mode. The default fetches it from the recorded
repository. `--source PATH --local` explicitly builds current local files in
place. It records `local_source: true` in `pkg/TGO_BUILD.json` and is for development.
Pinned snapshots record both source revisions. Distribution packaging uses only
tracked SDK sources and pinned Go sources.

The driver places `bin/tgo` beside conventional `bin/go`, forwards ordinary
commands and selects the isolate experiment before graph loading. Explicit
`GOEXPERIMENT=noisolates` is rejected for tgo build/test/run. Use the bundled
`go` directly for disabled conformance checks. Discovery honors source overlays,
build tags and test variants, including paths containing spaces.

[Distribution and installation](DISTRIBUTION.md) describes archive building,
checksums, offline installation, relocation and Homebrew packaging.
