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

The core temporarily retains dependency-source checks while its old metadata
rewrites exist. The driver checks those versions as integration policy too.
Neither the build manifest nor generated adapters grant metadata privileges.

The driver preserves selected Go source/build constraints, ordinary command
forwarding, internal/external test variants and dependency cache reuse. The
`internal/toolchain` tests cover directive diagnostics, graph flags, package
identities and negative dependency provenance. Set an absolute `TGO_GOROOT`
when running those integration tests with an ordinary Go command.
