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
