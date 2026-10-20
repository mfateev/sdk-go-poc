# SDK test coverage inventory

Generated from [internal/testports/inventory.json](internal/testports/inventory.json). The JSON contains source fingerprints, dispositions, reasons and native test targets used by the executable gate. It is the maintained mapping; this page summarizes its scope.

Inventory source: `b87b0a1d5dda22d68ddb97237fdc42cd2492043f` (v1.49.0). Declared runtime dependency: `go.temporal.io/sdk v1.50.0`, replaced by `../temporal-go-sdk`. The actual matching host SDK revision is pinned in [toolchain/go-source.json](toolchain/go-source.json).

The source-test inventory covers v1.49.0, while the runtime dependency is v1.50.0. It does not inventory every new or changed field, hook or test in the newer SDK. A passing inventory check does not establish full native compatibility with that dependency.

The inventory includes named unit tests and suite methods across source modules. The separate integration-test module, testdata, generated non-test helpers and build directories are excluded; optional live-server cases have explicit dispositions. Native tests replace SDK concurrency constructs with Go contexts, goroutines, channels, select and timers.

| Disposition | Meaning | Tests |
| --- | --- | ---: |
| covered | Mapped to native unit or compiled-isolate targets | 243 |
| retained | Delegated host implementation and original tests; native targets are supplemental | 857 |
| blocked | Missing behavior with an explicit recorded feature disposition | 0 |
| obsolete | Replaced facade or excluded contract; see the row's reason | 39 |
| out_of_scope | Optional modules, tooling or live-server scope outside this mapping | 332 |
| suite | Suite entry point, with methods inventoried separately | 26 |
| pending | Unreviewed entry; rejected by the gate | 0 |
| Total | | 1497 |

Run from the SDK root with the matching tgo:

```sh
tgo run ./internal/cmd/testports
tgo run ./internal/cmd/testports -write-checklist
```

The gate rejects unfinished entries, missing reasons, duplicate rows, absent source fingerprints, broken native targets, stale generated text and declared dependency drift. `-require-no-blocked` also rejects blocked rows; it is not a complete feature-parity audit.

For source comparison, add `-upstream /path/to/the-inventory-source-checkout`. That checkout must match the inventory source above; comparing the newer host SDK detects drift rather than silently extending coverage. Update/review the JSON when refreshing source scope, then regenerate this page.

See [TESTING.md](TESTING.md) for native/host runners, fixture budgets, replay and dev-server checks, and [COMPATIBILITY.md](COMPATIBILITY.md) for the supported API scope. Retained tests and signature checks do not establish exhaustive native behavior.
