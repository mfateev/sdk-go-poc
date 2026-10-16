---
name: sdk-go-compatibility
description: Audit or maintain sdk-go-poc feature compatibility with Temporal sdk-go, including every options field, worker configurations, plugin hooks, and native isolate behavior. Use for compatibility assessments, regression gates, or closing identified parity gaps.
---

# Go SDK compatibility

Establish compatibility against an identified sdk-go revision through a complete inventory and observable behavior. API shape, retained upstream tests, and getter round trips are supporting evidence; none establishes native workflow behavior by itself.

Default to an audit when asked to assess compatibility. Fix SDK behavior only when requested. Creating this skill does not authorize publishing, changing dependency pins, or implementing every missing feature.

## Decisions belong to the user

Resolve a discrepancy mechanically only when pinned source, documented contracts, or executable evidence determines the answer without choosing intended behavior. If evidence conflicts, contracts are ambiguous, or reconciliation requires changing semantics, scope, defaults, plugin policy, API mappings, or feature support, ask the user. Do not choose the most convenient interpretation, infer approval from silence, or classify the discrepancy as excluded/obsolete/compatible yourself.

Record the conflicting evidence, affected field or hook, observable consequences, and concrete alternatives in a `needs_decision` matrix entry. Ask a concise question with that context. Keep dependent implementation and compatibility claims blocked until the answer arrives; continue independent inventory and validation. Apply existing explicit decisions only to the scope they actually resolve. An existing ledger classification is evidence to inspect, not permission to decide a newly discovered discrepancy.

## Establish the comparison

- Locate `sdk-go-poc`, its runtime/compiler checkout, and the sdk-go source. Read applicable `AGENTS.md` files and inspect worktree status before edits.
- Read the POC's `go.mod`, `go.sum`, replacements, workspaces, and build/test instructions. Resolve the actual host SDK source with the selected toolchain's `list -m -json go.temporal.io/sdk`; a sibling checkout can be newer than the dependency.
- Record three distinct versions: the official sdk-go feature baseline, the fork actually used by the POC, and the POC revision. Include local changes, runtime/compiler revision, toolchain, nested contrib modules, server version/capabilities, OS/architecture, and build tags. Never silently use a fork's HEAD as the official baseline.
- Default to the official version corresponding to the declared SDK dependency. Use an already available exact snapshot or an isolated temporary worktree; if unavailable, report the provisional fork-based comparison and the missing official-baseline check. Do not upgrade dependencies to make the audit pass.

Read [the repository guide](references/repository-guide.md) for the known integration points, existing test ledger, and commands. These are discovery hints, not frozen feature decisions.

## Inventory before sampling

Use [the audit contract](references/audit-contract.md) for required evidence and gates. Generate a fresh inventory with `scripts/inventory.go` for each source tree, including nested modules and any source roots needed to resolve aliases:

```sh
go run /absolute/path/to/sdk-go-compatibility/scripts/inventory.go \
  -root /absolute/path/to/sdk-go -out /tmp/sdk-go-inventory.json
go run /absolute/path/to/sdk-go-compatibility/scripts/inventory.go \
  -root /absolute/path/to/sdk-go-poc \
  -dependency /absolute/path/to/resolved-host-sdk \
  -out /tmp/sdk-go-poc-inventory.json
```

The helper parses source without compiling workflows. It inventories exported declarations, resolves aliases using supplied module trees, expands struct fields and embedded interfaces, and records all source build variants. It is an inventory seed, not a type checker or compatibility proof. Review its `issues`, supplement external configuration types using their pinned module sources, and type-check supported build configurations with the proper toolchain. Never discard unresolved inventory to obtain a passing result.

Maintain one matrix entry for every baseline inventory item, including each field and hook. Explicitly map upstream APIs to adapted native APIs, delegated host APIs, or gaps. Inventory additional runtime flags, environment variables, functional options, worker setters, plugin registrations, and documented configuration paths as supplemental items; source declarations alone do not enumerate their behavior. Keep unsupported and deprecated baseline items visible.

Initialize a review matrix with:

```sh
python3 /absolute/path/to/sdk-go-compatibility/scripts/gate.py init \
  --inventory /tmp/sdk-go-inventory.json \
  --candidate-inventory /tmp/sdk-go-poc-inventory.json \
  --matrix /tmp/compatibility-matrix.json
```

This creates pending entries without overwriting an existing file. On later runs, preserve reviewed evidence, merge newly discovered IDs as pending, and explicitly review removed or changed IDs. The gate rejects inventory drift and stale fingerprints; reinitializing must not erase the audit trail.

## Validate behavior

For every options/configuration field, trace caller input through validation, defaulting, plugins, host SDK, bridge encoding, isolate state, operation, and observable result. Test default/zero, explicit valid values, invalid values where meaningful, omission versus explicit zero/nil, and relevant interactions. Cover nested members and callbacks individually. Match documented error type, timing, and panic behavior where applicable. Record why a case does not apply.

Use shared scenario definitions or paired fixtures against the official SDK and the native POC, with explicit adapters for native context/concurrency APIs. Compare semantic command attributes, payloads, callbacks, errors, and outcomes; normalize only documented incidental IDs/timestamps. Use existing upstream tests as assertions to preserve, not a numeric proxy for feature coverage.

Read the worker and plugin sections of [the audit contract](references/audit-contract.md) before testing those areas. In particular, registrations made by plugin callbacks must be tested with marked native workflows: invoking a registry on the underlying worker can bypass the POC wrapper. Treat this as an audit hypothesis until demonstrated by a test.

Exercise changed native paths in compiled isolates, live server tests, eviction/replay, cache enabled/disabled, and relevant concurrency/ownership checks. Reuse established checkers and history fixtures. Verify command attributes and payloads separately where the replayer does not compare them. Port tests through native equivalents without reinstating deliberately replaced concurrency APIs or compatibility shims for earlier POC versions.

## Report and gate

- Keep an evidence matrix, a concise gap report with severity and reproductions, exact test commands/results, and baseline metadata. Place durable audit artifacts in the repository's established location when running an audit; temporary outputs are sufficient when only demonstrating the skill.
- Separate inventory completeness, behavioral compatibility, and evidence freshness. Report counts by options/worker/plugins and distinguish native execution from delegated host behavior.
- Use `compatible`, `adapted`, or `delegated` only with the evidence required by the audit contract. `unsupported`, `unverified`, `pending`, and `needs_decision` block strict compatibility. `excluded` needs an explicit user-approved scope decision and still prevents an unqualified full-compatibility claim.
- Run `scripts/gate.py check --inventory ... --candidate-inventory ... --matrix ...` for the strict evidence gate. `--allow-exclusions` is only for an explicitly scoped claim; report exclusions alongside the result. The gate validates evidence records, not whether their claims are true. Review the assertions and test logs.
- CI should regenerate the inventory, run the relevant scenarios, generate fresh run evidence, and invoke the gate. New fields, changed signatures/docs, missing rows, unresolved aliases, failures/skips, stale source snapshots, and unsupported features must not pass unnoticed. Reuse repository CI; do not schedule recurring jobs unless asked.

Finish with what was verified, remaining gaps and exclusions, and commands to reproduce. If only this process was requested, state that the skill was validated and that SDK compatibility itself has not yet been established.
