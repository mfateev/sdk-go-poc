package testports

import (
	"fmt"
	"strings"
)

// RenderChecklist describes the executable inventory without duplicating its rows.
func RenderChecklist(m Manifest) string {
	var out strings.Builder
	out.WriteString("# SDK test coverage inventory\n\n")
	out.WriteString("Generated from [internal/testports/inventory.json](internal/testports/inventory.json). The JSON contains source fingerprints, dispositions, reasons and native test targets used by the executable gate. It is the maintained mapping; this page summarizes its scope.\n\n")
	fmt.Fprintf(&out, "Inventory source: `%s` (%s). Declared runtime dependency: `go.temporal.io/sdk %s`, replaced by `%s`. The actual matching host SDK revision is pinned in [toolchain/go-source.json](toolchain/go-source.json).\n\n", m.SourceCommit, m.SourceSDKVersion, m.SDKVersion, m.SDKReplacement)
	if m.SourceSDKVersion != "" && m.SourceSDKVersion != m.SDKVersion {
		fmt.Fprintf(&out, "The source-test inventory covers %s, while the runtime dependency is %s. It does not inventory every new or changed field, hook or test in the newer SDK. A passing inventory check does not establish full native compatibility with that dependency.\n\n", m.SourceSDKVersion, m.SDKVersion)
	}
	out.WriteString("The inventory includes named unit tests and suite methods across source modules. The separate integration-test module, testdata, generated non-test helpers and build directories are excluded; optional live-server cases have explicit dispositions. Native tests replace SDK concurrency constructs with Go contexts, goroutines, channels, select and timers.\n\n")
	out.WriteString("| Disposition | Meaning | Tests |\n| --- | --- | ---: |\n")
	counts := map[string]int{}
	for _, entry := range m.Tests {
		counts[entry.Status]++
	}
	for _, row := range []struct{ status, meaning string }{
		{"covered", "Mapped to native unit or compiled-isolate targets"},
		{"retained", "Delegated host implementation and original tests; native targets are supplemental"},
		{"blocked", "Missing behavior with an explicit recorded feature disposition"},
		{"obsolete", "Replaced facade or excluded contract; see the row's reason"},
		{"out_of_scope", "Optional modules, tooling or live-server scope outside this mapping"},
		{"suite", "Suite entry point, with methods inventoried separately"},
		{"pending", "Unreviewed entry; rejected by the gate"},
	} {
		fmt.Fprintf(&out, "| %s | %s | %d |\n", row.status, row.meaning, counts[row.status])
	}
	fmt.Fprintf(&out, "| Total | | %d |\n\n", len(m.Tests))
	out.WriteString("Run from the SDK root with the matching tgo:\n\n```sh\ntgo run ./internal/cmd/testports\ntgo run ./internal/cmd/testports -write-checklist\n```\n\nThe gate rejects unfinished entries, missing reasons, duplicate rows, absent source fingerprints, broken native targets, stale generated text and declared dependency drift. `-require-no-blocked` also rejects blocked rows; it is not a complete feature-parity audit.\n\nFor source comparison, add `-upstream /path/to/the-inventory-source-checkout`. That checkout must match the inventory source above; comparing the newer host SDK detects drift rather than silently extending coverage. Update/review the JSON when refreshing source scope, then regenerate this page.\n\nSee [TESTING.md](TESTING.md) for native/host runners, fixture budgets, replay and dev-server checks, and [COMPATIBILITY.md](COMPATIBILITY.md) for the supported API scope. Retained tests and signature checks do not establish exhaustive native behavior.\n")
	return out.String()
}
