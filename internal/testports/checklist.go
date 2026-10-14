package testports

import (
	"fmt"
	"sort"
	"strings"
)

// RenderChecklist keeps the readable list and the audited inventory in sync.
func RenderChecklist(m Manifest) string {
	var out strings.Builder
	out.WriteString("# Temporal SDK unit-test porting checklist\n\n")
	fmt.Fprintf(&out, "Source checkout: `%s`. Runtime dependency: `go.temporal.io/sdk %s`, replaced by `%s`.\n\n", m.SourceCommit, m.SDKVersion, m.SDKReplacement)
	out.WriteString("This list inventories every named unit test and suite test method in the source checkout, including nested contrib modules. The separate `test/` integration module, testdata, generated non-test helpers, and build directories are excluded. Live dev-server scenarios found in contrib test files are listed and marked out of scope.\n\n")
	out.WriteString("`internal/testports/inventory.json` is the source of truth. This document is generated from it. Every source test has a disposition and a reason; native coverage entries link to runnable tests. A consolidated native test may cover several original cases. Native goroutines, channels, context, select, timers and sync.WaitGroup replace the old workflow concurrency facade, so those ports assert native Go semantics.\n\n")
	out.WriteString("Statuses:\n\n- **covered**: assertions are mapped to new SDK unit tests or compiled isolate checks.\n- **retained**: implementation and original tests remain in the delegated host SDK; listed native tests are supplemental boundary coverage.\n- **blocked**: a required feature is missing, with a recorded user question or deferral. These entries are not counted as completed ports.\n- **obsolete**: the removed facade or an explicitly excluded feature no longer has the original contract; replacement coverage is linked where applicable.\n- **out_of_scope**: unshipped optional modules, build/release tools, or live server integration scenarios.\n- **suite**: suite entry point whose individual methods are listed separately.\n\n")
	counts := map[string]int{}
	for _, entry := range m.Tests {
		counts[entry.Status]++
	}
	out.WriteString("| Disposition | Source tests |\n| --- | ---: |\n")
	for _, status := range []string{"covered", "retained", "blocked", "obsolete", "out_of_scope", "suite", "pending"} {
		fmt.Fprintf(&out, "| %s | %d |\n", status, counts[status])
	}
	fmt.Fprintf(&out, "| Total | %d |\n\n", len(m.Tests))
	out.WriteString("## Feature decisions\n\nThe initial feature group was deferred by the user on 2026-10-11. Subsequently selected context propagation, data converters, custom failure conversion, generated protobuf values, corrupt-signal handling, stack-trace queries, native dynamic workflow/query/update handlers, named independent random streams with upstream golden semantics, native workflow streams and the isolate test environment are implemented. Other questions remain marked `awaiting_user` until answered; deferred tests stay blocked. No missing feature is implicitly treated as a completed port.\n\nThe isolate test facade reuses the host SDK test engine. Its original activity, timeout, retry, child scheduling and tracing assertion suites remain retained; linked native tests supplement those assertions. Direct ports cover the facade's native mock, listener, registration, ordering and completed-query behavior. Retained entries do not claim exhaustive duplication of every original assertion inside an isolate.\n\n| Feature | Decision | Blocked tests |\n| --- | --- | ---: |\n")
	keys := make([]string, 0, len(m.Features))
	for key := range m.Features {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		feature := m.Features[key]
		n := 0
		for _, entry := range m.Tests {
			if entry.Status == "blocked" && entry.Feature == key {
				n++
			}
		}
		fmt.Fprintf(&out, "| %s | %s | %d |\n", feature.Description, feature.Decision, n)
	}
	out.WriteString("\n## Enforcement and verification\n\nRun from this repository with the custom toolchain:\n\n```sh\n../golang-go/bin/tgo test ./...\n../golang-go/bin/tgo run ./internal/cmd/testports -upstream ../temporal-go-sdk\n```\n\nThe normal test suite rejects unfinished ports, missing reasons, duplicate entries, unpinned source files, missing runnable coverage targets, stale checklist text, and an SDK dependency version change. Comparing the source checkout also rejects new/unlisted tests, removed tests, and changed source test files. Run that comparison whenever upgrading or changing the upstream SDK.\n\nFull feature parity remains incomplete while blockers exist. To require that stricter condition:\n\n```sh\n../golang-go/bin/tgo run ./internal/cmd/testports -upstream ../temporal-go-sdk -require-no-blocked\n```\n\nRefresh this document after reviewing and updating the JSON inventory:\n\n```sh\n../golang-go/bin/tgo run ./internal/cmd/testports -write-checklist\n```\n\nRetained host tests can be run through the upstream repository's canonical runner:\n\n```sh\ncd ../temporal-go-sdk/internal/cmd/build\nTEMPORAL_GO_COMMAND=\"$(cd ../../../../golang-go/bin && pwd)/tgo\" go run . unit-test\n```\n\n## Source-test list\n")
	current := ""
	for _, entry := range m.Tests {
		if entry.Source != current {
			current = entry.Source
			fmt.Fprintf(&out, "\n### %s\n\n| Source test | Disposition | Coverage / blocker / reason |\n| --- | --- | --- |\n", current)
		}
		detail := entry.Reason
		if entry.Feature != "" {
			detail = "`" + entry.Feature + "`: " + detail
		}
		for _, target := range entry.Targets {
			parts := strings.SplitN(target, "::", 2)
			detail += fmt.Sprintf(" [%s](%s)", parts[1], parts[0])
		}
		detail = strings.ReplaceAll(detail, "|", "\\|")
		fmt.Fprintf(&out, "| `%s` | %s | %s |\n", entry.Name, entry.Status, detail)
	}
	return out.String()
}
