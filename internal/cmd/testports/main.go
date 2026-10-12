package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sort"

	"github.com/mfateev/sdk-go-poc/internal/testports"
)

func main() {
	root := flag.String("root", ".", "SDK repository root")
	upstream := flag.String("upstream", "", "Temporal SDK checkout to compare against the pinned inventory")
	strict := flag.Bool("require-no-blocked", false, "fail if user-deferred feature tests remain blocked")
	writeChecklist := flag.Bool("write-checklist", false, "regenerate TEST_PORTING.md from the reviewed inventory")
	flag.Parse()
	absolute, err := filepath.Abs(*root)
	if err != nil {
		fail(err)
	}
	m, err := testports.Load()
	if err != nil {
		fail(err)
	}
	if *writeChecklist {
		if err := os.WriteFile(filepath.Join(absolute, "TEST_PORTING.md"), []byte(testports.RenderChecklist(m)), 0644); err != nil {
			fail(err)
		}
	}
	counts, auditErr := testports.Audit(m, absolute, *upstream, *strict)
	statuses := make([]string, 0, len(counts))
	for status := range counts {
		statuses = append(statuses, status)
	}
	sort.Strings(statuses)
	total := 0
	for _, status := range statuses {
		fmt.Printf("%s: %d\n", status, counts[status])
		total += counts[status]
	}
	fmt.Printf("total: %d\nsource: %s\n", total, m.SourceCommit)
	if auditErr != nil {
		fail(auditErr)
	}
}
func fail(err error) { fmt.Fprintln(os.Stderr, err); os.Exit(1) }
