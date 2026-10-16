package testports

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestTemporalUnitTestMigrationList(t *testing.T) {
	m, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	root := filepath.Join("..", "..")
	counts, err := Audit(m, root, "", false)
	if err != nil {
		t.Fatal(err)
	}
	checklist, err := os.ReadFile(filepath.Join(root, "TEST_PORTING.md"))
	if err != nil {
		t.Fatal(err)
	}
	if string(checklist) != RenderChecklist(m) {
		t.Fatal("TEST_PORTING.md is stale; run go run ./internal/cmd/testports -write-checklist")
	}
	t.Logf("Temporal test dispositions: %v", counts)
}

func TestAuditTracksLocalSDKReplacement(t *testing.T) {
	root := t.TempDir()
	mod := "module example.org/probe\n\ngo 1.26\n\nrequire go.temporal.io/sdk v1.50.0\nreplace go.temporal.io/sdk => ../temporal-go-sdk\n"
	if err := os.WriteFile(filepath.Join(root, "go.mod"), []byte(mod), 0600); err != nil {
		t.Fatal(err)
	}
	m := Manifest{SourceCommit: "baseline", SDKVersion: "v1.50.0", SDKReplacement: "../temporal-go-sdk", Files: map[string]string{"source_test.go": "hash"}, Tests: []Entry{{Source: "source_test.go", Name: "TestSource", Status: "retained", Reason: "baseline assertion"}}}
	if _, err := Audit(m, root, "", false); err != nil {
		t.Fatal(err)
	}
	m.SDKReplacement = "../another-sdk"
	if _, err := Audit(m, root, "", false); err == nil || !strings.Contains(err.Error(), "SDK dependency changed") {
		t.Fatalf("replacement drift accepted: %v", err)
	}
}

func TestAuditRejectsUnfinishedOrBrokenCoverage(t *testing.T) {
	root := t.TempDir()
	m := Manifest{SourceCommit: "revision", SDKVersion: "version", Files: map[string]string{"source_test.go": "hash"}}
	for _, entry := range []Entry{
		{Source: "source_test.go", Name: "TestSource", Status: "pending", Reason: "awaiting port"},
		{Source: "source_test.go", Name: "TestSource", Status: "covered", Reason: "ported", Targets: []string{"missing_test.go::TestMissing"}},
		{Source: "source_test.go", Name: "TestSource", Status: "blocked", Reason: "missing feature", Feature: "unapproved"},
	} {
		m.Tests = []Entry{entry}
		if _, err := Audit(m, root, "", false); err == nil {
			t.Fatalf("accepted invalid entry: %+v", entry)
		}
	}
	m.Features = map[string]Feature{"deferred": {Description: "feature", Decision: "user_deferred", Request: "User requested deferral"}}
	m.Tests = []Entry{{Source: "source_test.go", Name: "TestSource", Status: "blocked", Reason: "missing feature", Feature: "deferred"}}
	if _, err := Audit(m, root, "", false); err != nil {
		t.Fatal(err)
	}
	if _, err := Audit(m, root, "", true); err == nil || !strings.Contains(err.Error(), "blocked test") {
		t.Fatalf("strict audit accepted blocker: %v", err)
	}
}

func TestAuditDetectsUnlistedSuiteMethodAndSourceDrift(t *testing.T) {
	root, source := t.TempDir(), t.TempDir()
	path := filepath.Join(source, "source_test.go")
	initial := "package source\nimport \"testing\"\nfunc TestFirst(t *testing.T) {}\ntype SourceSuite struct{}\nfunc (*SourceSuite) TestMethod() {}\n"
	if err := os.WriteFile(path, []byte(initial), 0644); err != nil {
		t.Fatal(err)
	}
	tests, hashes, err := Scan(source, true)
	if err != nil {
		t.Fatal(err)
	}
	if len(tests) != 2 {
		t.Fatalf("suite methods not inventoried: %v", tests)
	}
	m := Manifest{SourceCommit: "revision", SDKVersion: "version", Files: hashes}
	for id := range tests {
		parts := strings.SplitN(id, "::", 2)
		m.Tests = append(m.Tests, Entry{Source: parts[0], Name: parts[1], Status: "retained", Reason: "Retained host behavior"})
	}
	if _, err := Audit(m, root, source, false); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(initial+"func (*SourceSuite) TestNewMethod() {}\n"), 0644); err != nil {
		t.Fatal(err)
	}
	_, err = Audit(m, root, source, false)
	if err == nil || !strings.Contains(err.Error(), "unlisted upstream test: source_test.go::SourceSuite.TestNewMethod") || !strings.Contains(err.Error(), "upstream test file changed") {
		t.Fatalf("source upgrade accepted: %v", err)
	}
}
