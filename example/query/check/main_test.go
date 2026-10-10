package main

import (
	"testing"

	"github.com/mfateev/sdk-go-poc/internal/checktest"
)

func TestCompiledQueriesAndCompletedState(t *testing.T) {
	binary := checktest.Build(t, "query-check")
	run := checktest.Command(t, binary)
	if output, err := run.CombinedOutput(); err != nil {
		t.Fatalf("query check: %v\n%s", err, output)
	}
}
