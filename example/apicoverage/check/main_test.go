package main

import (
	"path/filepath"
	"testing"

	"github.com/mfateev/sdk-go-poc/internal/checktest"
)

func TestCompiledAPICoverageAndReplay(t *testing.T) {
	binary := checktest.Build(t, "api-check")
	dir, err := filepath.Abs("../testdata")
	if err != nil {
		t.Fatal(err)
	}
	for _, adapter := range []string{"", "v1", "v2", "datadog", "opentracing"} {
		t.Run("tracing-"+adapter, func(t *testing.T) {
			if out, err := checktest.Command(t, binary, "-history-dir", dir, "-tracing", adapter).CombinedOutput(); err != nil {
				t.Fatalf("API coverage/replay: %v\n%s", err, out)
			}
		})
	}
}
