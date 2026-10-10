package main

import (
	"testing"

	"github.com/mfateev/sdk-go-poc/internal/checktest"
)

func TestResourcesCompiled(t *testing.T) {
	binary := checktest.Build(t, "resources")
	if output, err := checktest.Command(t, binary).CombinedOutput(); err != nil {
		t.Fatalf("compiled controls: %v\n%s", err, output)
	}
}
