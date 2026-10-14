package main

import (
	"github.com/mfateev/sdk-go-poc/internal/checktest"
	"path/filepath"
	"strings"
	"testing"
)

func TestGeneratedProtobufWorkflowAPIs(t *testing.T) {
	binary := checktest.Build(t, "protobuf-check")
	for _, encoding := range []string{"json", "binary"} {
		for _, scenario := range []string{"values", "failure", "payloads", "random"} {
			t.Run(encoding+"/"+scenario, func(t *testing.T) {
				if out, err := checktest.Command(t, binary, "-encoding", encoding, "-scenario", scenario).CombinedOutput(); err != nil {
					t.Fatalf("protobuf APIs: %v\n%s", err, out)
				}
			})
		}
	}
}
func TestGeneratedProtobufHistoryReplay(t *testing.T) {
	binary := checktest.Build(t, "protobuf-history-check")
	dir, err := filepath.Abs("../testdata")
	if err != nil {
		t.Fatal(err)
	}
	if out, err := checktest.Command(t, binary, "-history-dir", dir).CombinedOutput(); err != nil {
		t.Fatalf("protobuf replay: %v\n%s", err, out)
	}
}

func TestGeneratedProtobufRejectsCustomResolver(t *testing.T) {
	binary := checktest.Build(t, "protobuf-resolver-check")
	out, err := checktest.Command(t, binary, "-scenario", "resolver").CombinedOutput()
	if err == nil || !strings.Contains(string(out), "forbidden operation custom protobuf resolver") {
		t.Fatalf("custom resolver policy: %v\n%s", err, out)
	}
}

func TestGeneratedProtobufSharedMetadata(t *testing.T) {
	binary := checktest.Build(t, "protobuf-cache-check")
	if out, err := checktest.Command(t, binary, "-scenario", "cache").CombinedOutput(); err != nil {
		t.Fatalf("shared protobuf metadata: %v\n%s", err, out)
	}
}
