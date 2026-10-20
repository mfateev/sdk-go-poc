package main

import (
	"github.com/mfateev/sdk-go-poc/internal/checktest"
	"path/filepath"
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

func TestGeneratedProtobufAcceptsCustomResolver(t *testing.T) {
	binary := checktest.Build(t, "protobuf-resolver-check")
	out, err := checktest.Command(t, binary, "-scenario", "resolver").CombinedOutput()
	if err != nil {
		t.Fatalf("custom resolver policy: %v\n%s", err, out)
	}
}

func TestGeneratedProtobufIndependentMetadata(t *testing.T) {
	binary := checktest.Build(t, "protobuf-cache-check")
	if out, err := checktest.Command(t, binary, "-scenario", "cache").CombinedOutput(); err != nil {
		t.Fatalf("independent protobuf metadata: %v\n%s", err, out)
	}
}

func TestCustomProtoReflectCannotRunAsHost(t *testing.T) {
	binary := checktest.Build(t, "protobuf-custom-check")
	output, err := checktest.Command(t, binary, "-scenario", "custom-message").CombinedOutput()
	if err != nil {
		t.Fatalf("custom protobuf implementation: %v\n%s", err, output)
	}
}
