package main

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/mfateev/sdk-go-poc/internal/checktest"
)

func TestCompiledNamedRandomStreams(t *testing.T) {
	binary := checktest.Build(t, "named-streams-check")
	var baseline []byte
	for _, procs := range []string{"1", "2", "8"} {
		command := checktest.Command(t, binary, "-streams")
		command.Env = append(os.Environ(), "GOMAXPROCS="+procs, "GOGC=1")
		output, err := command.CombinedOutput()
		if err != nil {
			t.Fatalf("named streams procs=%s: %v\n%s", procs, err, output)
		}
		if baseline == nil {
			baseline = output
		} else if !bytes.Equal(output, baseline) {
			t.Fatalf("named streams changed with GOMAXPROCS=%s", procs)
		}
	}
}

func TestNamedRandomStreamHistoryReplay(t *testing.T) {
	binary := checktest.Build(t, "named-streams-replay")
	paths, err := filepath.Glob("../testdata/*-cache-*.json")
	if err != nil {
		t.Fatal(err)
	}
	if len(paths) != 4 {
		t.Fatalf("expected four named stream histories, got %d", len(paths))
	}
	for _, procs := range []string{"1", "2", "8"} {
		for _, path := range paths {
			absolute, err := filepath.Abs(path)
			if err != nil {
				t.Fatal(err)
			}
			command := checktest.Command(t, binary, "-streams-history", absolute)
			command.Env = append(os.Environ(), "GOMAXPROCS="+procs, "GOGC=1")
			if output, err := command.CombinedOutput(); err != nil {
				t.Fatalf("named stream replay %s procs=%s: %v\n%s", path, procs, err, output)
			}
		}
	}
}
func TestLiveNamedRandomStreams(t *testing.T) {
	if os.Getenv("RANDOM_STREAMS_DEV_SERVER") != "1" {
		t.Skip("set RANDOM_STREAMS_DEV_SERVER=1 for live named stream replay")
	}
	binary := checktest.Build(t, "named-streams-live")
	if output, err := checktest.Command(t, binary, "-live-streams", "-history-dir", t.TempDir()).CombinedOutput(); err != nil {
		t.Fatalf("live named streams: %v\n%s", err, output)
	}
}
