package worker

import (
	"testing"

	"github.com/mfateev/sdk-go-poc/temporalbridge"
	"github.com/mfateev/sdk-go-poc/workflow"
)

func unmarkedPropagators([]byte) ([]workflow.ContextPropagator, error) { return nil, nil }
func TestContextPropagatorConfiguration(t *testing.T) {
	for _, owner := range []any{&isolateWorker{}, &isolateReplayer{}} {
		if err := SetIsolateContextPropagators(owner, ContextPropagatorOptions{Factory: unmarkedPropagators}); err == nil {
			t.Fatal("unmarked factory accepted")
		}
		if err := SetIsolateContextPropagators(owner, ContextPropagatorOptions{Config: []byte("orphan")}); err == nil {
			t.Fatal("orphan config accepted")
		}
		if err := SetIsolateContextPropagators(owner, ContextPropagatorOptions{}); err != nil {
			t.Fatal(err)
		}
	}
	if err := SetIsolateContextPropagators(struct{}{}, ContextPropagatorOptions{}); err == nil {
		t.Fatal("ordinary worker accepted")
	}
	config := propagatorConfiguration{options: temporalbridge.ContextPropagatorOptions{Config: []byte("first")}}
	first := config.resolve()
	first.Config[0] = 'x'
	if string(config.resolve().Config) != "first" {
		t.Fatal("snapshot aliases configuration")
	}
	config.options.Config = []byte("later")
	if string(config.resolve().Config) != "later" {
		t.Fatal("configuration change was not published")
	}
}
