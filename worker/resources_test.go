package worker

import (
	"github.com/mfateev/sdk-go-poc/temporalbridge"
	"isolate"
	"testing"
	"time"
)

func TestResourcesConfigurationSnapshots(t *testing.T) {
	w := Wrap(&testWorker{}).(*isolateWorker)
	factory := resourceFactory(temporalbridge.Factory{}, &w.resources).(temporalbridge.Factory)
	options := ResourceOptions{Limits: isolate.ResourceLimits{MaxMemoryBytes: 1 << 20, MaxGoroutines: 10}, MaxTaskDuration: time.Second}
	if err := SetIsolateResourceOptions(w, options); err != nil {
		t.Fatal(err)
	}
	if got := factory.ResolveResourceOptions(); got.Limits != options.Limits || got.MaxTaskDuration != options.MaxTaskDuration {
		t.Fatalf("configuration=%+v", got)
	}
	if err := SetIsolateResourceOptions(w, ResourceOptions{MaxNoProgressDuration: -1}); err == nil {
		t.Fatal("accepted negative watchdog")
	}
	if err := SetIsolateResourceOptions(NewWorkflowReplayer(), options); err != nil {
		t.Fatal(err)
	}
	if err := SetIsolateResourceOptions(&testWorker{}, options); err == nil {
		t.Fatal("accepted unwrapped worker")
	}
	if err := SetIsolateResourceOptions(w, ResourceOptions{}); err != nil {
		t.Fatal(err)
	}
	if factory.ResolveResourceOptions().Limits != (isolate.ResourceLimits{}) {
		t.Fatal("failed to restore unlimited defaults")
	}
}
