package worker

import (
	"github.com/mfateev/sdk-go-poc/interceptor"
	"github.com/mfateev/sdk-go-poc/temporalbridge"
	"testing"
)

func TestInterceptorConfigurationValidation(t *testing.T) {
	w := Wrap(nil)
	if err := SetIsolateInterceptors(w, InterceptorOptions{}); err != nil {
		t.Fatal(err)
	}
	if err := SetIsolateInterceptors(w, InterceptorOptions{Config: []byte("settings")}); err == nil {
		t.Fatal("config without factory accepted")
	}
	factory := func([]byte) ([]interceptor.WorkerInterceptor, error) { return nil, nil }
	if err := SetIsolateInterceptors(w, InterceptorOptions{Factory: factory}); err == nil {
		t.Fatal("unmarked factory accepted")
	}
	if err := SetIsolateInterceptors(struct{}{}, InterceptorOptions{}); err == nil {
		t.Fatal("ordinary worker accepted")
	}
}
func TestInterceptorSnapshotOwnsConfig(t *testing.T) {
	original := []byte("config")
	c := interceptorConfiguration{options: temporalbridge.InterceptorOptions{Config: original}}
	snapshot := c.resolve()
	snapshot.Config[0] = 'X'
	if string(c.resolve().Config) != "config" {
		t.Fatal("execution snapshot aliases worker configuration")
	}
}
