package worker

import (
	"github.com/mfateev/sdk-go-poc/temporalbridge"
	"go.temporal.io/sdk/converter"
	"testing"
)

func unmarkedFailures([]byte, converter.DataConverter) (converter.FailureConverter, error) {
	return nil, nil
}

func TestFailureConverterConfiguration(t *testing.T) {
	for _, w := range []any{&isolateWorker{}, &isolateReplayer{}} {
		if err := SetIsolateFailureConverter(w, FailureConverterOptions{Factory: unmarkedFailures}); err == nil {
			t.Fatal("unmarked factory accepted")
		}
		if err := SetIsolateFailureConverter(w, FailureConverterOptions{Config: []byte("orphan")}); err == nil {
			t.Fatal("orphan configuration accepted")
		}
		if err := SetIsolateFailureConverter(w, FailureConverterOptions{}); err != nil {
			t.Fatal(err)
		}
	}
	if err := SetIsolateFailureConverter(struct{}{}, FailureConverterOptions{}); err == nil {
		t.Fatal("ordinary worker accepted")
	}
	c := failureConverterConfiguration{options: temporalbridge.FailureConverterOptions{Config: []byte("private")}}
	snapshot := c.resolve()
	snapshot.Config[0] = 'x'
	if string(c.resolve().Config) != "private" {
		t.Fatal("snapshot aliases configuration")
	}
}
