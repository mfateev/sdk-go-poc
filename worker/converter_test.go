package worker

import (
	"github.com/mfateev/sdk-go-poc/temporalbridge"
	"go.temporal.io/sdk/converter"
	"testing"
)

func unmarkedConverter([]byte) (converter.DataConverter, error) { return nil, nil }

func TestConverterConfigurationValidation(t *testing.T) {
	w := &isolateWorker{}
	if err := SetIsolateDataConverter(w, DataConverterOptions{Factory: unmarkedConverter}); err == nil {
		t.Fatal("unmarked factory accepted")
	}
	if err := SetIsolateDataConverter(w, DataConverterOptions{Config: []byte("orphan")}); err == nil {
		t.Fatal("orphan configuration accepted")
	}
	if err := SetIsolateDataConverter(struct{}{}, DataConverterOptions{}); err == nil {
		t.Fatal("ordinary worker accepted")
	}
	for _, owner := range []any{w, &isolateReplayer{}} {
		if err := SetIsolateDataConverter(owner, DataConverterOptions{}); err != nil {
			t.Fatal(err)
		}
	}
}

func TestConverterConfigurationCopiesEverySnapshot(t *testing.T) {
	input := []byte("first")
	c := converterConfiguration{options: temporalbridge.DataConverterOptions{Config: input}}
	first := c.resolve()
	first.Config[0] = 'x'
	second := c.resolve()
	if string(second.Config) != "first" {
		t.Fatal("configuration aliased")
	}
	factory := converterFactory(temporalbridge.Factory{}, &c).(temporalbridge.Factory)
	c.options.Config = []byte("later")
	if string(factory.ResolveDataConverter().Config) != "later" {
		t.Fatal("configuration resolved during registration")
	}
}
