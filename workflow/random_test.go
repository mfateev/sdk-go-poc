package workflow

import (
	"context"
	"encoding/hex"
	"testing"
)

func TestRandomStreamSeed(t *testing.T) {
	baseline := deriveRandomStreamSeed("original-run", "choices")
	// Pin the domain and unambiguous byte encoding to protect saved histories.
	const want = "1c1d03759c60be03d3c61d416a5b071b7ba6acdef4b39870ff1d8c24395ef7cc"
	if got := hex.EncodeToString(baseline[:]); got != want {
		t.Fatalf("seed = %s, want %s", got, want)
	}
	for _, identity := range [][2]string{{"other-run", "choices"}, {"original-run", "other"}, {"original-run", "Choices"}, {"original-run", ""}, {"ab", "c"}, {"a", "bc"}} {
		if deriveRandomStreamSeed(identity[0], identity[1]) == baseline {
			t.Fatalf("stream identity %q reused seed", identity)
		}
	}
	if deriveRandomStreamSeed("ab", "c") == deriveRandomStreamSeed("a", "bc") {
		t.Fatal("stream identities have ambiguous encoding")
	}
}

func TestRandomStreamRequiresNativeContext(t *testing.T) {
	for _, ctx := range []context.Context{nil, context.Background()} {
		func() {
			defer func() {
				if recover() == nil {
					t.Error("random stream accepted an invalid execution context")
				}
			}()
			GetRandomStream(ctx, "name")
		}()
	}
}
