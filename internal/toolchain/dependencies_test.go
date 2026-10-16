package toolchain

import "testing"

func TestTemporalSDKSource(t *testing.T) {
	for _, test := range []struct {
		path, replacement string
		want              bool
	}{
		{"", "", true},
		{"../temporal-go-sdk-main", "", true},
		{"./sdk", "", true},
		{".", "", true},
		{"/tmp/sdk-go-main", "", true},
		{"../temporal-go-sdk-main", "v1.50.0", false},
		{"github.com/mfateev/temporal-go-sdk", "v1.49.0-isolates.1", false},
		{"example.org/temporal-go-sdk", "", false},
		{"", "v1.50.0", false},
	} {
		if got := TemporalSDKSource(test.path, test.replacement); got != test.want {
			t.Errorf("TemporalSDKSource(%q, %q) = %v, want %v", test.path, test.replacement, got, test.want)
		}
	}
}
