package tracing

import (
	"errors"
	"testing"
)

type recordingSpan struct{ finished []*TracerFinishSpanOptions }

func (s *recordingSpan) Finish(options *TracerFinishSpanOptions) {
	s.finished = append(s.finished, options)
}
func TestUpstreamWriteSpanHeaderFinishesSpanOnError(t *testing.T) {
	cause := errors.New("header error")
	span := new(recordingSpan)
	finish, err := writeSpanHeader(span, true, func(TracerSpanRef) error { return cause })
	if !errors.Is(err, cause) || finish != nil || len(span.finished) != 1 || !errors.Is(span.finished[0].Error, cause) {
		t.Fatalf("finish=%v error=%v spans=%v", finish == nil, err, span.finished)
	}
}
