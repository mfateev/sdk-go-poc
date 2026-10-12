package workflow

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"isolate"
	rand "math/rand/v2"
	"sync"

	"github.com/mfateev/sdk-go-poc/internal/interceptorscope"
)

type randomStream struct {
	source *randomStreamSource
	random *rand.Rand
}

type randomStreamSource struct {
	name  string
	state *rand.ChaCha8
	mu    sync.Mutex
}

var randomStreams = make(map[string]*randomStream)
var randomStreamsMu sync.Mutex
var randomStreamRunID string

// GetRandomStream returns the execution's persistent generator for name. Names
// may be empty and use exact, case-sensitive matching. Streams are independent
// of one another and of top-level random APIs and native select polling.
//
// The generator is safe for concurrent use by workflow goroutines, although
// their draw order still depends on deterministic scheduling. Derived contexts
// share streams. Cancellation does not reset or disable a stream. Queries and
// update validators use per-request scratch copies of the current stream state,
// including when calling a generator captured from workflow code.
//
// Seeds derive from the recorded original run ID and name. They are public and
// predictable: never use these streams for cryptographic secrets. Replay and
// reset preserve the original identity; Continue-As-New starts fresh streams.
// Calls require a non-nil context inside an active isolate.
func GetRandomStream(ctx context.Context, name string) *rand.Rand {
	if ctx == nil {
		panic("workflow: nil context")
	}
	if !isolate.IsActive() {
		panic("workflow: GetRandomStream requires an active isolate")
	}
	if isolate.IsReadOnly() {
		return scratchRandomStream(name, nil).random
	}
	randomStreamsMu.Lock()
	defer randomStreamsMu.Unlock()
	stream := randomStreams[name]
	if stream == nil {
		source := &randomStreamSource{name: name, state: rand.NewChaCha8(randomStreamSeed(name))}
		stream = &randomStream{source: source, random: rand.New(source)}
		randomStreams[name] = stream
	}
	return stream.random
}

func (s *randomStreamSource) Uint64() uint64 {
	if isolate.IsReadOnly() {
		return scratchRandomStream(s.name, s).source.state.Uint64()
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.state.Uint64()
}

func scratchRandomStream(name string, source *randomStreamSource) *randomStream {
	streams, _ := interceptorscope.RandomStreams.(map[string]*randomStream)
	if streams == nil {
		streams = make(map[string]*randomStream)
		interceptorscope.RandomStreams = streams
	}
	if stream := streams[name]; stream != nil {
		return stream
	}
	// Workflow dispatch is fenced during read-only requests, so reading the map
	// and copying the ChaCha8 value cannot race a workflow draw or registration.
	if source == nil {
		if stream := randomStreams[name]; stream != nil {
			source = stream.source
		}
	}
	var state *rand.ChaCha8
	if source == nil {
		state = rand.NewChaCha8(randomStreamSeed(name))
	} else {
		snapshot := *source.state
		state = &snapshot
	}
	stream := &randomStream{source: &randomStreamSource{name: name, state: state}, random: rand.New(state)}
	streams[name] = stream
	return stream
}

func randomStreamSeed(name string) [32]byte {
	if randomStreamRunID == "" {
		panic("workflow: random streams require startup execution identity")
	}
	return deriveRandomStreamSeed(randomStreamRunID, name)
}

// The domain and length encoding are a replay contract. Streams do not draw
// seed bytes from the runtime's default generator.
func deriveRandomStreamSeed(runID, name string) [32]byte {
	data := []byte("isolate/workflow-random-stream/v1")
	data = binary.LittleEndian.AppendUint64(data, uint64(len(runID)))
	data = append(data, runID...)
	data = binary.LittleEndian.AppendUint64(data, uint64(len(name)))
	data = append(data, name...)
	return sha256.Sum256(data)
}
