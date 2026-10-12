package workflow

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"io"
	"isolate"
	rand "math/rand/v2"
	"sync"

	"github.com/mfateev/sdk-go-poc/internal/interceptorscope"
)

// WorkflowRandomStream is a deterministic random source and byte reader.
// Uint64 consumes the next eight bytes in little-endian order, so Read and
// Uint64 may be freely interleaved. Wrap the source with rand.New for v2.Rand
// distributions. Seed and UnmarshalBinary are deliberately not exposed.
type WorkflowRandomStream interface {
	rand.Source
	io.Reader
}

type randomStreamSource struct {
	name  string
	state *rand.ChaCha8
	mu    sync.Mutex
}

var randomStreams = make(map[string]*randomStreamSource)
var randomStreamsMu sync.Mutex
var randomStreamRunID string

// GetRandomStream returns the execution's persistent source for name. Names
// use exact, case-sensitive matching, including the empty name. Streams are
// independent of each other and of top-level randomness and native select.
// Derived and canceled contexts share the source; draws are synchronized.
//
// Queries and update validators use per-request scratch snapshots, including
// through captured source handles. Accepted updates advance workflow state.
// Replay reproduces previous draws. Reset reproduces draws before the reset
// point and reseeds existing handles in place for subsequent draws. Each
// Continue-As-New run starts independent streams.
//
// The seed and sequence match the upstream Temporal SDK. Draw ordering is part
// of the workflow's replay contract. These predictable generators must not be
// used for cryptographic secrets. Calls require a non-nil workflow context.
func GetRandomStream(ctx context.Context, name string) WorkflowRandomStream {
	if ctx == nil {
		panic("workflow: nil context")
	}
	if !isolate.IsActive() {
		panic("workflow: GetRandomStream requires an active isolate")
	}
	if isolate.IsReadOnly() {
		return scratchRandomStream(name, nil)
	}
	randomStreamsMu.Lock()
	defer randomStreamsMu.Unlock()
	return getRandomStream(randomStreams, randomStreamRunID, name)
}

func getRandomStream(streams map[string]*randomStreamSource, runID, name string) *randomStreamSource {
	if source := streams[name]; source != nil {
		return source
	}
	if runID == "" {
		panic("workflow: random streams require startup execution identity")
	}
	source := &randomStreamSource{name: name, state: rand.NewChaCha8(deriveRandomStreamSeed(runID, name))}
	streams[name] = source
	return source
}

func (s *randomStreamSource) Uint64() uint64 {
	var buf [8]byte
	_, _ = s.Read(buf[:])
	return binary.LittleEndian.Uint64(buf[:])
}

func (s *randomStreamSource) Read(p []byte) (int, error) {
	if isolate.IsReadOnly() {
		return scratchRandomStream(s.name, s).state.Read(p)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.state.Read(p)
}

func scratchRandomStream(name string, source *randomStreamSource) *randomStreamSource {
	streams, _ := interceptorscope.RandomStreams.(map[string]*randomStreamSource)
	if streams == nil {
		streams = make(map[string]*randomStreamSource)
		interceptorscope.RandomStreams = streams
	}
	if stream := streams[name]; stream != nil {
		return stream
	}
	// Workflow dispatch is fenced throughout the read-only request.
	if source == nil {
		source = randomStreams[name]
	}
	if source == nil {
		return getRandomStream(streams, randomStreamRunID, name)
	}
	snapshot := *source.state
	stream := &randomStreamSource{name: name, state: &snapshot}
	streams[name] = stream
	return stream
}

func reseedRandomStreams(streams map[string]*randomStreamSource, runID string) {
	for name, source := range streams {
		source.mu.Lock()
		source.state.Seed(deriveRandomStreamSeed(runID, name))
		source.mu.Unlock()
	}
}

func startRandomStreams(runID string) { randomStreamRunID = runID }

// WorkflowControl is copied task metadata delivered through the existing
// cancellation service before workflow or update continuations resume.
type WorkflowControl struct {
	RandomRunID string `json:"random_run_id"`
	Canceled    bool   `json:"canceled"`
}

func applyRandomRunID(runID string) {
	if runID == "" || runID == randomStreamRunID {
		return
	}
	randomStreamsMu.Lock()
	defer randomStreamsMu.Unlock()
	randomStreamRunID = runID
	reseedRandomStreams(randomStreams, runID)
}

// Keep upstream's domain and separators byte-for-byte: this is a replay contract.
func deriveRandomStreamSeed(runID, name string) [32]byte {
	return sha256.Sum256([]byte("temporal.sdk.random.v1\x00" + runID + "\x00" + name))
}
