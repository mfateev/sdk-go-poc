package workflow

import (
	"encoding/binary"
	"encoding/hex"
	"math/rand/v2"
	"testing"

	"github.com/stretchr/testify/require"
)

const (
	workflowRandomTestRunID = "runID"
	workflowRandomTestName  = "go.temporal.io/sdk/internal/test"
)

func TestDeriveSeed(t *testing.T) {
	cases := []struct {
		runID string
		name  string
	}{
		{runID: "other", name: workflowRandomTestName},
		{runID: workflowRandomTestRunID, name: "other"},
		{runID: "other", name: "other"},
	}

	for _, tc := range cases {
		require.NotEqual(t, deriveRandomStreamSeed(workflowRandomTestRunID, workflowRandomTestName), deriveRandomStreamSeed(tc.runID, tc.name))
	}
}

// TestGetRandomStreamGolden pins the seed derivation and resulting byte stream.
// Changing either would break replay for existing workflows.
func TestGetRandomStreamGolden(t *testing.T) {
	seed := deriveRandomStreamSeed(workflowRandomTestRunID, workflowRandomTestName)
	require.Equal(t, "0d143739fa5a902590bac3b5bff5f52b539f57ae2ba4cf0ab3b034623b1da7ec", hex.EncodeToString(seed[:]))

	randoms := make(map[string]*randomStreamSource)
	randomBytes := make([]byte, 32)

	n, err := getRandomStream(randoms, workflowRandomTestRunID, workflowRandomTestName).Read(randomBytes)
	require.NoError(t, err)
	require.Equal(t, len(randomBytes), n)
	require.Equal(t, "10861bf410d33891bef9b1f2ebddc1af2f5bceffe86c13fdcb8534a08805b1a7", hex.EncodeToString(randomBytes))
}

// TestUint64Golden pins Uint64's value for a known seed. Changing the
// Uint64/Read derivation would break replay for existing workflows.
func TestUint64Golden(t *testing.T) {
	randoms := make(map[string]*randomStreamSource)
	v := getRandomStream(randoms, workflowRandomTestRunID, workflowRandomTestName).Uint64()
	require.Equal(t, uint64(0x9138d310f41b8610), v)
}

// TestInterleavedReadUint64StableOrdering verifies that interleaving Read and
// Uint64 calls consumes the underlying byte stream in the same order, and
// with the same byte boundaries, as a single equivalent-length Read: Uint64
// must be equivalent to reading 8 bytes and decoding them as little-endian,
// not a separate, independently-buffered draw from the source. This is the
// stability guarantee tracked by
// https://github.com/temporalio/sdk-go/issues/2547.
//
// The reads are deliberately NOT 8-byte-aligned (3, then 5): an 8-byte Read
// never leaves anything in ChaCha8's internal leftover-byte buffer, so an
// aligned interleaving passes even on the unfixed implementation and would
// not catch a regression here.
func TestInterleavedReadUint64StableOrdering(t *testing.T) {
	randomsInterleaved := make(map[string]*randomStreamSource)
	c := getRandomStream(randomsInterleaved, workflowRandomTestRunID, workflowRandomTestName)

	read1 := make([]byte, 3)
	_, err := c.Read(read1)
	require.NoError(t, err)

	u := c.Uint64()
	uBytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(uBytes, u)

	read2 := make([]byte, 5)
	_, err = c.Read(read2)
	require.NoError(t, err)

	reconstructed := append(append(append([]byte{}, read1...), uBytes...), read2...)

	randomsFull := make(map[string]*randomStreamSource)
	full := make([]byte, 16)
	_, err = getRandomStream(randomsFull, workflowRandomTestRunID, workflowRandomTestName).Read(full)
	require.NoError(t, err)

	require.Equal(t, full, reconstructed)
}

func TestDeriveSeedSeparators(t *testing.T) {
	require.NotEqual(t,
		deriveRandomStreamSeed("ab", "c"),
		deriveRandomStreamSeed("a", "bc"),
	)
}

// TestGetRandomStreamMemoizes verifies a second lookup under the same name continues
// the sequence rather than restarting it.
func TestGetRandomStreamMemoizes(t *testing.T) {
	randoms := make(map[string]*randomStreamSource)

	c1 := getRandomStream(randoms, workflowRandomTestRunID, workflowRandomTestName)
	firstDraw := c1.Uint64()

	c2 := getRandomStream(randoms, workflowRandomTestRunID, workflowRandomTestName)
	secondDraw := c2.Uint64()

	require.Same(t, c1, c2)
	require.NotEqual(t, firstDraw, secondDraw)
}

// TestGetRandomStreamNamesAreIndependent verifies that interleaving draws across two
// names yields the same sequence per name as drawing from each on its own, so
// how often a workflow draws from one name cannot shift another.
func TestGetRandomStreamNamesAreIndependent(t *testing.T) {

	solo := func(name string, draws int) []uint64 {
		var res []uint64

		randoms := make(map[string]*randomStreamSource)
		c := getRandomStream(randoms, workflowRandomTestRunID, name)
		r := rand.New(c)

		for range draws {
			res = append(res, r.Uint64())
		}
		return res
	}

	var interleavedA, interleavedB []uint64

	randoms := make(map[string]*randomStreamSource)
	c1 := getRandomStream(randoms, workflowRandomTestRunID, workflowRandomTestName)
	c2 := getRandomStream(randoms, workflowRandomTestRunID, "other")

	for range 3 {
		interleavedA = append(interleavedA, c1.Uint64())
		interleavedB = append(interleavedB, c2.Uint64())
	}

	require.Equal(t, solo(workflowRandomTestName, 3), interleavedA)
	require.Equal(t, solo("other", 3), interleavedB)
	require.NotEqual(t, interleavedA, interleavedB)
}

func TestReseedRandomsInPlace(t *testing.T) {
	randoms := make(map[string]*randomStreamSource)

	c1 := getRandomStream(randoms, workflowRandomTestRunID, workflowRandomTestName)
	reseedRandomStreams(randoms, "other")
	c2 := getRandomStream(randoms, "other", workflowRandomTestName)

	require.Same(t, c1, c2)
}
