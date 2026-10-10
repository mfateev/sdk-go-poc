// Package random exercises replay-seeded standard random APIs and UUIDs.
package random

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"math/big"
	mathrand "math/rand"
	randv2 "math/rand/v2"
	"strconv"
	"time"

	"github.com/google/uuid"
	"github.com/mfateev/sdk-go-poc/workflow"
)

var initial [17]byte

func init() {
	_, _ = rand.Read(initial[:])
}

//go:isolate
func RandomWorkflow(ctx context.Context) ([]string, error) {
	trace := []string{hex.EncodeToString(initial[:])}
	private := make([]byte, 17)
	if err := workflow.SetQueryHandler(ctx, "random", func() (string, error) {
		return uuid.NewString() + rand.Text(), nil
	}); err != nil {
		return nil, err
	}
	if err := workflow.SetQueryHandler(ctx, "bad", func() (string, error) {
		_, _ = rand.Read(private) // A random write must still enforce read-only ownership.
		return "unexpected", nil
	}); err != nil {
		return nil, err
	}
	appendValues := func() {
		trace = append(trace, uuid.NewString(), rand.Text())
		n, err := rand.Int(rand.Reader, big.NewInt(1000003))
		if err != nil {
			panic(err)
		}
		trace = append(trace, n.String(), strconv.FormatUint(mathrand.Uint64(), 16), strconv.FormatUint(randv2.Uint64(), 16))
		closed := make(chan struct{})
		close(closed)
		select {
		case <-closed:
			trace = append(trace, "select:0")
		case <-closed:
			trace = append(trace, "select:1")
		}
		_, _ = rand.Reader.Read(private)
		trace = append(trace, hex.EncodeToString(private))
	}
	appendValues()
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{StartToCloseTimeout: time.Minute})
	var echo []string
	if err := workflow.ExecuteActivity(ctx, Echo, trace).Get(ctx, &echo); err != nil {
		return nil, err
	}
	trace = append(trace, echo...)
	appendValues()
	// Exercise the library's cached reader and byte pool as isolate-owned state.
	uuid.EnableRandPool()
	trace = append(trace, uuid.NewString(), uuid.NewString())
	return trace, nil
}

func Echo(_ context.Context, input []string) ([]string, error) { return input, nil }
