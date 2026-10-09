// Package sinkop defines the shared wire range for application observations.
package sinkop

// Keep SDK Calls below this range and runtime operations (including logging)
// above it. Operation codes are stable application contracts, not registration
// order or workflow Call IDs.
const (
	Min uint32 = 0x00010000
	Max uint32 = 0xfffeffff
)

func Valid(op uint32) bool { return op >= Min && op <= Max }
