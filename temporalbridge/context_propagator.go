package temporalbridge

import "isolate"

// ContextPropagatorOptions snapshots compiler-created native propagators. Only
// the factory metadata and copied configuration bytes enter an isolate.
type ContextPropagatorOptions struct {
	Factory isolate.Handle
	Config  []byte
}
