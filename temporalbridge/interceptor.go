package temporalbridge

import "isolate"

// InterceptorOptions is an immutable execution snapshot. Application objects
// never cross into an isolate; only compiler handles and copied config do.
type InterceptorOptions struct {
	Factory isolate.Handle
	Config  []byte
}
