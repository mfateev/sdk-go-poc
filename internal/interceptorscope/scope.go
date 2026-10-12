// Package interceptorscope holds scratch interceptor and random-stream state. The
// isolate compiler gives this package private query/validator globals, separate
// from workflow globals. This lets handlers use captured workflow contexts
// without exposing mutable workflow state.
package interceptorscope

// Current is set only after constructing a fresh read-only chain. Its concrete
// type lives in workflow, avoiding an import cycle. The scratch service owns it.
var Current any

// RandomStreams holds the named random generators for the current read-only
// request. Its concrete map type lives in workflow to avoid an import cycle.
var RandomStreams any
