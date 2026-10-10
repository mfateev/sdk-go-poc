// Package interceptorscope holds the active scratch interceptor chain. The
// isolate compiler gives this package private query/validator globals, separate
// from workflow globals. This lets handlers use captured workflow contexts
// without exposing the cached mutable interceptor chain.
package interceptorscope

// Current is set only after constructing a fresh read-only chain. Its concrete
// type lives in workflow, avoiding an import cycle. The scratch service owns it.
var Current any
