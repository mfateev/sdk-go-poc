// Package tracecontext carries observation-only identity for read-only scopes.
package tracecontext

// ScopeKey never enters writable workflow contexts. Queries/validators receive
// a host-generated token so separate requests do not export identical span IDs.
// This does not seed or advance the workflow's application random stream.
type ScopeKey struct{}
