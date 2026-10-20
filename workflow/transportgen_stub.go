//go:build transportgen

package workflow

// Code generation inspects domain types without compiling the old conversions.
// These functions are never part of an application build.
func MarshalTransport(any) ([]byte, error) { panic("transport generation stub") }
func UnmarshalTransport([]byte, any) error { panic("transport generation stub") }
