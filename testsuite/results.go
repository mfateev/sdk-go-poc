package testsuite

import "go.temporal.io/sdk/converter"

// DecodeResult returns a typed update result on the host. Native results arrive
// as copied payloads; ordinary SDK handlers already return the concrete value.
// Use the environment's data converter as the optional argument when customized.
func DecodeResult[T any](value any, converters ...converter.DataConverter) T {
	if len(converters) > 1 {
		panic("testsuite: at most one result data converter is supported")
	}
	if result, ok := value.(T); ok {
		return result
	}
	raw, ok := value.(converter.RawValue)
	if !ok {
		panic("testsuite: update result is not a typed value or raw payload")
	}
	dc := converter.GetDefaultDataConverter()
	if len(converters) == 1 {
		dc = converters[0]
	}
	var result T
	if err := dc.FromPayload(raw.Payload(), &result); err != nil {
		panic(err)
	}
	return result
}
