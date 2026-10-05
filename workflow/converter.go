package workflow

import "go.temporal.io/sdk/converter"

// This package's globals are initialized separately for every isolate. Build
// the pinned SDK's default converter here so its converter map, ordered list,
// and mutable converter values belong to that instance. Type metadata still uses the
// audited process services. The host worker keeps its ordinary default object.
// Keep this order aligned with sdk v1.49.0/converter/default_data_converter.go.
var instanceDataConverter = converter.NewCompositeDataConverter(
	converter.NewNilPayloadConverter(),
	converter.NewByteSlicePayloadConverter(),
	converter.NewProtoJSONPayloadConverter(),
	converter.NewProtoPayloadConverter(),
	converter.NewJSONPayloadConverter(),
)
