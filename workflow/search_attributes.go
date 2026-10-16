package workflow

import (
	bindings "go.temporal.io/sdk/internalbindings"
	"go.temporal.io/sdk/temporal"
)

// SearchAttributeEntry contains a typed key and its copied value. A nil value
// represents an unset update. Keyword-list slices are private copies.
type SearchAttributeEntry = bindings.SearchAttributeValue

// SearchAttributeEntries returns typed values in canonical name/type order.
// Use it to traverse a collection in a native workflow; interface-keyed map
// iteration is outside the deterministic map subset.
func SearchAttributeEntries(attributes temporal.SearchAttributes) []SearchAttributeEntry {
	return bindings.GetSearchAttributeUpdates(attributes)
}
