package engine

import "encoding/json"

// jsonUnmarshalString decodes a json.RawMessage containing a string.
// Centralised here so matching.go does not need to import encoding/json directly.
func jsonUnmarshalString(raw json.RawMessage, out *string) error {
	return json.Unmarshal(raw, out)
}
