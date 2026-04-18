package pytrader

import (
	"bytes"
	"encoding/json"
	"fmt"
)

// jsonObjectKeyOrder returns the keys of the nested object at `field` in the
// same order they appear in the source bytes. encoding/json discards this
// order when unmarshalling into a map, so we do a streaming pass to recover
// it when we need deterministic iteration (e.g. orders-by-symbol).
func jsonObjectKeyOrder(raw []byte, field string) ([]string, error) {
	dec := json.NewDecoder(bytes.NewReader(raw))
	tok, err := dec.Token()
	if err != nil {
		return nil, err
	}
	if d, ok := tok.(json.Delim); !ok || d != '{' {
		return nil, fmt.Errorf("jsonObjectKeyOrder: expected top-level object")
	}
	for dec.More() {
		tok, err := dec.Token()
		if err != nil {
			return nil, err
		}
		name, ok := tok.(string)
		if !ok {
			return nil, fmt.Errorf("jsonObjectKeyOrder: non-string key")
		}
		if name != field {
			// Skip value.
			var skip json.RawMessage
			if err := dec.Decode(&skip); err != nil {
				return nil, err
			}
			continue
		}
		subTok, err := dec.Token()
		if err != nil {
			return nil, err
		}
		if d, ok := subTok.(json.Delim); !ok || d != '{' {
			return nil, nil
		}
		var keys []string
		for dec.More() {
			keyTok, err := dec.Token()
			if err != nil {
				return nil, err
			}
			key, ok := keyTok.(string)
			if !ok {
				return nil, fmt.Errorf("jsonObjectKeyOrder: non-string nested key")
			}
			keys = append(keys, key)
			var skip json.RawMessage
			if err := dec.Decode(&skip); err != nil {
				return nil, err
			}
		}
		return keys, nil
	}
	return nil, nil
}
