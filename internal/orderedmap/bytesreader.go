package orderedmap

import "bytes"

// bytesReader returns a *bytes.Reader without forcing json.NewDecoder callers
// to import bytes. Keeping it here makes the unmarshal code above readable.
func bytesReader(data []byte) *bytes.Reader {
	return bytes.NewReader(data)
}
