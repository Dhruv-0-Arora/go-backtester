// Package orderedmap provides a small generic map-like container that preserves
// insertion order of its keys.
//
// The backtester produces deterministic JSON output whose top-level field order
// matches the order in which products were discovered. Go's built-in map does
// not guarantee iteration order, so most mapping state (positions, cash,
// per-product trade lists, etc.) flows through this helper instead.
//
// The implementation intentionally stays minimal: a slice of keys for
// deterministic iteration plus a regular map for O(1) point lookups. All
// operations are O(1) except for Delete which is O(n) in the number of keys.
// None of the methods are safe for concurrent use by multiple goroutines
// without external synchronization; the runner always drives a single
// goroutine per backtest.
package orderedmap

import (
	"encoding/json"
	"fmt"
)

// Map is a string-keyed map that preserves insertion order.
type Map[V any] struct {
	keys   []string
	values map[string]V
}

// New returns an empty Map.
func New[V any]() *Map[V] {
	return &Map[V]{values: map[string]V{}}
}

// WithCapacity returns an empty Map preallocated for n entries.
func WithCapacity[V any](n int) *Map[V] {
	return &Map[V]{
		keys:   make([]string, 0, n),
		values: make(map[string]V, n),
	}
}

// FromPairs builds a Map from an ordered slice of key/value pairs. If the
// same key appears more than once, the later value overwrites the earlier
// one but the earlier key position in the iteration order is preserved.
func FromPairs[V any](pairs []Pair[V]) *Map[V] {
	m := WithCapacity[V](len(pairs))
	for _, p := range pairs {
		m.Set(p.Key, p.Value)
	}
	return m
}

// Pair is a typed (key, value) tuple returned by Entries.
type Pair[V any] struct {
	Key   string
	Value V
}

// Len returns the number of entries.
func (m *Map[V]) Len() int {
	if m == nil {
		return 0
	}
	return len(m.keys)
}

// Keys returns a snapshot of the keys in insertion order.
func (m *Map[V]) Keys() []string {
	if m == nil {
		return nil
	}
	out := make([]string, len(m.keys))
	copy(out, m.keys)
	return out
}

// Has reports whether the key exists.
func (m *Map[V]) Has(key string) bool {
	if m == nil {
		return false
	}
	_, ok := m.values[key]
	return ok
}

// Get returns the value for key and whether the key was present.
func (m *Map[V]) Get(key string) (V, bool) {
	if m == nil {
		var zero V
		return zero, false
	}
	v, ok := m.values[key]
	return v, ok
}

// GetOrZero returns the value for key, or the zero value when the key is absent.
func (m *Map[V]) GetOrZero(key string) V {
	v, _ := m.Get(key)
	return v
}

// Set assigns value to key, appending the key at the end of iteration order if
// it is new, or updating the value in place if it already exists.
func (m *Map[V]) Set(key string, value V) {
	if m.values == nil {
		m.values = map[string]V{}
	}
	if _, ok := m.values[key]; !ok {
		m.keys = append(m.keys, key)
	}
	m.values[key] = value
}

// Upsert applies update to the existing value for key, or to the zero value
// when the key is missing, and stores the result. It is a small convenience
// helper used by accumulators (cash and position state for example).
func (m *Map[V]) Upsert(key string, update func(current V) V) {
	cur, _ := m.Get(key)
	m.Set(key, update(cur))
}

// Delete removes a key from the map. It is O(n) in the current length because
// the key slice must be compacted.
func (m *Map[V]) Delete(key string) {
	if m == nil {
		return
	}
	if _, ok := m.values[key]; !ok {
		return
	}
	delete(m.values, key)
	for i, k := range m.keys {
		if k == key {
			m.keys = append(m.keys[:i], m.keys[i+1:]...)
			return
		}
	}
}

// Entries returns a stable snapshot of key/value pairs in insertion order.
func (m *Map[V]) Entries() []Pair[V] {
	if m == nil {
		return nil
	}
	out := make([]Pair[V], 0, len(m.keys))
	for _, k := range m.keys {
		out = append(out, Pair[V]{Key: k, Value: m.values[k]})
	}
	return out
}

// ForEach calls fn for every entry in insertion order. Mutating the map
// during iteration is not supported.
func (m *Map[V]) ForEach(fn func(key string, value V)) {
	if m == nil {
		return
	}
	for _, k := range m.keys {
		fn(k, m.values[k])
	}
}

// Clone returns a shallow copy.
func (m *Map[V]) Clone() *Map[V] {
	if m == nil {
		return nil
	}
	out := WithCapacity[V](len(m.keys))
	for _, k := range m.keys {
		out.Set(k, m.values[k])
	}
	return out
}

// MarshalJSON emits a JSON object whose keys appear in insertion order. This
// is the primary reason this type exists.
func (m *Map[V]) MarshalJSON() ([]byte, error) {
	if m == nil || len(m.keys) == 0 {
		return []byte("{}"), nil
	}
	buf := make([]byte, 0, 64+len(m.keys)*16)
	buf = append(buf, '{')
	for i, k := range m.keys {
		if i > 0 {
			buf = append(buf, ',')
		}
		keyBytes, err := json.Marshal(k)
		if err != nil {
			return nil, fmt.Errorf("orderedmap: key %q: %w", k, err)
		}
		valueBytes, err := json.Marshal(m.values[k])
		if err != nil {
			return nil, fmt.Errorf("orderedmap: value for %q: %w", k, err)
		}
		buf = append(buf, keyBytes...)
		buf = append(buf, ':')
		buf = append(buf, valueBytes...)
	}
	buf = append(buf, '}')
	return buf, nil
}

// UnmarshalJSON accepts any JSON object and records its keys in the order
// they appear in the source stream. This lets round-trip tests compare
// ordering stability.
func (m *Map[V]) UnmarshalJSON(data []byte) error {
	dec := json.NewDecoder(bytesReader(data))
	tok, err := dec.Token()
	if err != nil {
		return err
	}
	if delim, ok := tok.(json.Delim); !ok || delim != '{' {
		// Treat `null` as an empty map, mirroring the Rust serde behavior.
		if tok == nil {
			m.keys = nil
			m.values = map[string]V{}
			return nil
		}
		return fmt.Errorf("orderedmap: expected object, got %v", tok)
	}
	m.keys = m.keys[:0]
	if m.values == nil {
		m.values = map[string]V{}
	} else {
		for k := range m.values {
			delete(m.values, k)
		}
	}
	for dec.More() {
		tok, err := dec.Token()
		if err != nil {
			return err
		}
		key, ok := tok.(string)
		if !ok {
			return fmt.Errorf("orderedmap: non-string key %v", tok)
		}
		var value V
		if err := dec.Decode(&value); err != nil {
			return fmt.Errorf("orderedmap: value for %q: %w", key, err)
		}
		m.Set(key, value)
	}
	if _, err := dec.Token(); err != nil {
		return err
	}
	return nil
}
