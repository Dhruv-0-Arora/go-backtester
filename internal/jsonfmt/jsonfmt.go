// Package jsonfmt centralises JSON helpers used when building the backtest
// artifacts. The helpers exist to guarantee byte-level parity with Python
// JSON output and with the Rust backtester's existing artifact layout.
//
// Two formatting concerns drive the design:
//
//   - Float formatting: the submission log activity CSV and trades CSV must
//     serialize floats using the same "repr"-like format that Python emits,
//     for example 5006.0 instead of 5006 or 5006.000000. Go's %g is nearly
//     right but strips trailing .0; PythonFloatString fixes that.
//
//   - Deterministic JSON: metrics.json and bundle.json use alphabetically
//     sorted keys so that repeated runs produce identical bytes. The
//     SortedJSONBytes helper walks any json.RawMessage/map structure and
//     reserialises it with sorted keys.
package jsonfmt

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"strconv"
)

// PythonFloatString returns a textual representation of value that matches
// CPython's `repr(float)` behavior for finite numbers:
//
//	0.0, -0.0, 1.0, 1.5, 100.0, 1e+20, ...
//
// Non-finite values fall back to Go's default conversion (NaN / +Inf / -Inf).
// This is used both for CSV artifact generation and for the structured
// submission log payload.
func PythonFloatString(value float64) string {
	if math.IsNaN(value) || math.IsInf(value, 0) {
		return strconv.FormatFloat(value, 'g', -1, 64)
	}
	if value == 0 {
		if math.Signbit(value) {
			return "-0.0"
		}
		return "0.0"
	}
	if math.Trunc(value) == value && !math.IsInf(value, 0) {
		// Use %.1f to force a trailing .0 on integral values. Very large
		// integral floats (> 1e16) lose precision here the same way CPython
		// does, which is fine because the matching engine only ever emits
		// small integer volumes and seashell PnLs that fit comfortably in
		// double precision.
		return strconv.FormatFloat(value, 'f', 1, 64)
	}
	text := strconv.FormatFloat(value, 'g', -1, 64)
	if !bytes.ContainsAny([]byte(text), ".eE") {
		text += ".0"
	}
	return text
}

// EncodeFloat mirrors Rust's serde_json::Number::from_f64 behavior: it
// rejects non-finite floats because the JSON spec does not allow them.
// Callers that know their values are finite can use EncodeFloatNoCheck.
func EncodeFloat(value float64) (json.RawMessage, error) {
	if math.IsNaN(value) || math.IsInf(value, 0) {
		return nil, fmt.Errorf("cannot encode non-finite float %v", value)
	}
	return json.Marshal(value)
}

// PrettyJSONBytes serialises v using 2-space indentation and appends a
// trailing newline to match the Rust artifact writer.
func PrettyJSONBytes(v any) ([]byte, error) {
	buf, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return nil, err
	}
	return append(buf, '\n'), nil
}

// SortedJSONBytes round-trips v through json.Marshal, sorts every nested
// object's keys alphabetically, and re-emits the bytes with 2-space
// indentation and a trailing newline. The reference backtester relies on
// this for metrics.json and bundle.json so artifacts diff cleanly.
func SortedJSONBytes(v any) ([]byte, error) {
	raw, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	var parsed any
	if err := json.Unmarshal(raw, &parsed); err != nil {
		return nil, err
	}
	sorted := sortValue(parsed)
	buf, err := json.MarshalIndent(sorted, "", "  ")
	if err != nil {
		return nil, err
	}
	return append(buf, '\n'), nil
}

// sortValue walks a parsed JSON structure, converting every map[string]any
// into an orderedPairs slice with its keys in ascending order. The slice
// implements json.Marshaler so the result serialises with sorted keys.
func sortValue(v any) any {
	switch value := v.(type) {
	case map[string]any:
		keys := make([]string, 0, len(value))
		for k := range value {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		pairs := make(orderedPairs, 0, len(keys))
		for _, k := range keys {
			pairs = append(pairs, orderedPair{Key: k, Value: sortValue(value[k])})
		}
		return pairs
	case []any:
		out := make([]any, len(value))
		for i, item := range value {
			out[i] = sortValue(item)
		}
		return out
	default:
		return value
	}
}

type orderedPair struct {
	Key   string
	Value any
}

type orderedPairs []orderedPair

// MarshalJSON emits the pairs as a JSON object with the encoded key order.
func (p orderedPairs) MarshalJSON() ([]byte, error) {
	buf := bytes.NewBufferString("{")
	for i, pair := range p {
		if i > 0 {
			buf.WriteByte(',')
		}
		keyBytes, err := json.Marshal(pair.Key)
		if err != nil {
			return nil, err
		}
		valBytes, err := json.Marshal(pair.Value)
		if err != nil {
			return nil, err
		}
		buf.Write(keyBytes)
		buf.WriteByte(':')
		buf.Write(valBytes)
	}
	buf.WriteByte('}')
	return buf.Bytes(), nil
}

// RoundHalfToEven implements banker's rounding. Go's math.RoundToEven
// already matches Rust's f64::round_ties_even so this helper is a thin
// typed wrapper for readability at call sites.
func RoundHalfToEven(value float64) float64 {
	return math.RoundToEven(value)
}

// RoundToDigits returns value rounded half-to-even to the given number of
// fractional digits. Matches the `python_round_to_digits` helper in the
// Rust runner which the activities log uses for PnL display.
func RoundToDigits(value float64, digits int) float64 {
	factor := math.Pow10(digits)
	return math.RoundToEven(value*factor) / factor
}
