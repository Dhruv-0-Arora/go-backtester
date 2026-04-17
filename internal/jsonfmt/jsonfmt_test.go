package jsonfmt

import (
	"math"
	"testing"
)

func TestRoundHalfToEven(t *testing.T) {
	tests := []struct {
		in   float64
		want float64
	}{
		{0.5, 0},
		{1.5, 2},
		{2.5, 2},
		{-0.5, 0},
		{-1.5, -2},
		{0.49, 0},
		{2.51, 3},
	}
	for _, tt := range tests {
		if got := RoundHalfToEven(tt.in); got != tt.want {
			t.Errorf("RoundHalfToEven(%v)=%v want %v", tt.in, got, tt.want)
		}
	}
}

func TestRoundToDigits(t *testing.T) {
	if got := RoundToDigits(1.2345, 2); got != 1.23 {
		t.Errorf("expected 1.23 got %v", got)
	}
	if got := RoundToDigits(1.235, 2); got != 1.24 {
		t.Errorf("expected banker's 1.24 got %v", got)
	}
}

func TestPythonFloatString(t *testing.T) {
	if got := PythonFloatString(1.0); got != "1.0" {
		t.Errorf("expected '1.0', got %q", got)
	}
	// Go constant-folds `-0.0` to `0.0`; build negative zero from bits instead.
	negZero := math.Copysign(0, -1)
	if got := PythonFloatString(negZero); got != "-0.0" {
		t.Errorf("expected '-0.0', got %q", got)
	}
	if got := PythonFloatString(1.5); got != "1.5" {
		t.Errorf("expected '1.5', got %q", got)
	}
	if got := PythonFloatString(123456789.0); got != "123456789.0" {
		t.Errorf("expected '123456789.0', got %q", got)
	}
}
