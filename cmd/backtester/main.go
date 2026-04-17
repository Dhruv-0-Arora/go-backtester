// Command backtester is the Go port of the IMC Prosperity Rust backtester.
//
// The executable orchestrates Python trader processes against recorded IMC
// market data. Details and supported flags are documented in the internal
// `cli` package; this file only forwards control and maps errors to a
// non-zero exit code.
package main

import (
	"fmt"
	"os"

	"github.com/darora1/imc-prosperity-4/backtester/internal/cli"
)

func main() {
	if err := cli.Run(); err != nil {
		fmt.Fprintf(os.Stderr, "backtester: %v\n", err)
		os.Exit(1)
	}
}
