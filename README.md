# IMC Prosperity Go Backtester

A Go port of the Rust `prosperity_rust_backtester`. The runner replays IMC
Prosperity data through a Python trader and produces matching metrics,
submission logs, and CSV artifacts. Output is intentionally byte-compatible
with the Rust reference so diffs stay clean.

## Requirements

- Go 1.22+
- `python3` on `PATH` (override with `BACKTESTER_PYTHON`)
- A trader file that exposes a `class Trader` with a `run(state)` method

No CGO/FFI: the Python trader is driven through a long-lived subprocess that
speaks newline-delimited JSON over stdin/stdout. If a trader crashes, only
the worker process goes down.

## Build & test

```bash
cd backtester
go build ./...
go test ./...
```

A release-mode binary can be produced with:

```bash
go build -o bin/backtester ./cmd/backtester
```

## Run

```bash
# Latest populated round with artifact mode = submission log only (default)
./bin/backtester --trader traders/latest_trader.py

# Named dataset aliases (same names the Rust CLI accepts)
./bin/backtester --dataset tutorial --carry

# A single IMC prices CSV
./bin/backtester --dataset datasets/tutorial/prices_round_0_day_-1.csv --day -1

# An IMC submission log (treated as an already-executed tape)
./bin/backtester --dataset datasets/tutorial/submission.log --artifact-mode full
```

Flags:

| flag | description |
|---|---|
| `--trader <file.py>` | Python trader file. Auto-picked from `scripts/`, `traders/submissions/`, `traders/` when omitted. |
| `--dataset <path-or-alias>` | Dataset file/directory, or alias (`tutorial`, `round1`..`round8`, `latest`, `submission`, `<round>-submission`, `tutorial-1`, `tut-d-2`, …) |
| `--day <N\|all>` | Pick one day out of a multi-day dataset (default: all days). |
| `--run-id <string>` | Deterministic run ID; defaults to `backtest-<unix-ms>`. |
| `--trade-match-mode all\|worse\|none` | Tape matching aggressiveness (default `all`). |
| `--queue-penetration <float>` | Scale tape quantities before matching (default 1.0). |
| `--price-slippage-bps <float>` | bps slippage against the trader on every own fill (default 0). |
| `--output-root <dir>` | Where per-run artifact directories are written (default `./runs`). |
| `--persist` | Shortcut for `--artifact-mode full`. |
| `--artifact-mode none\|submission\|diagnostic\|full` | Controls which files are materialised. |
| `--carry` | Merge consecutive day datasets from the same round into one state-carrying run. |
| `--flat` | Multi-run outputs are placed in a single flat directory with filename prefixes, plus a `manifest.json`. |
| `--products off\|summary\|full` | Control the per-product PnL table in the CLI summary (default `summary`). |

## Package layout

```
cmd/backtester/          main entry point; forwards to internal/cli
internal/cli/            flag parsing, dataset/trader resolution, summary
internal/dataset/        IMC CSV + submission-log loaders → NormalizedDataset
internal/engine/         matching engine (trade-history and raw-csv-tape modes) and runner
internal/model/          shared data structures with JSON parity to the Rust crate
internal/jsonfmt/        Python-style float formatting + deterministic JSON
internal/orderedmap/     generic insertion-ordered map for reproducible JSON
internal/pytrader/       Python subprocess worker and its Go client
```

## Design notes

### Python trader integration

The runner starts a single Python subprocess per backtest and sends
serialised `TradingState` messages one per tick. See `internal/pytrader`
for the protocol. The embedded worker script is written to a temporary
file on startup so exception tracebacks point at real line numbers.

### Deterministic output

`internal/orderedmap` keeps insertion order on top of Go's random-iteration
map so artifact JSON (e.g. `metrics.final_pnl_by_product`) is reproducible.
`internal/jsonfmt.SortedJSONBytes` round-trips any value through a key-sorted
renderer to match the Rust writer byte-for-byte.

### Numeric fidelity

All price math stays in `int64`. Cash is `float64`. Rounding uses
`math.RoundToEven` which matches Rust's `f64::round_ties_even`, and
`jsonfmt.PythonFloatString` keeps CSV floats visually identical to CPython.

## Smoke tests

The `scripts/` folder is not required; a minimal check against the bundled
tutorial data is:

```bash
./bin/backtester --trader ../prosperity_rust_backtester/traders/latest_trader.py \
  --dataset ../prosperity_rust_backtester/datasets/tutorial \
  --artifact-mode none
```

which should produce per-day PnL around the same magnitudes the Rust
reference emits for the same dataset/trader pair.
