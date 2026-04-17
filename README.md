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
