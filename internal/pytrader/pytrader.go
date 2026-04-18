// Package pytrader hosts a long-lived Python subprocess that executes the
// user's Trader.run(state) implementation for every tick. The subprocess is
// started once per backtest and kept alive until Close is called.
//
// Protocol:
//
//   - Request and response are single-line JSON documents.
//   - The first request is {"cmd":"load", "trader_file":..., "workspace_root":...}.
//   - Each subsequent tick sends the serialized TradingState payload and
//     expects back the list of orders, conversions, traderData, and captured
//     stdout.
//
// Using a subprocess instead of CPython FFI keeps the Go binary self-contained
// (no CGO, no libpython), portable across distributions, and trivially safe:
// if a trader panics it cannot corrupt the runner's address space.
package pytrader

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"sync"

	"github.com/darora1/imc-prosperity-4/backtester/internal/model"
	"github.com/darora1/imc-prosperity-4/backtester/internal/orderedmap"
)

// Invocation is the typed request passed to Trader.RunTick. The runner always
// hands in references the Python side will read but not mutate.
type Invocation struct {
	TraderData       string
	Tick             *model.TickSnapshot
	OwnTradesPrev    *orderedmap.Map[[]model.Trade]
	MarketTradesPrev *orderedmap.Map[[]model.Trade]
	Position         *orderedmap.Map[int64]
}

// RunResult is what the trader returned for a single tick. Stdout is the
// trader's captured stdout (already trimmed for length by the caller).
type RunResult struct {
	OrdersBySymbol *orderedmap.Map[[]model.Order]
	Conversions    int64
	TraderData     string
	Stdout         string
}

// Trader is a handle to a running Python worker.
type Trader struct {
	mu       sync.Mutex
	cmd      *exec.Cmd
	stdin    io.WriteCloser
	stdout   *bufio.Reader
	stderr   io.ReadCloser
	scratch  string
	stderrBuf []byte
	stderrWg sync.WaitGroup
}

// New starts the Python worker, sets sys.path, loads the trader module, and
// returns a ready handle. workspaceRoot is typically the dataset workspace
// and is added to sys.path so traders can import neighbouring modules
// (mirroring the Rust PyO3 implementation).
func New(ctx context.Context, workspaceRoot, traderFile string) (*Trader, error) {
	pythonBin := os.Getenv("BACKTESTER_PYTHON")
	if pythonBin == "" {
		pythonBin = "python3"
	}
	if _, err := exec.LookPath(pythonBin); err != nil {
		return nil, fmt.Errorf("python interpreter not found (%s); set BACKTESTER_PYTHON to override", pythonBin)
	}

	// Write the embedded worker to a scratch file. Using a file rather than
	// piping source via `-c` keeps tracebacks readable when a trader raises.
	scratchDir, err := os.MkdirTemp("", "go-backtester-pyworker-")
	if err != nil {
		return nil, fmt.Errorf("failed to create pyworker scratch dir: %w", err)
	}
	workerPath := filepath.Join(scratchDir, "worker.py")
	if err := os.WriteFile(workerPath, []byte(workerSource), 0o600); err != nil {
		os.RemoveAll(scratchDir)
		return nil, fmt.Errorf("failed to write pyworker: %w", err)
	}

	cmd := exec.CommandContext(ctx, pythonBin, "-u", workerPath)
	cmd.Env = append(os.Environ(), "PYTHONIOENCODING=utf-8", "PYTHONDONTWRITEBYTECODE=1")

	stdin, err := cmd.StdinPipe()
	if err != nil {
		os.RemoveAll(scratchDir)
		return nil, err
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		os.RemoveAll(scratchDir)
		return nil, err
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		os.RemoveAll(scratchDir)
		return nil, err
	}
	if err := cmd.Start(); err != nil {
		os.RemoveAll(scratchDir)
		return nil, fmt.Errorf("failed to start python worker: %w", err)
	}

	t := &Trader{
		cmd:     cmd,
		stdin:   stdin,
		stdout:  bufio.NewReaderSize(stdout, 1<<16),
		stderr:  stderr,
		scratch: scratchDir,
	}
	// Drain stderr in the background so large tracebacks do not deadlock the
	// worker. The collected bytes are included in error messages when the
	// protocol breaks.
	t.stderrWg.Add(1)
	go func() {
		defer t.stderrWg.Done()
		data, _ := io.ReadAll(t.stderr)
		t.mu.Lock()
		t.stderrBuf = append(t.stderrBuf, data...)
		t.mu.Unlock()
	}()

	load := map[string]any{
		"cmd":             "load",
		"trader_file":     traderFile,
		"workspace_root":  workspaceRoot,
	}
	if _, err := t.roundTrip(load); err != nil {
		t.Close()
		return nil, err
	}
	return t, nil
}

// Close shuts down the worker cleanly. It is safe to call more than once.
// The mutex is only held long enough to take the handles away from
// concurrent writers; the stderr drain goroutine also needs the mutex to
// append to stderrBuf, so we release our lock before Wait() to avoid a
// classic rendezvous deadlock.
func (t *Trader) Close() error {
	t.mu.Lock()
	if t.cmd == nil {
		t.mu.Unlock()
		return nil
	}
	cmd := t.cmd
	stdin := t.stdin
	scratch := t.scratch
	t.cmd = nil
	t.stdin = nil
	t.scratch = ""
	t.mu.Unlock()

	if stdin != nil {
		_, _ = fmt.Fprintln(stdin, `{"cmd":"exit"}`)
		_ = stdin.Close()
	}
	err := cmd.Wait()
	t.stderrWg.Wait()
	if scratch != "" {
		_ = os.RemoveAll(scratch)
	}
	if err != nil {
		// A dying worker during Close is only fatal when we never received
		// a successful response; callers that already got their result
		// shouldn't care.
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) {
			return nil
		}
		return err
	}
	return nil
}

// RunTick sends the serialized TradingState payload to the worker and returns
// the trader's output. It is safe to call from a single goroutine only.
func (t *Trader) RunTick(inv Invocation) (*RunResult, error) {
	msg := buildTickMessage(inv)
	raw, err := t.roundTrip(msg)
	if err != nil {
		return nil, err
	}
	return parseTickResponse(raw)
}

// roundTrip writes one JSON request and reads one JSON response.
func (t *Trader) roundTrip(msg map[string]any) ([]byte, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.cmd == nil {
		return nil, errors.New("python worker is not running")
	}
	encoded, err := json.Marshal(msg)
	if err != nil {
		return nil, fmt.Errorf("encode worker request: %w", err)
	}
	encoded = append(encoded, '\n')
	if _, err := t.stdin.Write(encoded); err != nil {
		return nil, fmt.Errorf("write to worker: %w (stderr: %s)", err, t.stderrSnapshot())
	}
	line, err := t.stdout.ReadBytes('\n')
	if err != nil {
		return nil, fmt.Errorf("read from worker: %w (stderr: %s)", err, t.stderrSnapshot())
	}
	var envelope struct {
		OK    bool   `json:"ok"`
		Error string `json:"error,omitempty"`
	}
	if err := json.Unmarshal(line, &envelope); err != nil {
		return nil, fmt.Errorf("worker returned malformed JSON: %w (raw: %s)", err, truncate(line, 500))
	}
	if !envelope.OK {
		return nil, fmt.Errorf("python trader error: %s", envelope.Error)
	}
	return line, nil
}

func (t *Trader) stderrSnapshot() string {
	// Caller holds t.mu.
	if len(t.stderrBuf) == 0 {
		return "<empty>"
	}
	return truncate(t.stderrBuf, 1024)
}

func truncate(b []byte, max int) string {
	if len(b) <= max {
		return string(b)
	}
	return string(b[:max]) + "..."
}

// buildTickMessage serializes the invocation into the worker JSON schema.
// Arrays are used instead of objects wherever an ordering must be preserved.
func buildTickMessage(inv Invocation) map[string]any {
	products := inv.Tick.Products.Keys()
	orderDepths := map[string][2][][2]int64{}
	inv.Tick.Products.ForEach(func(symbol string, snapshot *model.ProductSnapshot) {
		buy := make([][2]int64, 0, len(snapshot.Bids))
		for _, level := range snapshot.Bids {
			buy = append(buy, [2]int64{level.Price, level.Volume})
		}
		sell := make([][2]int64, 0, len(snapshot.Asks))
		for _, level := range snapshot.Asks {
			// The Python OrderDepth expects negative sell volumes (IMC convention).
			sell = append(sell, [2]int64{level.Price, -level.Volume})
		}
		orderDepths[symbol] = [2][][2]int64{buy, sell}
	})

	ownTrades := tradeMapToPayload(inv.OwnTradesPrev)
	marketTrades := tradeMapToPayload(inv.MarketTradesPrev)

	positionPairs := make([][2]any, 0, inv.Position.Len())
	inv.Position.ForEach(func(k string, v int64) {
		positionPairs = append(positionPairs, [2]any{k, v})
	})

	plainPairs := make([][2]any, 0)
	if inv.Tick.Observations.Plain != nil {
		inv.Tick.Observations.Plain.ForEach(func(k string, v int64) {
			plainPairs = append(plainPairs, [2]any{k, v})
		})
	}

	// Conversion observations are sent as positional tuples keyed by product.
	convObs := map[string][7]float64{}
	if inv.Tick.Observations.Conversion != nil {
		inv.Tick.Observations.Conversion.ForEach(func(product string, values *orderedmap.Map[float64]) {
			get := func(k string) float64 {
				v, _ := values.Get(k)
				return v
			}
			convObs[product] = [7]float64{
				get("bidPrice"), get("askPrice"), get("transportFees"),
				get("exportTariff"), get("importTariff"), get("sugarPrice"),
				get("sunlightIndex"),
			}
		})
	}

	return map[string]any{
		"cmd":              "tick",
		"trader_data":      inv.TraderData,
		"timestamp":        inv.Tick.Timestamp,
		"listing_symbols":  products,
		"order_depths":     orderDepths,
		"own_trades":       ownTrades,
		"market_trades":    marketTrades,
		"position":         positionPairs,
		"plain_obs":        plainPairs,
		"conversion_obs":   convObs,
	}
}

func tradeMapToPayload(m *orderedmap.Map[[]model.Trade]) map[string][][6]any {
	out := map[string][][6]any{}
	if m == nil {
		return out
	}
	m.ForEach(func(symbol string, rows []model.Trade) {
		serialized := make([][6]any, 0, len(rows))
		for _, t := range rows {
			serialized = append(serialized, [6]any{
				t.Symbol, t.Price, t.Quantity, t.Buyer, t.Seller, t.Timestamp,
			})
		}
		out[symbol] = serialized
	})
	return out
}

func parseTickResponse(raw []byte) (*RunResult, error) {
	var resp struct {
		OK          bool                 `json:"ok"`
		Error       string               `json:"error"`
		Orders      map[string][][3]any  `json:"orders"`
		Conversions int64                `json:"conversions"`
		TraderData  string               `json:"trader_data"`
		Stdout      string               `json:"stdout"`
	}
	if err := json.Unmarshal(raw, &resp); err != nil {
		return nil, fmt.Errorf("decode worker tick response: %w", err)
	}
	if !resp.OK {
		return nil, fmt.Errorf("python trader error: %s", resp.Error)
	}
	orders := orderedmap.New[[]model.Order]()
	// JSON objects do not guarantee key order through encoding/json; to get a
	// deterministic order we re-parse using a streaming decoder that preserves
	// the source ordering.
	if len(resp.Orders) > 0 {
		keys, err := jsonObjectKeyOrder(raw, "orders")
		if err != nil {
			return nil, err
		}
		for _, key := range keys {
			rows := resp.Orders[key]
			product := make([]model.Order, 0, len(rows))
			for _, row := range rows {
				symbol, _ := row[0].(string)
				priceF, _ := toFloat64(row[1])
				qtyF, _ := toFloat64(row[2])
				product = append(product, model.Order{
					Symbol:   symbol,
					Price:    int64(priceF),
					Quantity: int64(qtyF),
				})
			}
			orders.Set(key, product)
		}
	}
	return &RunResult{
		OrdersBySymbol: orders,
		Conversions:    resp.Conversions,
		TraderData:     resp.TraderData,
		Stdout:         resp.Stdout,
	}, nil
}

func toFloat64(v any) (float64, bool) {
	switch x := v.(type) {
	case float64:
		return x, true
	case json.Number:
		f, err := x.Float64()
		return f, err == nil
	default:
		return 0, false
	}
}
