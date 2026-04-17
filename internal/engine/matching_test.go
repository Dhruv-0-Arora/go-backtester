package engine

import (
	"encoding/json"
	"testing"

	"github.com/darora1/imc-prosperity-4/backtester/internal/model"
	"github.com/darora1/imc-prosperity-4/backtester/internal/orderedmap"
)

// newBookLevels is a tiny helper for building BookLevel slices inline.
func newBookLevels(entries ...[2]int64) []BookLevel {
	out := make([]BookLevel, 0, len(entries))
	for _, e := range entries {
		out = append(out, BookLevel{Price: e[0], Volume: e[1]})
	}
	return out
}

func TestMatchExecutedTradeHistory_BuyFillsAskBook(t *testing.T) {
	position := orderedmap.New[int64]()
	cash := orderedmap.New[float64]()
	asks := newBookLevels([2]int64{100, 5}, [2]int64{101, 5})
	orders := []model.Order{{Symbol: "FOO", Price: 101, Quantity: 7}}

	out := MatchExecutedTradeHistory(
		"FOO", orders, nil, asks, nil, position, cash,
		1000, model.DefaultMatchingConfig(), false,
	)

	if len(out.OwnTrades) != 2 {
		t.Fatalf("expected 2 own trades, got %d", len(out.OwnTrades))
	}
	total := int64(0)
	for _, tr := range out.OwnTrades {
		if tr.Buyer != "SUBMISSION" {
			t.Fatalf("expected buyer=SUBMISSION, got %q", tr.Buyer)
		}
		total += tr.Quantity
	}
	if total != 7 {
		t.Fatalf("expected total fill 7, got %d", total)
	}
	if got := position.GetOrZero("FOO"); got != 7 {
		t.Fatalf("expected position +7, got %d", got)
	}
	// 5@100 + 2@101 = -702
	if got := cash.GetOrZero("FOO"); got != -702 {
		t.Fatalf("expected cash -702, got %v", got)
	}
}

func TestMatchExecutedTradeHistory_SellFillsBidBook(t *testing.T) {
	position := orderedmap.New[int64]()
	cash := orderedmap.New[float64]()
	bids := newBookLevels([2]int64{99, 3}, [2]int64{98, 4})
	orders := []model.Order{{Symbol: "FOO", Price: 98, Quantity: -5}}

	out := MatchExecutedTradeHistory(
		"FOO", orders, bids, nil, nil, position, cash,
		5000, model.DefaultMatchingConfig(), false,
	)

	if len(out.OwnTrades) != 2 {
		t.Fatalf("expected 2 own trades, got %d", len(out.OwnTrades))
	}
	if got := position.GetOrZero("FOO"); got != -5 {
		t.Fatalf("expected position -5, got %d", got)
	}
	// 3@99 + 2@98 = +493
	if got := cash.GetOrZero("FOO"); got != 493 {
		t.Fatalf("expected cash 493, got %v", got)
	}
}

func TestMatchExecutedTradeHistory_RespectsLimitPrice(t *testing.T) {
	position := orderedmap.New[int64]()
	cash := orderedmap.New[float64]()
	asks := newBookLevels([2]int64{105, 10})
	orders := []model.Order{{Symbol: "BAR", Price: 104, Quantity: 5}}

	out := MatchExecutedTradeHistory(
		"BAR", orders, nil, asks, nil, position, cash,
		0, model.DefaultMatchingConfig(), false,
	)

	if len(out.OwnTrades) != 0 {
		t.Fatalf("expected no fills above limit, got %d", len(out.OwnTrades))
	}
	if position.GetOrZero("BAR") != 0 {
		t.Fatalf("position should stay flat when no fill occurs")
	}
}

func TestMatchExecutedTradeHistory_TapeMatchingAll(t *testing.T) {
	position := orderedmap.New[int64]()
	cash := orderedmap.New[float64]()
	tape := []model.MarketTrade{
		{Symbol: "FOO", Price: 100, Quantity: 3, Buyer: "A", Seller: "B", Timestamp: 10},
	}
	orders := []model.Order{{Symbol: "FOO", Price: 100, Quantity: 5}}

	out := MatchExecutedTradeHistory(
		"FOO", orders, nil, nil, tape, position, cash,
		10, model.DefaultMatchingConfig(), false,
	)

	if len(out.OwnTrades) != 1 {
		t.Fatalf("expected 1 own trade from tape, got %d", len(out.OwnTrades))
	}
	if out.OwnTrades[0].Quantity != 3 {
		t.Fatalf("expected fill 3 from tape, got %d", out.OwnTrades[0].Quantity)
	}
}

func TestMatchExecutedTradeHistory_TapeMatchingNone(t *testing.T) {
	position := orderedmap.New[int64]()
	cash := orderedmap.New[float64]()
	tape := []model.MarketTrade{
		{Symbol: "FOO", Price: 100, Quantity: 3, Buyer: "A", Seller: "B", Timestamp: 10},
	}
	orders := []model.Order{{Symbol: "FOO", Price: 100, Quantity: 5}}
	cfg := model.DefaultMatchingConfig()
	cfg.TradeMatchMode = "none"

	out := MatchExecutedTradeHistory(
		"FOO", orders, nil, nil, tape, position, cash, 10, cfg, false,
	)

	if len(out.OwnTrades) != 0 {
		t.Fatalf("expected no fills in mode=none, got %d", len(out.OwnTrades))
	}
}

func TestEnforcePositionLimits_CancelsWhenExceeding(t *testing.T) {
	position := orderedmap.New[int64]()
	position.Set("RAINFOREST_RESIN", 48) // limit 50

	orders := orderedmap.New[[]model.Order]()
	orders.Set("RAINFOREST_RESIN", []model.Order{{Symbol: "RAINFOREST_RESIN", Price: 10, Quantity: 5}})
	orders.Set("KELP", []model.Order{{Symbol: "KELP", Price: 10, Quantity: 5}})

	filtered, messages := EnforcePositionLimits(position, orders)
	if filtered.Has("RAINFOREST_RESIN") {
		t.Fatalf("expected RAINFOREST_RESIN orders to be cancelled")
	}
	if !filtered.Has("KELP") {
		t.Fatalf("expected KELP orders to survive")
	}
	if len(messages) != 1 {
		t.Fatalf("expected 1 limit message, got %d", len(messages))
	}
}

func TestEnforcePositionLimits_AllowsWhenWithinLimit(t *testing.T) {
	position := orderedmap.New[int64]()
	position.Set("RAINFOREST_RESIN", 40) // limit 50, room for +10

	orders := orderedmap.New[[]model.Order]()
	orders.Set("RAINFOREST_RESIN", []model.Order{{Symbol: "RAINFOREST_RESIN", Price: 10, Quantity: 5}})

	filtered, messages := EnforcePositionLimits(position, orders)
	if !filtered.Has("RAINFOREST_RESIN") {
		t.Fatalf("expected orders to be kept")
	}
	if len(messages) != 0 {
		t.Fatalf("expected no messages, got %v", messages)
	}
}

func TestPositionLimitFor_Defaults(t *testing.T) {
	tests := []struct {
		symbol string
		want   int64
	}{
		{"KELP", 50},
		{"CROISSANTS", 250},
		{"UNKNOWN", DefaultPositionLimit},
		{"PICNIC_BASKET1", 60},
		{"VOLCANIC_ROCK_VOUCHER_10000", 200},
		{"MAGNIFICENT_MACARONS", 75},
	}
	for _, tt := range tests {
		if got := PositionLimitFor(tt.symbol); got != tt.want {
			t.Errorf("PositionLimitFor(%q) = %d, want %d", tt.symbol, got, tt.want)
		}
	}
}

func TestSnapshotToBook_SortsBidsDescending(t *testing.T) {
	snap := &model.ProductSnapshot{
		Bids: []model.OrderBookLevel{
			{Price: 98, Volume: 2},
			{Price: 100, Volume: 5},
			{Price: 99, Volume: 3},
		},
	}
	bids := SnapshotToBook(snap, true)
	if len(bids) != 3 {
		t.Fatalf("expected 3 bids, got %d", len(bids))
	}
	if bids[0].Price != 100 || bids[1].Price != 99 || bids[2].Price != 98 {
		t.Fatalf("bids not sorted descending: %v", bids)
	}
}

func TestSnapshotToBook_SortsAsksAscending(t *testing.T) {
	snap := &model.ProductSnapshot{
		Asks: []model.OrderBookLevel{
			{Price: 105, Volume: 2},
			{Price: 100, Volume: 5},
			{Price: 103, Volume: 3},
		},
	}
	asks := SnapshotToBook(snap, false)
	if asks[0].Price != 100 || asks[1].Price != 103 || asks[2].Price != 105 {
		t.Fatalf("asks not sorted ascending: %v", asks)
	}
}

func TestSlippageAdjustedPrice(t *testing.T) {
	if got := slippageAdjustedPrice(100, true, 0); got != 100 {
		t.Fatalf("expected no-op for 0bps, got %d", got)
	}
	// 10bps on 10_000 = 10 → buy becomes 10_010.
	if got := slippageAdjustedPrice(10_000, true, 10); got != 10_010 {
		t.Fatalf("expected 10_010 for buy with 10bps, got %d", got)
	}
	// 10bps on 10_000 sell = 10000 / 1.001 ≈ 9990.0099.. → rounds to 9990.
	if got := slippageAdjustedPrice(10_000, false, 10); got != 9990 {
		t.Fatalf("expected 9990 for sell with 10bps, got %d", got)
	}
}

func TestQueuePenetrationAvailable(t *testing.T) {
	if got := queuePenetrationAvailable(10, 1.0); got != 10 {
		t.Fatalf("expected full quantity, got %d", got)
	}
	if got := queuePenetrationAvailable(10, 0.5); got != 5 {
		t.Fatalf("expected half quantity, got %d", got)
	}
	// Positive tape quantity with tiny penetration should floor at 1.
	if got := queuePenetrationAvailable(2, 0.1); got != 1 {
		t.Fatalf("expected floor-of-1, got %d", got)
	}
	// Zero penetration should disable.
	if got := queuePenetrationAvailable(10, 0); got != 0 {
		t.Fatalf("expected 0, got %d", got)
	}
}

func TestInferReplayMode(t *testing.T) {
	ds := &model.NormalizedDataset{}
	if InferReplayMode(ds) != MarketReplayExecutedTradeHistory {
		t.Fatalf("expected executed-trade-history for empty metadata")
	}
	ds.Metadata = orderedmap.New[json.RawMessage]()
	ds.Metadata.Set("source_format", json.RawMessage(`"imc_csv"`))
	if InferReplayMode(ds) != MarketReplayRawCSVTape {
		t.Fatalf("expected raw-csv-tape when metadata marks imc_csv")
	}
}
