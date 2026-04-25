// Package engine contains the matching and runner logic for the IMC
// Prosperity backtester. It is the Go counterpart of the Rust `runner` module.
//
// Two matching modes are supported and selected based on dataset metadata:
//
//   - MarketReplayExecutedTradeHistory: the default for submission logs and
//     normalized datasets. Market trades on the tape are treated as already
//     executed fills; own orders first sweep visible book levels and then
//     opportunistically match against the remaining tape.
//
//   - MarketReplayRawCSVTape: used when the dataset comes from raw IMC CSVs.
//     In this mode every tape trade is replayed as a synthetic order that
//     sweeps both sides of the book, giving resting submission orders a chance
//     to fill against the tape.
//
// Position and cash bookkeeping is done in integers (price) and doubles
// (cash), matching the reference implementation.
package engine

import (
	"github.com/darora1/imc-prosperity-4/backtester/internal/jsonfmt"
	"github.com/darora1/imc-prosperity-4/backtester/internal/model"
	"github.com/darora1/imc-prosperity-4/backtester/internal/orderedmap"
)

// DefaultPositionLimit is the cap applied to any product not in
// PositionLimitFor. It matches the Rust default.
const DefaultPositionLimit int64 = 100

// BookLevel is an interior, mutable order-book level used by the matcher.
type BookLevel struct {
	Price  int64
	Volume int64
}

// restingSubmissionOrder tracks the portion of an own order that did not fill
// immediately against visible book liquidity. It is only used in raw-CSV mode.
type restingSubmissionOrder struct {
	Price    int64
	Quantity int64
}

// FlatOrderRow is a flat representation of a trader-submitted order, useful
// for materialized artifacts (bundle.json, activity traces).
type FlatOrderRow struct {
	Symbol   string
	Price    int64
	Quantity int64
}

// MarketReplayMode is inferred from dataset metadata.
type MarketReplayMode int

const (
	MarketReplayExecutedTradeHistory MarketReplayMode = iota
	MarketReplayRawCSVTape
)

// InferReplayMode mirrors runner.market_replay_mode in the Rust codebase.
func InferReplayMode(ds *model.NormalizedDataset) MarketReplayMode {
	if ds == nil || ds.Metadata == nil {
		return MarketReplayExecutedTradeHistory
	}
	raw, ok := ds.Metadata.Get("source_format")
	if !ok {
		return MarketReplayExecutedTradeHistory
	}
	var format string
	if len(raw) > 0 {
		_ = jsonUnmarshalString(raw, &format)
	}
	if format == "imc_csv" {
		return MarketReplayRawCSVTape
	}
	return MarketReplayExecutedTradeHistory
}

// PositionLimitFor returns the per-product position limit. Symbols not in
// the table fall back to DefaultPositionLimit.
func PositionLimitFor(symbol string) int64 {
	switch symbol {
	case "EMERALDS", "TOMATOES", "INTARIAN_PEPPER_ROOT", "ASH_COATED_OSMIUM":
		return 80
	case "RAINFOREST_RESIN", "KELP", "SQUID_INK":
		return 50
	case "CROISSANTS":
		return 250
	case "JAMS":
		return 350
	case "DJEMBES":
		return 60
	case "PICNIC_BASKET1":
		return 60
	case "PICNIC_BASKET2":
		return 100
	case "VOLCANIC_ROCK":
		return 400
	case "VOLCANIC_ROCK_VOUCHER_9500",
		"VOLCANIC_ROCK_VOUCHER_9750",
		"VOLCANIC_ROCK_VOUCHER_10000",
		"VOLCANIC_ROCK_VOUCHER_10250",
		"VOLCANIC_ROCK_VOUCHER_10500":
		return 200
	case "MAGNIFICENT_MACARONS":
		return 75
	// Prosperity 4 round 3: "Options Require Decisions"
	// (limits taken from problemset/round_3.md)
	case "HYDROGEL_PACK", "VELVETFRUIT_EXTRACT":
		return 200
	case "VEV_4000", "VEV_4500",
		"VEV_5000", "VEV_5100", "VEV_5200",
		"VEV_5300", "VEV_5400", "VEV_5500",
		"VEV_6000", "VEV_6500":
		return 300
	default:
		return DefaultPositionLimit
	}
}

// EnforcePositionLimits filters ordersBySymbol so that no product is allowed
// to exceed ±limit. Rejected products are summarised in messages for the
// sandbox log.
func EnforcePositionLimits(
	position *orderedmap.Map[int64],
	ordersBySymbol *orderedmap.Map[[]model.Order],
) (*orderedmap.Map[[]model.Order], []string) {
	filtered := orderedmap.New[[]model.Order]()
	var messages []string
	ordersBySymbol.ForEach(func(symbol string, orders []model.Order) {
		pos := position.GetOrZero(symbol)
		var totalLong, totalShort int64
		for _, o := range orders {
			if o.Quantity > 0 {
				totalLong += o.Quantity
			} else if o.Quantity < 0 {
				totalShort += -o.Quantity
			}
		}
		limit := PositionLimitFor(symbol)
		if pos+totalLong > limit || pos-totalShort < -limit {
			messages = append(messages, "Orders for product "+symbol+
				" exceeded limit "+itoa(limit)+
				"; product orders canceled for this tick")
			return
		}
		filtered.Set(symbol, orders)
	})
	return filtered, messages
}

// SnapshotToBook converts a product snapshot's bids or asks into an
// internal, sort-ordered slice suitable for consumption by the matcher.
// Bids are sorted price-descending so index 0 is always the best price;
// asks are sorted price-ascending for the same reason.
func SnapshotToBook(snapshot *model.ProductSnapshot, bids bool) []BookLevel {
	if snapshot == nil {
		return nil
	}
	src := snapshot.Asks
	if bids {
		src = snapshot.Bids
	}
	out := make([]BookLevel, 0, len(src))
	for _, lvl := range src {
		out = append(out, BookLevel{Price: lvl.Price, Volume: lvl.Volume})
	}
	if bids {
		// price descending
		for i := 1; i < len(out); i++ {
			for j := i; j > 0 && out[j-1].Price < out[j].Price; j-- {
				out[j-1], out[j] = out[j], out[j-1]
			}
		}
	} else {
		for i := 1; i < len(out); i++ {
			for j := i; j > 0 && out[j-1].Price > out[j].Price; j-- {
				out[j-1], out[j] = out[j], out[j-1]
			}
		}
	}
	return out
}

// SymbolMatchOutput is the result of processing one symbol's orders for a tick.
type SymbolMatchOutput struct {
	OwnTrades       []model.Trade
	RemainingMarket []model.Trade
	OrderRows       []FlatOrderRow
}

// MatchExecutedTradeHistory applies the default matching mode: orders first
// consume visible book levels, then sweep the tape depending on config.
func MatchExecutedTradeHistory(
	symbol string,
	orders []model.Order,
	bids []BookLevel,
	asks []BookLevel,
	marketTrades []model.MarketTrade,
	position *orderedmap.Map[int64],
	cash *orderedmap.Map[float64],
	timestamp int64,
	config model.MatchingConfig,
	recordOrders bool,
) SymbolMatchOutput {
	var own []model.Trade
	var orderRows []FlatOrderRow
	bestBid, bestAsk := bestTouchPrices(bids, asks)

	// Build the tape of available (synthetic) trades the order flow can
	// match against, scaled by queue-penetration and filtered to exclude
	// rows that clearly duplicate the touch.
	available := make([]model.Trade, 0, len(marketTrades))
	for _, tr := range marketTrades {
		if marketTradeDuplicatesTouch(tr, bestBid, bestAsk) {
			continue
		}
		ts := tr.Timestamp
		if ts == 0 {
			ts = timestamp
		}
		available = append(available, model.Trade{
			Symbol:    symbol,
			Price:     tr.Price,
			Quantity:  queuePenetrationAvailable(tr.Quantity, config.QueuePenetration),
			Buyer:     tr.Buyer,
			Seller:    tr.Seller,
			Timestamp: ts,
		})
	}

	// Queue-ahead trackers: we model a visible resting level as a queue of
	// liquidity that must be consumed before our own order can trade at the
	// same price.
	buyQueue := map[int64]int64{}
	for _, l := range bids {
		if l.Volume > 0 {
			buyQueue[l.Price] = l.Volume
		}
	}
	sellQueue := map[int64]int64{}
	for _, l := range asks {
		if l.Volume > 0 {
			sellQueue[l.Price] = l.Volume
		}
	}

	for _, order := range orders {
		remaining := order.Quantity
		if recordOrders {
			orderRows = append(orderRows, FlatOrderRow{
				Symbol:   order.Symbol,
				Price:    order.Price,
				Quantity: order.Quantity,
			})
		}
		if remaining > 0 {
			for i := range asks {
				if remaining <= 0 {
					break
				}
				lvl := &asks[i]
				if lvl.Price > order.Price || lvl.Volume <= 0 {
					continue
				}
				fill := minI64(remaining, lvl.Volume)
				price := slippageAdjustedPrice(lvl.Price, true, config.PriceSlippageBps)
				own = append(own, model.Trade{
					Symbol:    symbol,
					Price:     price,
					Quantity:  fill,
					Buyer:     "SUBMISSION",
					Timestamp: timestamp,
				})
				adjustPosition(position, symbol, fill)
				adjustCash(cash, symbol, -float64(price)*float64(fill))
				lvl.Volume -= fill
				remaining -= fill
			}
		} else if remaining < 0 {
			for i := range bids {
				if remaining >= 0 {
					break
				}
				lvl := &bids[i]
				if lvl.Price < order.Price || lvl.Volume <= 0 {
					continue
				}
				fill := minI64(-remaining, lvl.Volume)
				price := slippageAdjustedPrice(lvl.Price, false, config.PriceSlippageBps)
				own = append(own, model.Trade{
					Symbol:    symbol,
					Price:     price,
					Quantity:  fill,
					Seller:    "SUBMISSION",
					Timestamp: timestamp,
				})
				adjustPosition(position, symbol, -fill)
				adjustCash(cash, symbol, float64(price)*float64(fill))
				lvl.Volume -= fill
				remaining += fill
			}
		}

		if remaining != 0 && !config.ModeIsNone() {
			for i := range available {
				if remaining == 0 {
					break
				}
				tr := &available[i]
				if tr.Quantity <= 0 {
					continue
				}
				if !eligibleTradePrice(order.Price, tr.Price, remaining, config.TradeMatchMode) {
					continue
				}
				if remaining > 0 && tr.Price == order.Price {
					// Consume queue-ahead liquidity before our order fills.
					if ahead, ok := buyQueue[order.Price]; ok {
						consumed := minI64(tr.Quantity, ahead)
						tr.Quantity -= consumed
						ahead -= consumed
						if ahead <= 0 {
							delete(buyQueue, order.Price)
						} else {
							buyQueue[order.Price] = ahead
						}
					}
				} else if remaining < 0 && tr.Price == order.Price {
					if ahead, ok := sellQueue[order.Price]; ok {
						consumed := minI64(tr.Quantity, ahead)
						tr.Quantity -= consumed
						ahead -= consumed
						if ahead <= 0 {
							delete(sellQueue, order.Price)
						} else {
							sellQueue[order.Price] = ahead
						}
					}
				}
				if tr.Quantity <= 0 {
					continue
				}
				fill := minI64(absI64(remaining), tr.Quantity)
				price := slippageAdjustedPrice(order.Price, remaining > 0, config.PriceSlippageBps)
				if remaining > 0 {
					own = append(own, model.Trade{
						Symbol:    symbol,
						Price:     price,
						Quantity:  fill,
						Buyer:     "SUBMISSION",
						Seller:    tr.Seller,
						Timestamp: timestamp,
					})
					adjustPosition(position, symbol, fill)
					adjustCash(cash, symbol, -float64(price)*float64(fill))
					remaining -= fill
				} else {
					own = append(own, model.Trade{
						Symbol:    symbol,
						Price:     price,
						Quantity:  fill,
						Buyer:     tr.Buyer,
						Seller:    "SUBMISSION",
						Timestamp: timestamp,
					})
					adjustPosition(position, symbol, -fill)
					adjustCash(cash, symbol, float64(price)*float64(fill))
					remaining += fill
				}
				tr.Quantity -= fill
			}
		}
	}

	remainingMarket := make([]model.Trade, 0, len(available))
	for _, tr := range available {
		if tr.Quantity > 0 {
			remainingMarket = append(remainingMarket, tr)
		}
	}
	return SymbolMatchOutput{
		OwnTrades:       own,
		RemainingMarket: remainingMarket,
		OrderRows:       orderRows,
	}
}

// MatchRawCSVTape applies the raw-CSV replay policy. In this mode:
//
//  1. Own orders first sweep visible book levels. Unfilled portions become
//     resting submission orders for the tape sweep step.
//  2. Each tape trade creates a synthetic buyer and seller, both of which
//     sweep opposing liquidity (visible book + resting submission orders).
//     If any residual liquidity remains after both sides sweep, it is
//     surfaced as a public market trade on the next tick.
func MatchRawCSVTape(
	symbol string,
	orders []model.Order,
	bids []BookLevel,
	asks []BookLevel,
	marketTrades []model.MarketTrade,
	position *orderedmap.Map[int64],
	cash *orderedmap.Map[float64],
	timestamp int64,
	config model.MatchingConfig,
	recordOrders bool,
) SymbolMatchOutput {
	var own []model.Trade
	var publicTrades []model.Trade
	var orderRows []FlatOrderRow
	var restingBids []restingSubmissionOrder
	var restingAsks []restingSubmissionOrder

	for _, order := range orders {
		remaining := order.Quantity
		if recordOrders {
			orderRows = append(orderRows, FlatOrderRow{
				Symbol:   order.Symbol,
				Price:    order.Price,
				Quantity: order.Quantity,
			})
		}
		if remaining > 0 {
			for i := range asks {
				if remaining <= 0 {
					break
				}
				lvl := &asks[i]
				if lvl.Price > order.Price || lvl.Volume <= 0 {
					continue
				}
				fill := minI64(remaining, lvl.Volume)
				price := slippageAdjustedPrice(lvl.Price, true, config.PriceSlippageBps)
				own = append(own, model.Trade{
					Symbol:    symbol,
					Price:     price,
					Quantity:  fill,
					Buyer:     "SUBMISSION",
					Timestamp: timestamp,
				})
				adjustPosition(position, symbol, fill)
				adjustCash(cash, symbol, -float64(price)*float64(fill))
				lvl.Volume -= fill
				remaining -= fill
			}
			if remaining > 0 {
				restingBids = append(restingBids, restingSubmissionOrder{Price: order.Price, Quantity: remaining})
			}
		} else if remaining < 0 {
			for i := range bids {
				if remaining >= 0 {
					break
				}
				lvl := &bids[i]
				if lvl.Price < order.Price || lvl.Volume <= 0 {
					continue
				}
				fill := minI64(-remaining, lvl.Volume)
				price := slippageAdjustedPrice(lvl.Price, false, config.PriceSlippageBps)
				own = append(own, model.Trade{
					Symbol:    symbol,
					Price:     price,
					Quantity:  fill,
					Seller:    "SUBMISSION",
					Timestamp: timestamp,
				})
				adjustPosition(position, symbol, -fill)
				adjustCash(cash, symbol, float64(price)*float64(fill))
				lvl.Volume -= fill
				remaining += fill
			}
			if remaining < 0 {
				restingAsks = append(restingAsks, restingSubmissionOrder{Price: order.Price, Quantity: -remaining})
			}
		}
	}

	for _, tr := range marketTrades {
		if tr.Quantity <= 0 {
			continue
		}
		ts := tr.Timestamp
		if ts == 0 {
			ts = timestamp
		}
		askRemaining := tr.Quantity
		bidRemaining := tr.Quantity
		sweepSyntheticSell(symbol, tr.Price, &askRemaining, bids, restingBids,
			position, cash, &own, ts, tr.Seller, config.PriceSlippageBps)
		sweepSyntheticBuy(symbol, tr.Price, &bidRemaining, asks, restingAsks,
			position, cash, &own, ts, tr.Buyer, config.PriceSlippageBps)
		residual := minI64(bidRemaining, askRemaining)
		if residual > 0 {
			publicTrades = append(publicTrades, model.Trade{
				Symbol:    symbol,
				Price:     tr.Price,
				Quantity:  residual,
				Buyer:     tr.Buyer,
				Seller:    tr.Seller,
				Timestamp: ts,
			})
		}
	}

	return SymbolMatchOutput{
		OwnTrades:       own,
		RemainingMarket: publicTrades,
		OrderRows:       orderRows,
	}
}

// sweepSyntheticSell / sweepSyntheticBuy model the two halves of a tape trade
// walking through both visible and resting-submission liquidity. Visible book
// and resting submission orders compete for each match; the best price wins,
// with visible book preferred at equal price (matching Rust's implementation).
func sweepSyntheticSell(
	symbol string,
	price int64,
	remaining *int64,
	bids []BookLevel,
	restingBids []restingSubmissionOrder,
	position *orderedmap.Map[int64],
	cash *orderedmap.Map[float64],
	own *[]model.Trade,
	timestamp int64,
	counterpartySeller string,
	slippageBps float64,
) {
	for *remaining > 0 {
		visibleIdx := bestVisibleBidIndex(bids, price)
		restingIdx := bestRestingBidIndex(restingBids, price)
		useVisible := false
		if visibleIdx >= 0 && restingIdx >= 0 {
			useVisible = bids[visibleIdx].Price >= restingBids[restingIdx].Price
		} else {
			useVisible = visibleIdx >= 0
		}
		if visibleIdx < 0 && restingIdx < 0 {
			return
		}
		if useVisible {
			fill := minI64(*remaining, bids[visibleIdx].Volume)
			bids[visibleIdx].Volume -= fill
			*remaining -= fill
		} else {
			fill := minI64(*remaining, restingBids[restingIdx].Quantity)
			executionPrice := slippageAdjustedPrice(restingBids[restingIdx].Price, true, slippageBps)
			*own = append(*own, model.Trade{
				Symbol:    symbol,
				Price:     executionPrice,
				Quantity:  fill,
				Buyer:     "SUBMISSION",
				Seller:    counterpartySeller,
				Timestamp: timestamp,
			})
			adjustPosition(position, symbol, fill)
			adjustCash(cash, symbol, -float64(executionPrice)*float64(fill))
			restingBids[restingIdx].Quantity -= fill
			*remaining -= fill
		}
	}
}

func sweepSyntheticBuy(
	symbol string,
	price int64,
	remaining *int64,
	asks []BookLevel,
	restingAsks []restingSubmissionOrder,
	position *orderedmap.Map[int64],
	cash *orderedmap.Map[float64],
	own *[]model.Trade,
	timestamp int64,
	counterpartyBuyer string,
	slippageBps float64,
) {
	for *remaining > 0 {
		visibleIdx := bestVisibleAskIndex(asks, price)
		restingIdx := bestRestingAskIndex(restingAsks, price)
		useVisible := false
		if visibleIdx >= 0 && restingIdx >= 0 {
			useVisible = asks[visibleIdx].Price <= restingAsks[restingIdx].Price
		} else {
			useVisible = visibleIdx >= 0
		}
		if visibleIdx < 0 && restingIdx < 0 {
			return
		}
		if useVisible {
			fill := minI64(*remaining, asks[visibleIdx].Volume)
			asks[visibleIdx].Volume -= fill
			*remaining -= fill
		} else {
			fill := minI64(*remaining, restingAsks[restingIdx].Quantity)
			executionPrice := slippageAdjustedPrice(restingAsks[restingIdx].Price, false, slippageBps)
			*own = append(*own, model.Trade{
				Symbol:    symbol,
				Price:     executionPrice,
				Quantity:  fill,
				Buyer:     counterpartyBuyer,
				Seller:    "SUBMISSION",
				Timestamp: timestamp,
			})
			adjustPosition(position, symbol, -fill)
			adjustCash(cash, symbol, float64(executionPrice)*float64(fill))
			restingAsks[restingIdx].Quantity -= fill
			*remaining -= fill
		}
	}
}

func bestVisibleBidIndex(bids []BookLevel, minPrice int64) int {
	for i, l := range bids {
		if l.Volume > 0 && l.Price >= minPrice {
			return i
		}
	}
	return -1
}

func bestVisibleAskIndex(asks []BookLevel, maxPrice int64) int {
	for i, l := range asks {
		if l.Volume > 0 && l.Price <= maxPrice {
			return i
		}
	}
	return -1
}

func bestRestingBidIndex(resting []restingSubmissionOrder, minPrice int64) int {
	best := -1
	var bestPrice int64
	for i, o := range resting {
		if o.Quantity <= 0 || o.Price < minPrice {
			continue
		}
		if best < 0 || o.Price > bestPrice {
			best = i
			bestPrice = o.Price
		}
	}
	return best
}

func bestRestingAskIndex(resting []restingSubmissionOrder, maxPrice int64) int {
	best := -1
	var bestPrice int64
	for i, o := range resting {
		if o.Quantity <= 0 || o.Price > maxPrice {
			continue
		}
		if best < 0 || o.Price < bestPrice {
			best = i
			bestPrice = o.Price
		}
	}
	return best
}

// marketTradeDuplicatesTouch filters out tape rows that clearly duplicate a
// trade we just reconstructed from the book. The heuristic catches the case
// where IMC replays a SUBMISSION tape row for the same touch we already have
// in the visible book, which would otherwise double-count liquidity.
func marketTradeDuplicatesTouch(tr model.MarketTrade, bestBid, bestAsk *int64) bool {
	if tr.Buyer == "SUBMISSION" && bestAsk != nil && tr.Price >= *bestAsk {
		return true
	}
	if tr.Seller == "SUBMISSION" && bestBid != nil && tr.Price <= *bestBid {
		return true
	}
	return false
}

func bestTouchPrices(bids, asks []BookLevel) (*int64, *int64) {
	var bestBid, bestAsk *int64
	for _, l := range bids {
		if l.Volume > 0 {
			if bestBid == nil || l.Price > *bestBid {
				v := l.Price
				bestBid = &v
			}
		}
	}
	for _, l := range asks {
		if l.Volume > 0 {
			if bestAsk == nil || l.Price < *bestAsk {
				v := l.Price
				bestAsk = &v
			}
		}
	}
	return bestBid, bestAsk
}

// queuePenetrationAvailable applies the queue-penetration factor to a tape
// quantity, using banker's rounding and a floor of 1 when penetration is > 0.
func queuePenetrationAvailable(quantity int64, penetration float64) int64 {
	if penetration < 0 {
		penetration = 0
	}
	raw := float64(quantity) * penetration
	available := int64(jsonfmt.RoundHalfToEven(raw))
	if quantity > 0 && penetration > 0 && available == 0 {
		available = 1
	}
	if available < 0 {
		return 0
	}
	return available
}

// eligibleTradePrice encodes the TradeMatchMode semantics. "all" allows
// at-or-better-than-order prices, "worse" requires strictly better, "none"
// disables tape matching entirely.
func eligibleTradePrice(orderPrice, tradePrice, quantity int64, mode string) bool {
	if mode == "none" {
		return false
	}
	if quantity > 0 {
		if mode == "all" {
			return tradePrice <= orderPrice
		}
		return tradePrice < orderPrice
	}
	if quantity < 0 {
		if mode == "all" {
			return tradePrice >= orderPrice
		}
		return tradePrice > orderPrice
	}
	return false
}

// slippageAdjustedPrice nudges the fill price against the trader by `bps`
// basis points, using banker's rounding to stay integer-valued.
func slippageAdjustedPrice(price int64, isBuy bool, bps float64) int64 {
	if bps <= 0 {
		return price
	}
	factor := 1.0 + (bps / 10_000.0)
	var adjusted float64
	if isBuy {
		adjusted = float64(price) * factor
	} else {
		adjusted = float64(price) / factor
	}
	return int64(jsonfmt.RoundHalfToEven(adjusted))
}

func adjustPosition(position *orderedmap.Map[int64], symbol string, delta int64) {
	position.Upsert(symbol, func(cur int64) int64 { return cur + delta })
}

func adjustCash(cash *orderedmap.Map[float64], symbol string, delta float64) {
	cash.Upsert(symbol, func(cur float64) float64 { return cur + delta })
}

func minI64(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

func absI64(a int64) int64 {
	if a < 0 {
		return -a
	}
	return a
}

func itoa(v int64) string {
	// Tiny helper that avoids pulling in strconv in this hot path.
	if v == 0 {
		return "0"
	}
	var buf [20]byte
	neg := v < 0
	u := uint64(v)
	if neg {
		u = uint64(-v)
	}
	i := len(buf)
	for u > 0 {
		i--
		buf[i] = byte('0' + u%10)
		u /= 10
	}
	if neg {
		i--
		buf[i] = '-'
	}
	return string(buf[i:])
}
