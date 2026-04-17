package engine

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/darora1/imc-prosperity-4/backtester/internal/dataset"
	"github.com/darora1/imc-prosperity-4/backtester/internal/jsonfmt"
	"github.com/darora1/imc-prosperity-4/backtester/internal/model"
	"github.com/darora1/imc-prosperity-4/backtester/internal/orderedmap"
	"github.com/darora1/imc-prosperity-4/backtester/internal/pytrader"
)

// activityHeader is the semicolon-separated header emitted at the top of
// every activities CSV and submission-log activitiesLog.
const activityHeader = "day;timestamp;product;bid_price_1;bid_volume_1;bid_price_2;bid_volume_2;bid_price_3;bid_volume_3;ask_price_1;ask_volume_1;ask_price_2;ask_volume_2;ask_price_3;ask_volume_3;mid_price;profit_and_loss"

// logCharLimit caps captured trader stdout per tick, matching the reference
// IMC portal behavior. Longer logs are truncated without further notice.
const logCharLimit = 3750

// Run executes one backtest and returns the metrics + artifacts.
func Run(ctx context.Context, req *model.RunRequest) (*model.RunOutput, error) {
	ds := req.DatasetOverride
	if ds == nil {
		loaded, err := dataset.Load(req.DatasetFile)
		if err != nil {
			return nil, err
		}
		ds = loaded
	}
	replayMode := InferReplayMode(ds)

	ticks := filterTicks(ds.Ticks, req.Day)
	if len(ticks) == 0 {
		return nil, errors.New("no ticks available for selected dataset/day")
	}

	trader, err := pytrader.New(ctx, workspaceRoot(), req.TraderFile)
	if err != nil {
		return nil, err
	}
	defer trader.Close()

	runID, err := resolveRunID(req)
	if err != nil {
		return nil, err
	}
	runDir := filepath.Join(req.OutputRoot, runID)
	needSubmissionLog := req.Persist || req.WriteSubmissionLog
	writeBundle := req.Persist || req.WriteBundle || req.MaterializeArtifacts
	fullArtifacts := req.Persist || req.MaterializeArtifacts
	if needSubmissionLog || req.WriteMetrics || writeBundle {
		if err := os.MkdirAll(runDir, 0o755); err != nil {
			return nil, fmt.Errorf("failed to create run directory %s: %w", runDir, err)
		}
	}

	recordedTraderPath := displayPath(req.TraderFile)
	if req.MetadataOverrides.RecordedTraderPath != nil {
		recordedTraderPath = *req.MetadataOverrides.RecordedTraderPath
	}
	recordedDatasetPath := displayPath(req.DatasetFile)
	if req.MetadataOverrides.RecordedDatasetPath != nil {
		recordedDatasetPath = *req.MetadataOverrides.RecordedDatasetPath
	}
	generatedAt := nowUTCISO()
	if req.MetadataOverrides.GeneratedAt != nil {
		generatedAt = *req.MetadataOverrides.GeneratedAt
	}

	cash := orderedmap.New[float64]()
	for _, p := range ds.Products {
		cash.Set(p, 0.0)
	}
	position := orderedmap.New[int64]()
	ownTradesPrev := orderedmap.New[[]model.Trade]()
	marketTradesPrev := orderedmap.New[[]model.Trade]()
	traderData := ""
	lastStableMark := orderedmap.New[float64]()

	var ownTradeCount int
	var finalPnLTotal float64
	finalPnLByProduct := orderedmap.New[float64]()
	for _, p := range ds.Products {
		finalPnLByProduct.Set(p, 0.0)
	}

	activityRows := []string{}
	if needSubmissionLog {
		activityRows = append(activityRows, activityHeader)
	}
	var ownTradeRows []map[string]any
	var combinedTradeHistory []map[string]any
	var sandboxRows []map[string]any
	var timeline []any
	var pnlSeries []any

	for _, tick := range ticks {
		tickResult, err := trader.RunTick(pytrader.Invocation{
			TraderData:       traderData,
			Tick:             tick,
			OwnTradesPrev:    ownTradesPrev,
			MarketTradesPrev: marketTradesPrev,
			Position:         position,
		})
		if err != nil {
			return nil, err
		}
		traderData = tickResult.TraderData

		orders, limitMessages := EnforcePositionLimits(position, tickResult.OrdersBySymbol)

		ownTradesTick := orderedmap.New[[]model.Trade]()
		marketTradesNext := orderedmap.New[[]model.Trade]()
		var ordersFlat []FlatOrderRow

		for _, product := range ds.Products {
			snapshot, _ := tick.Products.Get(product)
			bids := SnapshotToBook(snapshot, true)
			asks := SnapshotToBook(snapshot, false)
			marketTrades, _ := tick.MarketTrades.Get(product)
			productOrders, _ := orders.Get(product)
			var match SymbolMatchOutput
			if replayMode == MarketReplayRawCSVTape {
				match = MatchRawCSVTape(product, productOrders, bids, asks,
					marketTrades, position, cash, tick.Timestamp, req.Matching, fullArtifacts)
			} else {
				match = MatchExecutedTradeHistory(product, productOrders, bids, asks,
					marketTrades, position, cash, tick.Timestamp, req.Matching, fullArtifacts)
			}

			if needSubmissionLog {
				for _, tr := range match.RemainingMarket {
					combinedTradeHistory = append(combinedTradeHistory, tradeHistoryJSON(tr, tick.Day))
				}
				for _, tr := range match.OwnTrades {
					combinedTradeHistory = append(combinedTradeHistory, tradeHistoryJSON(tr, tick.Day))
					if fullArtifacts {
						ownTradeRows = append(ownTradeRows, tradeHistoryJSON(tr, tick.Day))
					}
				}
			}
			if fullArtifacts {
				ordersFlat = append(ordersFlat, match.OrderRows...)
			}
			if len(match.OwnTrades) > 0 {
				ownTradeCount += len(match.OwnTrades)
				ownTradesTick.Set(product, match.OwnTrades)
			}
			if len(match.RemainingMarket) > 0 {
				marketTradesNext.Set(product, match.RemainingMarket)
			}
		}

		algorithmLogs := truncateLogs(tickResult.Stdout)
		sandboxLog := strings.Join(limitMessages, "\n")

		pnlByProduct := orderedmap.WithCapacity[float64](len(ds.Products))
		markPrices := orderedmap.WithCapacity[float64](len(ds.Products))
		for _, product := range ds.Products {
			snapshot, _ := tick.Products.Get(product)
			if stable, ok := resolveStableMark(snapshot); ok {
				lastStableMark.Set(product, stable)
			}
			markPrice, ok := resolveMarkPrice(snapshot, lastStableMark, product)
			if ok {
				markPrices.Set(product, markPrice)
			}
			var markToMarket float64
			if ok {
				markToMarket = float64(position.GetOrZero(product)) * markPrice
			}
			pnl := cash.GetOrZero(product) + markToMarket
			pnlByProduct.Set(product, pnl)
			if needSubmissionLog {
				var markPtr *float64
				if ok {
					m := markPrice
					markPtr = &m
				}
				activityRows = append(activityRows, formatActivityRow(tick, product, snapshot, markPtr, pnl))
			}
		}

		finalPnLTotal = 0
		for _, pair := range pnlByProduct.Entries() {
			finalPnLTotal += pair.Value
			finalPnLByProduct.Set(pair.Key, pair.Value)
		}

		if needSubmissionLog {
			sandboxRows = append(sandboxRows, map[string]any{
				"day":        optionalInt64JSON(tick.Day),
				"timestamp":  tick.Timestamp,
				"sandboxLog": sandboxLog,
				"lambdaLog":  algorithmLogs,
			})
		}

		if writeBundle {
			pnlByProductCopy := orderedmap.New[float64]()
			for _, p := range pnlByProduct.Entries() {
				pnlByProductCopy.Set(p.Key, p.Value)
			}
			pnlSeries = append(pnlSeries, map[string]any{
				"timestamp":  tick.Timestamp,
				"total":      finalPnLTotal,
				"by_product": pnlByProductCopy,
			})
			if fullArtifacts {
				timeline = append(timeline, buildTimelineRow(tick, ordersFlat,
					ownTradesTick, marketTradesNext, position, markPrices,
					pnlByProduct, finalPnLTotal, sandboxLog, algorithmLogs,
					tickResult.Conversions, traderData))
			}
		}

		ownTradesPrev = ownTradesTick
		marketTradesPrev = marketTradesNext
	}

	metrics := model.RunMetrics{
		RunID:             runID,
		DatasetID:         ds.DatasetID,
		DatasetPath:       recordedDatasetPath,
		TraderPath:        recordedTraderPath,
		Day:               req.Day,
		Matching:          req.Matching,
		TickCount:         len(ticks),
		OwnTradeCount:     ownTradeCount,
		FinalPnLTotal:     finalPnLTotal,
		FinalPnLByProduct: finalPnLByProduct,
		GeneratedAt:       generatedAt,
	}

	metricsValue := map[string]any{
		"run_id":               metrics.RunID,
		"dataset_id":           metrics.DatasetID,
		"dataset_path":         metrics.DatasetPath,
		"trader_path":          metrics.TraderPath,
		"day":                  optionalInt64JSON(metrics.Day),
		"matching":             metrics.Matching,
		"tick_count":           metrics.TickCount,
		"own_trade_count":      metrics.OwnTradeCount,
		"final_pnl_total":      metrics.FinalPnLTotal,
		"final_pnl_by_product": metrics.FinalPnLByProduct,
		"generated_at":         metrics.GeneratedAt,
	}

	result := map[string]any{
		"run_id":  runID,
		"run_dir": displayPath(runDir),
		"metrics": metricsValue,
	}
	resultBytes, err := jsonfmt.SortedJSONBytes(result)
	if err != nil {
		return nil, err
	}

	output := &model.RunOutput{
		RunID:      runID,
		RunDir:     runDir,
		Metrics:    metrics,
		ResultJSON: resultBytes,
	}

	if needSubmissionLog || req.WriteMetrics || writeBundle {
		bundle := map[string]any{
			"run": map[string]any{
				"run_id":       runID,
				"dataset_id":   ds.DatasetID,
				"dataset_path": recordedDatasetPath,
				"trader_path":  recordedTraderPath,
				"day":          optionalInt64JSON(req.Day),
				"matching":     req.Matching,
				"generated_at": generatedAt,
			},
			"products":   ds.Products,
			"timeline":   emptyArrayIfNil(timeline, fullArtifacts),
			"pnl_series": emptyArrayIfNil(pnlSeries, writeBundle),
		}
		artifacts := &model.ArtifactSet{}
		if writeBundle {
			bundleBytes, err := jsonfmt.SortedJSONBytes(bundle)
			if err != nil {
				return nil, err
			}
			artifacts.BundleJSON = bundleBytes
		}
		if fullArtifacts || req.WriteMetrics {
			metricsBytes, err := jsonfmt.SortedJSONBytes(metricsValue)
			if err != nil {
				return nil, err
			}
			artifacts.MetricsJSON = metricsBytes
		}
		if fullArtifacts {
			artifacts.ActivityCSV = joinLinesBytes(activityRows)
		}
		if needSubmissionLog {
			submissionBytes, err := buildSubmissionLog(runID, activityRows, sandboxRows, combinedTradeHistory)
			if err != nil {
				return nil, err
			}
			artifacts.SubmissionLog = submissionBytes
		}
		if fullArtifacts {
			pnlCSV, err := buildPnLCSV(ds.Products, pnlSeries)
			if err != nil {
				return nil, err
			}
			artifacts.PnLByProductCSV = pnlCSV

			combinedBytes, err := buildCombinedLog(sandboxRows, activityRows, combinedTradeHistory)
			if err != nil {
				return nil, err
			}
			artifacts.CombinedLog = combinedBytes

			tradesBytes, err := buildTradesCSV(ownTradeRows)
			if err != nil {
				return nil, err
			}
			artifacts.TradesCSV = tradesBytes
		}

		if err := writeArtifacts(runDir, artifacts, req); err != nil {
			return nil, err
		}
		output.Artifacts = artifacts
	}

	return output, nil
}

// filterTicks keeps only ticks for the requested day (or all when day is nil)
// and returns them sorted by (day, timestamp).
func filterTicks(ticks []*model.TickSnapshot, day *int64) []*model.TickSnapshot {
	out := make([]*model.TickSnapshot, 0, len(ticks))
	for _, t := range ticks {
		if day == nil || (t.Day != nil && *t.Day == *day) {
			out = append(out, t)
		}
	}
	sort.SliceStable(out, func(i, j int) bool {
		da, db := out[i].Day, out[j].Day
		if da == nil && db != nil {
			return true
		}
		if da != nil && db == nil {
			return false
		}
		if da != nil && db != nil && *da != *db {
			return *da < *db
		}
		return out[i].Timestamp < out[j].Timestamp
	})
	return out
}

func resolveRunID(req *model.RunRequest) (string, error) {
	if req.MetadataOverrides.RunID != nil {
		return *req.MetadataOverrides.RunID, nil
	}
	if req.RunID != nil {
		return *req.RunID, nil
	}
	return strings.ReplaceAll(nowUTCISO(), ":", "-"), nil
}

func nowUTCISO() string {
	return time.Now().UTC().Format("2006-01-02T15-04-05+00:00")
}

func workspaceRoot() string {
	cwd, err := os.Getwd()
	if err != nil {
		return "."
	}
	return cwd
}

// displayPath collapses absolute paths to workspace-relative, forward-slashed
// strings for clean artifact output.
func displayPath(path string) string {
	if path == "" {
		return path
	}
	clean := filepath.Clean(path)
	if cwd, err := os.Getwd(); err == nil {
		if rel, err := filepath.Rel(cwd, clean); err == nil && !strings.HasPrefix(rel, "..") {
			return strings.ReplaceAll(rel, "\\", "/")
		}
	}
	return strings.ReplaceAll(clean, "\\", "/")
}

func resolveStableMark(snapshot *model.ProductSnapshot) (float64, bool) {
	if snapshot == nil {
		return 0, false
	}
	var bestBid, bestAsk *float64
	if len(snapshot.Bids) > 0 {
		v := float64(snapshot.Bids[0].Price)
		bestBid = &v
	}
	if len(snapshot.Asks) > 0 {
		v := float64(snapshot.Asks[0].Price)
		bestAsk = &v
	}
	if bestBid == nil || bestAsk == nil {
		return 0, false
	}
	if snapshot.MidPrice != nil && *snapshot.MidPrice > 0 {
		return *snapshot.MidPrice, true
	}
	return (*bestBid + *bestAsk) / 2, true
}

func resolveMarkPrice(snapshot *model.ProductSnapshot, lastStable *orderedmap.Map[float64], product string) (float64, bool) {
	if v, ok := resolveStableMark(snapshot); ok {
		return v, true
	}
	previous, hasPrevious := lastStable.Get(product)
	if snapshot == nil {
		if hasPrevious {
			return previous, true
		}
		return 0, false
	}
	if hasPrevious {
		return previous, true
	}
	if snapshot.MidPrice != nil && *snapshot.MidPrice > 0 {
		return *snapshot.MidPrice, true
	}
	if len(snapshot.Bids) > 0 {
		return float64(snapshot.Bids[0].Price), true
	}
	if len(snapshot.Asks) > 0 {
		return float64(snapshot.Asks[0].Price), true
	}
	return 0, false
}

func buildTimelineRow(
	tick *model.TickSnapshot,
	ordersFlat []FlatOrderRow,
	ownTradesTick *orderedmap.Map[[]model.Trade],
	marketTradesNext *orderedmap.Map[[]model.Trade],
	position *orderedmap.Map[int64],
	markPrices *orderedmap.Map[float64],
	pnlByProduct *orderedmap.Map[float64],
	pnlTotal float64,
	sandboxLog string,
	algorithmLogs string,
	conversions int64,
	traderData string,
) map[string]any {
	products := orderedmap.New[map[string]any]()
	tick.Products.ForEach(func(product string, snapshot *model.ProductSnapshot) {
		entry := map[string]any{
			"bids":      levelsToJSON(snapshot.Bids),
			"asks":      levelsToJSON(snapshot.Asks),
			"mid_price": optionalFloatJSON(getOptFloat(markPrices, product)),
		}
		products.Set(product, entry)
	})
	var ownTrades []map[string]any
	ownTradesTick.ForEach(func(_ string, trades []model.Trade) {
		for _, t := range trades {
			ownTrades = append(ownTrades, tradeJSON(t))
		}
	})
	var marketTrades []map[string]any
	marketTradesNext.ForEach(func(_ string, trades []model.Trade) {
		for _, t := range trades {
			marketTrades = append(marketTrades, tradeJSON(t))
		}
	})
	return map[string]any{
		"timestamp":      tick.Timestamp,
		"day":            optionalInt64JSON(tick.Day),
		"products":       products,
		"orders":         ordersToJSON(ordersFlat),
		"own_trades":     emptyArrayIfNil(ownTrades, true),
		"market_trades":  emptyArrayIfNil(marketTrades, true),
		"position":       position,
		"pnl_total":      pnlTotal,
		"pnl_by_product": pnlByProduct,
		"sandbox_logs":   sandboxLog,
		"algorithm_logs": algorithmLogs,
		"conversions":    conversions,
		"trader_data":    traderData,
	}
}

func getOptFloat(m *orderedmap.Map[float64], key string) *float64 {
	if v, ok := m.Get(key); ok {
		return &v
	}
	return nil
}

func levelsToJSON(levels []model.OrderBookLevel) []map[string]any {
	out := make([]map[string]any, 0, len(levels))
	for _, l := range levels {
		out = append(out, map[string]any{"price": l.Price, "volume": l.Volume})
	}
	return out
}

func ordersToJSON(rows []FlatOrderRow) []map[string]any {
	out := make([]map[string]any, 0, len(rows))
	for _, r := range rows {
		out = append(out, map[string]any{
			"symbol":   r.Symbol,
			"price":    r.Price,
			"quantity": r.Quantity,
		})
	}
	return out
}

func tradeJSON(t model.Trade) map[string]any {
	return map[string]any{
		"symbol":    t.Symbol,
		"price":     t.Price,
		"quantity":  t.Quantity,
		"buyer":     t.Buyer,
		"seller":    t.Seller,
		"timestamp": t.Timestamp,
	}
}

func tradeHistoryJSON(t model.Trade, day *int64) map[string]any {
	return map[string]any{
		"day":       optionalInt64JSON(day),
		"timestamp": t.Timestamp,
		"buyer":     t.Buyer,
		"seller":    t.Seller,
		"symbol":    t.Symbol,
		"currency":  "SEASHELLS",
		"price":     t.Price,
		"quantity":  t.Quantity,
	}
}

func optionalInt64JSON(v *int64) any {
	if v == nil {
		return nil
	}
	return *v
}

func optionalFloatJSON(v *float64) any {
	if v == nil {
		return nil
	}
	return *v
}

func emptyArrayIfNil[T any](in []T, include bool) []T {
	if !include {
		return []T{}
	}
	if in == nil {
		return []T{}
	}
	return in
}

// truncateLogs clamps the captured stdout to logCharLimit bytes (counting
// UTF-8 runes, matching Python character semantics).
func truncateLogs(raw string) string {
	trimmed := strings.TrimRightFunc(raw, func(r rune) bool {
		return r == '\n' || r == '\r' || r == ' ' || r == '\t'
	})
	if len(trimmed) == 0 {
		return ""
	}
	count := 0
	for i := range trimmed {
		count++
		if count > logCharLimit {
			return trimmed[:i]
		}
	}
	return trimmed
}

func formatActivityRow(tick *model.TickSnapshot, product string, snapshot *model.ProductSnapshot, markPrice *float64, pnl float64) string {
	var bidPrices, bidVolumes, askPrices, askVolumes []string
	if snapshot != nil {
		for _, l := range snapshot.Bids {
			bidPrices = append(bidPrices, strconv.FormatInt(l.Price, 10))
			bidVolumes = append(bidVolumes, strconv.FormatInt(l.Volume, 10))
		}
		for _, l := range snapshot.Asks {
			askPrices = append(askPrices, strconv.FormatInt(l.Price, 10))
			askVolumes = append(askVolumes, strconv.FormatInt(l.Volume, 10))
		}
	}
	roundedPnL := jsonfmt.RoundToDigits(pnl, 6)
	dayText := ""
	if tick.Day != nil {
		dayText = strconv.FormatInt(*tick.Day, 10)
	}
	markText := ""
	if markPrice != nil {
		markText = jsonfmt.PythonFloatString(*markPrice)
	}
	fields := []string{
		dayText,
		strconv.FormatInt(tick.Timestamp, 10),
		product,
		getOrEmpty(bidPrices, 0),
		getOrEmpty(bidVolumes, 0),
		getOrEmpty(bidPrices, 1),
		getOrEmpty(bidVolumes, 1),
		getOrEmpty(bidPrices, 2),
		getOrEmpty(bidVolumes, 2),
		getOrEmpty(askPrices, 0),
		getOrEmpty(askVolumes, 0),
		getOrEmpty(askPrices, 1),
		getOrEmpty(askVolumes, 1),
		getOrEmpty(askPrices, 2),
		getOrEmpty(askVolumes, 2),
		markText,
		jsonfmt.PythonFloatString(roundedPnL),
	}
	return strings.Join(fields, ";")
}

func getOrEmpty(values []string, idx int) string {
	if idx < len(values) {
		return values[idx]
	}
	return ""
}

func joinLinesBytes(lines []string) []byte {
	if len(lines) == 0 {
		return []byte{'\n'}
	}
	out := []byte(strings.Join(lines, "\n"))
	return append(out, '\n')
}

func buildPnLCSV(products []string, pnlSeries []any) ([]byte, error) {
	header := append([]string{"timestamp", "total"}, products...)
	lines := []string{strings.Join(header, ";")}
	for _, row := range pnlSeries {
		payload, ok := row.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("pnl_series row is not an object")
		}
		tsAny, ok := payload["timestamp"]
		if !ok {
			return nil, fmt.Errorf("pnl_series row missing timestamp")
		}
		totalAny, ok := payload["total"]
		if !ok {
			return nil, fmt.Errorf("pnl_series row missing total")
		}
		byProductAny, ok := payload["by_product"]
		if !ok {
			return nil, fmt.Errorf("pnl_series row missing by_product")
		}
		ts, _ := tsAny.(int64)
		total, _ := totalAny.(float64)
		byProduct, _ := byProductAny.(*orderedmap.Map[float64])
		fields := []string{strconv.FormatInt(ts, 10), jsonfmt.PythonFloatString(total)}
		for _, product := range products {
			v, _ := byProduct.Get(product)
			fields = append(fields, jsonfmt.PythonFloatString(v))
		}
		lines = append(lines, strings.Join(fields, ";"))
	}
	return joinLinesBytes(lines), nil
}

func buildCombinedLog(sandboxRows []map[string]any, activityRows []string, tradeHistory []map[string]any) ([]byte, error) {
	var buf bytes.Buffer
	buf.WriteString("Sandbox logs:\n")
	for _, row := range sandboxRows {
		data, err := jsonfmt.PrettyJSONBytes(row)
		if err != nil {
			return nil, err
		}
		buf.Write(data)
	}
	buf.WriteByte('\n')
	buf.WriteString("Activities log:\n")
	buf.WriteString(strings.Join(activityRows, "\n"))
	buf.WriteString("\n\nTrade History:\n")
	data, err := jsonfmt.PrettyJSONBytes(tradeHistory)
	if err != nil {
		return nil, err
	}
	buf.Write(data)
	return buf.Bytes(), nil
}

func buildSubmissionLog(runID string, activityRows []string, sandboxRows []map[string]any, tradeHistory []map[string]any) ([]byte, error) {
	payload := map[string]any{
		"submissionId": runID,
		"activitiesLog": strings.Join(activityRows, "\n"),
		"logs":          sandboxRows,
		"tradeHistory":  tradeHistory,
	}
	return jsonfmt.PrettyJSONBytes(payload)
}

func buildTradesCSV(rows []map[string]any) ([]byte, error) {
	lines := []string{"timestamp;buyer;seller;symbol;currency;price;quantity"}
	for _, row := range rows {
		ts, ok := row["timestamp"].(int64)
		if !ok {
			return nil, fmt.Errorf("trade row missing timestamp")
		}
		price, ok := row["price"].(int64)
		if !ok {
			return nil, fmt.Errorf("trade row missing price")
		}
		qty, ok := row["quantity"].(int64)
		if !ok {
			return nil, fmt.Errorf("trade row missing quantity")
		}
		lines = append(lines, strings.Join([]string{
			strconv.FormatInt(ts, 10),
			asString(row["buyer"]),
			asString(row["seller"]),
			asString(row["symbol"]),
			asString(row["currency"]),
			strconv.FormatInt(price, 10),
			strconv.FormatInt(qty, 10),
		}, ";"))
	}
	return joinLinesBytes(lines), nil
}

func asString(v any) string {
	s, _ := v.(string)
	return s
}

func writeArtifacts(runDir string, artifacts *model.ArtifactSet, req *model.RunRequest) error {
	writeFile := func(name string, data []byte) error {
		if len(data) == 0 {
			return nil
		}
		return os.WriteFile(filepath.Join(runDir, name), data, 0o644)
	}
	writeMetricsLog := req.WriteMetrics || req.Persist
	if req.Persist {
		if err := writeFile("metrics.json", artifacts.MetricsJSON); err != nil {
			return err
		}
		if err := writeFile("bundle.json", artifacts.BundleJSON); err != nil {
			return err
		}
		if err := writeFile("submission.log", artifacts.SubmissionLog); err != nil {
			return err
		}
		if err := writeFile("activity.csv", artifacts.ActivityCSV); err != nil {
			return err
		}
		if err := writeFile("pnl_by_product.csv", artifacts.PnLByProductCSV); err != nil {
			return err
		}
		if err := writeFile("combined.log", artifacts.CombinedLog); err != nil {
			return err
		}
		if err := writeFile("trades.csv", artifacts.TradesCSV); err != nil {
			return err
		}
		return nil
	}
	if writeMetricsLog && req.WriteBundle {
		if err := writeFile("metrics.json", artifacts.MetricsJSON); err != nil {
			return err
		}
		if err := writeFile("bundle.json", artifacts.BundleJSON); err != nil {
			return err
		}
		return nil
	}
	if writeMetricsLog && req.WriteSubmissionLog {
		if err := writeFile("metrics.json", artifacts.MetricsJSON); err != nil {
			return err
		}
		if err := writeFile("submission.log", artifacts.SubmissionLog); err != nil {
			return err
		}
		return nil
	}
	if req.WriteBundle {
		return writeFile("bundle.json", artifacts.BundleJSON)
	}
	if writeMetricsLog {
		return writeFile("metrics.json", artifacts.MetricsJSON)
	}
	if req.WriteSubmissionLog {
		return writeFile("submission.log", artifacts.SubmissionLog)
	}
	return nil
}

// DatasetFromJSONReader is a small convenience used by carry-mode planning
// in the CLI. It is exported so the CLI package can reuse it without pulling
// in encoding/json itself.
func DatasetFromJSONReader(data []byte) (*model.NormalizedDataset, error) {
	var ds model.NormalizedDataset
	if err := json.Unmarshal(data, &ds); err != nil {
		return nil, err
	}
	return &ds, nil
}
