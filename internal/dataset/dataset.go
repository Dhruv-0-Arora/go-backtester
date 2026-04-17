// Package dataset loads IMC Prosperity replay inputs into a normalized in-memory
// representation. Three input formats are supported:
//
//  1. Normalized JSON produced by earlier runs of the backtester.
//  2. Paired IMC CSV files: prices_*.csv plus matching trades_*.csv.
//  3. Submission logs emitted by the IMC portal (.log or .json with an
//     activitiesLog string).
//
// The loader is deliberately strict about header shape so that accidental
// use of an unrelated CSV returns a clear error instead of silently producing
// an empty tick stream.
package dataset

import (
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"github.com/darora1/imc-prosperity-4/backtester/internal/model"
	"github.com/darora1/imc-prosperity-4/backtester/internal/orderedmap"
)

const (
	activityHeaderPrefix = "day;timestamp;product;bid_price_1;bid_volume_1;bid_price_2;bid_volume_2"
	tradeHeaderPrefix    = "timestamp;buyer;seller;symbol;currency;price;quantity"
)

// Load reads the file at path and returns the dataset. The dispatch is based
// on the file extension because the same underlying content sometimes lives
// in files with different suffixes (e.g. a submission JSON vs. log).
func Load(path string) (*model.NormalizedDataset, error) {
	ext := strings.ToLower(filepath.Ext(path))
	switch ext {
	case ".csv":
		return loadPriceCSV(path)
	case ".log":
		return loadSubmissionLog(path)
	case ".json":
		return loadJSON(path)
	default:
		return nil, fmt.Errorf("unsupported dataset format for %s; expected JSON, prices CSV, or submission log", path)
	}
}

// MaterializeSubmissionJSONIfMissing mirrors the Rust helper that writes a
// normalized JSON snapshot next to a submission .log. It returns the path
// to the new JSON when one was created, nil when the file already existed
// or the input was not a submission log.
func MaterializeSubmissionJSONIfMissing(path string) (string, error) {
	if strings.ToLower(filepath.Ext(path)) != ".log" {
		return "", nil
	}
	jsonPath := strings.TrimSuffix(path, filepath.Ext(path)) + ".json"
	if info, err := os.Stat(jsonPath); err == nil && !info.IsDir() {
		return "", nil
	}
	value, ok, err := readSubmissionLogPayload(path)
	if err != nil {
		return "", err
	}
	if !ok {
		return "", nil
	}
	dataset, err := buildFromSubmissionValue(path, value)
	if err != nil {
		return "", err
	}
	payload, err := json.MarshalIndent(dataset, "", "  ")
	if err != nil {
		return "", fmt.Errorf("failed to serialize normalized submission dataset for %s: %w", path, err)
	}
	payload = append(payload, '\n')
	if err := os.WriteFile(jsonPath, payload, 0o644); err != nil {
		return "", fmt.Errorf("failed to write normalized submission dataset %s: %w", jsonPath, err)
	}
	return jsonPath, nil
}

// loadJSON handles the normalized JSON format and also the submission log
// payload that occasionally ships with a .json extension.
func loadJSON(path string) (*model.NormalizedDataset, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read dataset file %s: %w", path, err)
	}
	var ds model.NormalizedDataset
	if err := json.Unmarshal(raw, &ds); err == nil && ds.SchemaVersion != "" && len(ds.Ticks) > 0 {
		return &ds, nil
	}
	var generic map[string]json.RawMessage
	if err := json.Unmarshal(raw, &generic); err != nil {
		return nil, fmt.Errorf("failed to parse dataset JSON %s: %w", path, err)
	}
	if activitiesLog, ok := generic["activitiesLog"]; ok {
		var s string
		if err := json.Unmarshal(activitiesLog, &s); err == nil {
			return buildFromSubmissionValue(path, generic)
		}
	}
	return nil, fmt.Errorf("failed to parse supported dataset JSON %s", path)
}

func loadSubmissionLog(path string) (*model.NormalizedDataset, error) {
	value, ok, err := readSubmissionLogPayload(path)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, fmt.Errorf("failed to parse submission log JSON %s", path)
	}
	return buildFromSubmissionValue(path, value)
}

// readSubmissionLogPayload returns the decoded JSON object at path when the
// file is a submission log (i.e. it contains an "activitiesLog" string).
// Callers can distinguish "file exists but isn't a submission log" via ok=false.
func readSubmissionLogPayload(path string) (map[string]json.RawMessage, bool, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, false, fmt.Errorf("failed to read log %s: %w", path, err)
	}
	var generic map[string]json.RawMessage
	if err := json.Unmarshal(raw, &generic); err != nil {
		return nil, false, nil
	}
	activitiesLog, ok := generic["activitiesLog"]
	if !ok {
		return nil, false, nil
	}
	var s string
	if err := json.Unmarshal(activitiesLog, &s); err != nil {
		return nil, false, nil
	}
	return generic, true, nil
}

type submissionTradeRow struct {
	day   *int64
	trade model.MarketTrade
}

func buildFromSubmissionValue(path string, value map[string]json.RawMessage) (*model.NormalizedDataset, error) {
	raw, ok := value["activitiesLog"]
	if !ok {
		return nil, fmt.Errorf("submission payload missing activitiesLog in %s", path)
	}
	var activitiesLog string
	if err := json.Unmarshal(raw, &activitiesLog); err != nil {
		return nil, fmt.Errorf("submission activitiesLog in %s is not a string: %w", path, err)
	}
	var history []submissionTradeRow
	if rawHistory, ok := value["tradeHistory"]; ok {
		var rows []map[string]json.RawMessage
		if err := json.Unmarshal(rawHistory, &rows); err != nil {
			return nil, fmt.Errorf("submission tradeHistory in %s is not an array: %w", path, err)
		}
		history = make([]submissionTradeRow, 0, len(rows))
		for _, row := range rows {
			entry, err := parseSubmissionHistoryRow(row)
			if err != nil {
				return nil, err
			}
			history = append(history, entry)
		}
	}
	metadata := orderedmap.New[json.RawMessage]()
	metadata.Set("built_from", mustJSON(pathString(path)))

	return buildDatasetFromActivities(
		path,
		submissionDatasetIDFromPath(path),
		pathString(path),
		activitiesLog,
		history,
		metadata,
	)
}

func parseSubmissionHistoryRow(row map[string]json.RawMessage) (submissionTradeRow, error) {
	var out submissionTradeRow
	if rawDay, ok := row["day"]; ok {
		var d *int64
		var intVal int64
		if err := json.Unmarshal(rawDay, &intVal); err == nil {
			d = &intVal
			out.day = d
		}
	}
	getStr := func(key string) (string, error) {
		raw, ok := row[key]
		if !ok {
			return "", nil
		}
		var s string
		if err := json.Unmarshal(raw, &s); err != nil {
			return "", fmt.Errorf("tradeHistory row field %s not a string", key)
		}
		return s, nil
	}
	symbol, _ := getStr("symbol")
	buyer, _ := getStr("buyer")
	seller, _ := getStr("seller")

	var qty int64
	if raw, ok := row["quantity"]; ok {
		if err := json.Unmarshal(raw, &qty); err != nil {
			return out, fmt.Errorf("tradeHistory row quantity not an integer: %w", err)
		}
	} else {
		return out, fmt.Errorf("tradeHistory row missing quantity")
	}

	var ts int64
	if raw, ok := row["timestamp"]; ok {
		if err := json.Unmarshal(raw, &ts); err != nil {
			return out, fmt.Errorf("tradeHistory row timestamp not an integer: %w", err)
		}
	} else {
		return out, fmt.Errorf("tradeHistory row missing timestamp")
	}

	price, err := parseTradePriceJSON(row["price"])
	if err != nil {
		return out, err
	}

	out.trade = model.MarketTrade{
		Symbol:    symbol,
		Price:     price,
		Quantity:  qty,
		Buyer:     buyer,
		Seller:    seller,
		Timestamp: ts,
	}
	return out, nil
}

// parseTradePriceJSON accepts prices encoded as integers, floats, or strings.
// Older submission logs sometimes encode volumes as strings ("9992.0").
func parseTradePriceJSON(raw json.RawMessage) (int64, error) {
	if len(raw) == 0 {
		return 0, fmt.Errorf("tradeHistory row missing price")
	}
	var asInt int64
	if err := json.Unmarshal(raw, &asInt); err == nil {
		return asInt, nil
	}
	var asFloat float64
	if err := json.Unmarshal(raw, &asFloat); err == nil {
		return int64(math.Round(asFloat)), nil
	}
	var asStr string
	if err := json.Unmarshal(raw, &asStr); err == nil {
		return parsePriceInt64(asStr)
	}
	return 0, fmt.Errorf("tradeHistory row has unsupported price value")
}

func loadPriceCSV(path string) (*model.NormalizedDataset, error) {
	base := filepath.Base(path)
	if !strings.HasPrefix(base, "prices_") {
		return nil, fmt.Errorf("unsupported CSV input %s; pass a prices_*.csv file or a directory containing IMC CSV files", path)
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read prices CSV %s: %w", path, err)
	}
	tradePath := filepath.Join(filepath.Dir(path), strings.Replace(base, "prices_", "trades_", 1))
	if info, err := os.Stat(tradePath); err != nil || info.IsDir() {
		return nil, fmt.Errorf("missing paired trades CSV for %s; expected %s", path, tradePath)
	}
	trades, err := loadTradesCSV(tradePath)
	if err != nil {
		return nil, err
	}
	history := make([]submissionTradeRow, 0, len(trades))
	for _, t := range trades {
		history = append(history, submissionTradeRow{trade: t})
	}
	metadata := orderedmap.New[json.RawMessage]()
	metadata.Set("source_format", mustJSON("imc_csv"))
	metadata.Set("trade_rows", mustJSON(len(history)))

	return buildDatasetFromActivities(
		path,
		datasetIDFromPath(path),
		fmt.Sprintf("imc_csv:%s", base),
		string(raw),
		history,
		metadata,
	)
}

// buildDatasetFromActivities is the common parser used by both the price-CSV
// and submission-log paths. activitiesLog is the raw text of the IMC-format
// activities log (semicolon-separated values, always including a header).
func buildDatasetFromActivities(
	path string,
	datasetID string,
	source string,
	activitiesLog string,
	history []submissionTradeRow,
	metadata *orderedmap.Map[json.RawMessage],
) (*model.NormalizedDataset, error) {
	type tickKey struct {
		day       int64
		hasDay    bool
		timestamp int64
	}
	productsSeen := orderedmap.New[struct{}]()
	ticks := map[tickKey]*model.TickSnapshot{}
	var orderedKeys []tickKey
	activityRows := int64(0)

	lines := strings.Split(activitiesLog, "\n")
	for lineNo, line := range lines {
		if lineNo == 0 {
			if !strings.HasPrefix(line, activityHeaderPrefix) {
				return nil, fmt.Errorf("unexpected activities header in %s", path)
			}
			continue
		}
		if strings.TrimSpace(line) == "" {
			continue
		}
		fields := strings.Split(line, ";")
		if len(fields) < 17 {
			return nil, fmt.Errorf("invalid activities row %d in %s; expected at least 17 columns", lineNo+1, path)
		}
		day, dayOK, err := parseOptionalInt64(fields[0])
		if err != nil {
			return nil, err
		}
		timestamp, err := parseRequiredInt64(fields[1], "timestamp")
		if err != nil {
			return nil, err
		}
		product := strings.TrimSpace(fields[2])
		if product == "" {
			return nil, fmt.Errorf("missing product in activities row %d of %s", lineNo+1, path)
		}
		bids, err := parseBookSide(fields, [][2]int{{3, 4}, {5, 6}, {7, 8}})
		if err != nil {
			return nil, err
		}
		asks, err := parseBookSide(fields, [][2]int{{9, 10}, {11, 12}, {13, 14}})
		if err != nil {
			return nil, err
		}
		midPrice, err := parseOptionalFloat64(fields[15])
		if err != nil {
			return nil, err
		}
		snapshot := &model.ProductSnapshot{
			Product:  product,
			Bids:     bids,
			Asks:     asks,
			MidPrice: midPrice,
		}
		activityRows++
		productsSeen.Set(product, struct{}{})

		key := tickKey{timestamp: timestamp, hasDay: dayOK}
		if dayOK {
			key.day = day
		}
		tick, ok := ticks[key]
		if !ok {
			var dayPtr *int64
			if dayOK {
				d := day
				dayPtr = &d
			}
			tick = &model.TickSnapshot{
				Timestamp:    timestamp,
				Day:          dayPtr,
				Products:     orderedmap.New[*model.ProductSnapshot](),
				MarketTrades: orderedmap.New[[]model.MarketTrade](),
			}
			ticks[key] = tick
			orderedKeys = append(orderedKeys, key)
		}
		tick.Products.Set(product, snapshot)
	}
	if len(orderedKeys) == 0 {
		return nil, fmt.Errorf("no tick rows found in %s", path)
	}
	sort.Slice(orderedKeys, func(i, j int) bool {
		a, b := orderedKeys[i], orderedKeys[j]
		if a.hasDay != b.hasDay {
			// Missing day sorts before known days, matching BTreeMap's
			// Option<i64> ordering where None < Some(_).
			return !a.hasDay && b.hasDay
		}
		if a.day != b.day {
			return a.day < b.day
		}
		return a.timestamp < b.timestamp
	})

	// Bucket trade history by (day, timestamp) so it can be attached to the
	// matching tick. A nil day key falls back to any tick with a matching
	// timestamp regardless of day.
	type tradeKey struct {
		day       int64
		hasDay    bool
		timestamp int64
	}
	tradesByKey := map[tradeKey]*orderedmap.Map[[]model.MarketTrade]{}
	for _, entry := range history {
		key := tradeKey{timestamp: entry.trade.Timestamp}
		if entry.day != nil {
			key.day = *entry.day
			key.hasDay = true
		}
		bucket, ok := tradesByKey[key]
		if !ok {
			bucket = orderedmap.New[[]model.MarketTrade]()
			tradesByKey[key] = bucket
		}
		existing, _ := bucket.Get(entry.trade.Symbol)
		bucket.Set(entry.trade.Symbol, append(existing, entry.trade))
	}

	orderedTicks := make([]*model.TickSnapshot, 0, len(orderedKeys))
	for _, k := range orderedKeys {
		tick := ticks[k]
		// Prefer bucket matching (day, timestamp); fall back to (None, timestamp).
		primary := tradeKey{timestamp: tick.Timestamp, hasDay: k.hasDay, day: k.day}
		if bucket, ok := tradesByKey[primary]; ok {
			tick.MarketTrades = bucket
			delete(tradesByKey, primary)
		} else {
			fallback := tradeKey{timestamp: tick.Timestamp}
			if bucket, ok := tradesByKey[fallback]; ok {
				tick.MarketTrades = bucket
				delete(tradesByKey, fallback)
			}
		}
		orderedTicks = append(orderedTicks, tick)
	}

	productNames := productsSeen.Keys()
	sort.Strings(productNames)

	fullMetadata := orderedmap.New[json.RawMessage]()
	fullMetadata.Set("activity_rows", mustJSON(activityRows))
	if metadata != nil {
		for _, p := range metadata.Entries() {
			fullMetadata.Set(p.Key, p.Value)
		}
	}
	if !fullMetadata.Has("trade_rows") {
		fullMetadata.Set("trade_rows", mustJSON(int64(len(history))))
	}

	return &model.NormalizedDataset{
		SchemaVersion:      "1.0",
		CompetitionVersion: "p4",
		DatasetID:          datasetID,
		Source:             source,
		Products:           productNames,
		Metadata:           fullMetadata,
		Ticks:              orderedTicks,
	}, nil
}

func parseBookSide(fields []string, pairs [][2]int) ([]model.OrderBookLevel, error) {
	levels := make([]model.OrderBookLevel, 0, len(pairs))
	for _, pair := range pairs {
		priceIdx, volumeIdx := pair[0], pair[1]
		if priceIdx >= len(fields) || volumeIdx >= len(fields) {
			continue
		}
		priceText := fields[priceIdx]
		volumeText := fields[volumeIdx]
		if strings.TrimSpace(priceText) == "" || strings.TrimSpace(volumeText) == "" {
			continue
		}
		price, err := parsePriceInt64(priceText)
		if err != nil {
			return nil, err
		}
		volume, err := parseRequiredInt64(volumeText, "volume")
		if err != nil {
			return nil, err
		}
		levels = append(levels, model.OrderBookLevel{Price: price, Volume: volume})
	}
	return levels, nil
}

func loadTradesCSV(path string) ([]model.MarketTrade, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read trades CSV %s: %w", path, err)
	}
	var trades []model.MarketTrade
	for lineNo, line := range strings.Split(string(raw), "\n") {
		if lineNo == 0 {
			if !strings.HasPrefix(line, tradeHeaderPrefix) {
				return nil, fmt.Errorf("unexpected trades header in %s", path)
			}
			continue
		}
		if strings.TrimSpace(line) == "" {
			continue
		}
		fields := strings.Split(line, ";")
		if len(fields) < 7 {
			return nil, fmt.Errorf("invalid trades row %d in %s; expected 7 columns", lineNo+1, path)
		}
		ts, err := parseRequiredInt64(fields[0], "timestamp")
		if err != nil {
			return nil, err
		}
		price, err := parsePriceInt64(fields[5])
		if err != nil {
			return nil, err
		}
		qty, err := parseRequiredInt64(fields[6], "quantity")
		if err != nil {
			return nil, err
		}
		trades = append(trades, model.MarketTrade{
			Timestamp: ts,
			Buyer:     strings.TrimSpace(fields[1]),
			Seller:    strings.TrimSpace(fields[2]),
			Symbol:    strings.TrimSpace(fields[3]),
			Price:     price,
			Quantity:  qty,
		})
	}
	return trades, nil
}

func parseOptionalInt64(value string) (int64, bool, error) {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return 0, false, nil
	}
	v, err := strconv.ParseInt(trimmed, 10, 64)
	if err != nil {
		return 0, false, fmt.Errorf("failed to parse integer value %q: %w", value, err)
	}
	return v, true, nil
}

func parseRequiredInt64(value, field string) (int64, error) {
	v, err := strconv.ParseInt(strings.TrimSpace(value), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("failed to parse %s value %q: %w", field, value, err)
	}
	return v, nil
}

func parseOptionalFloat64(value string) (*float64, error) {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return nil, nil
	}
	v, err := strconv.ParseFloat(trimmed, 64)
	if err != nil {
		return nil, fmt.Errorf("failed to parse float value %q: %w", value, err)
	}
	return &v, nil
}

func parsePriceInt64(value string) (int64, error) {
	f, err := strconv.ParseFloat(strings.TrimSpace(value), 64)
	if err != nil {
		return 0, fmt.Errorf("failed to parse price value %q: %w", value, err)
	}
	return int64(math.Round(f)), nil
}

func datasetIDFromPath(path string) string {
	stem := strings.TrimSuffix(filepath.Base(path), filepath.Ext(path))
	if stem == "" {
		return "dataset"
	}
	return stem
}

func submissionDatasetIDFromPath(path string) string {
	stem := datasetIDFromPath(path)
	if stem == "" {
		return stem
	}
	allDigits := true
	for _, r := range stem {
		if r < '0' || r > '9' {
			allDigits = false
			break
		}
	}
	if allDigits {
		return fmt.Sprintf("official_submission_%s_alltrades", stem)
	}
	return stem
}

func pathString(path string) string {
	return strings.ReplaceAll(path, "\\", "/")
}

// mustJSON panics if v cannot be marshalled. The callers only ever use
// values that encoding/json handles by construction.
func mustJSON(v any) json.RawMessage {
	b, err := json.Marshal(v)
	if err != nil {
		panic(fmt.Sprintf("jsonfmt: failed to marshal internal value: %v", err))
	}
	return b
}
