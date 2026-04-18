// Package model defines the shared data types that flow between the dataset
// loader, the matching engine, the Python trader adapter, and the CLI.
//
// The shapes mirror the Rust backtester types so that generated artifacts
// (metrics.json, bundle.json, submission.log, CSV exports) are
// byte-for-byte compatible with output produced by the reference project.
package model

import (
	"encoding/json"

	"github.com/darora1/imc-prosperity-4/backtester/internal/orderedmap"
)

// OrderBookLevel describes one price level of a half-book.
//
// Volumes are always stored as positive integers. The Rust code flips ask
// volumes to negative values only at the Python boundary; we keep the same
// representation here.
type OrderBookLevel struct {
	Price  int64 `json:"price"`
	Volume int64 `json:"volume"`
}

// ProductSnapshot captures the top levels of a product's order book for a
// single tick.
type ProductSnapshot struct {
	Product  string           `json:"product"`
	Bids     []OrderBookLevel `json:"bids,omitempty"`
	Asks     []OrderBookLevel `json:"asks,omitempty"`
	MidPrice *float64         `json:"mid_price"`
}

// MarketTrade is a trade reported by the exchange tape (not executed by
// the trader under test). Buyer/seller strings are the counterparty names
// surfaced in the IMC submission log. When no counterparty is available the
// strings are empty.
type MarketTrade struct {
	Symbol    string `json:"symbol"`
	Price     int64  `json:"price"`
	Quantity  int64  `json:"quantity"`
	Buyer     string `json:"buyer,omitempty"`
	Seller    string `json:"seller,omitempty"`
	Timestamp int64  `json:"timestamp,omitempty"`
}

// ObservationState mirrors the Python datamodel.Observation object. The
// plain bucket holds scalar observations keyed by product; the conversion
// bucket holds the per-product conversion observation fields.
type ObservationState struct {
	Plain      *orderedmap.Map[int64]                      `json:"plain,omitempty"`
	Conversion *orderedmap.Map[*orderedmap.Map[float64]]   `json:"conversion,omitempty"`
}

// IsEmpty reports whether the observation payload has no values attached.
func (o ObservationState) IsEmpty() bool {
	return o.Plain.Len() == 0 && o.Conversion.Len() == 0
}

// TickSnapshot is a single point in the normalized replay timeline. It owns
// all the book snapshots and market trades that map to the same
// (day, timestamp) pair.
type TickSnapshot struct {
	Timestamp    int64                                     `json:"timestamp"`
	Day          *int64                                    `json:"day"`
	Products     *orderedmap.Map[*ProductSnapshot]         `json:"products"`
	MarketTrades *orderedmap.Map[[]MarketTrade]            `json:"market_trades,omitempty"`
	Observations ObservationState                          `json:"observations,omitempty"`
}

// NormalizedDataset is the in-memory representation of one replay input
// regardless of whether the source was IMC CSV files, a submission log, or a
// previously normalized JSON file.
type NormalizedDataset struct {
	SchemaVersion      string                          `json:"schema_version"`
	CompetitionVersion string                          `json:"competition_version"`
	DatasetID          string                          `json:"dataset_id"`
	Source             string                          `json:"source"`
	Products           []string                        `json:"products"`
	Metadata           *orderedmap.Map[json.RawMessage] `json:"metadata,omitempty"`
	Ticks              []*TickSnapshot                 `json:"ticks"`
}

// Order is a trader-submitted order. Positive quantity = buy, negative = sell.
type Order struct {
	Symbol   string `json:"symbol"`
	Price    int64  `json:"price"`
	Quantity int64  `json:"quantity"`
}

// Trade is the unified representation used by the matching engine for both
// own trades (produced by the simulated trader) and residual market trades.
type Trade struct {
	Symbol    string `json:"symbol"`
	Price     int64  `json:"price"`
	Quantity  int64  `json:"quantity"`
	Buyer     string `json:"buyer"`
	Seller    string `json:"seller"`
	Timestamp int64  `json:"timestamp"`
}

// MatchingConfig controls how aggressively the engine fills against market
// trades observed on the tape. Values mirror the Rust CLI flags.
type MatchingConfig struct {
	// TradeMatchMode is "all" (default), "worse", or "none".
	TradeMatchMode string `json:"trade_match_mode"`
	// QueuePenetration scales the tape quantity that is eligible to fill.
	// 1.0 means every tape trade may consume the full quantity; 0.5 halves it.
	QueuePenetration float64 `json:"queue_penetration"`
	// PriceSlippageBps applies basis-point slippage to every own trade price.
	PriceSlippageBps float64 `json:"price_slippage_bps"`
}

// DefaultMatchingConfig returns the engine defaults used by the CLI when the
// user does not override them.
func DefaultMatchingConfig() MatchingConfig {
	return MatchingConfig{
		TradeMatchMode:   "all",
		QueuePenetration: 1.0,
		PriceSlippageBps: 0.0,
	}
}

// ModeIsNone is true when the configuration disables opportunistic fills
// against market trades. Own orders still sweep the visible book in all modes.
func (c MatchingConfig) ModeIsNone() bool {
	return c.TradeMatchMode == "none"
}

// MetadataOverrides lets callers inject deterministic values for fields that
// would otherwise default to "now" or to the dataset path. The CLI uses this
// for carry-mode runs where the recorded dataset path describes the group of
// inputs rather than any single file.
type MetadataOverrides struct {
	RunID                *string
	GeneratedAt          *string
	RecordedTraderPath   *string
	RecordedDatasetPath  *string
}

// RunRequest is the sole public input to the runner. The runner never mutates
// its input; it copies any state it needs internally.
type RunRequest struct {
	TraderFile           string
	DatasetFile          string
	DatasetOverride      *NormalizedDataset
	Day                  *int64
	Matching             MatchingConfig
	RunID                *string
	OutputRoot           string
	Persist              bool
	WriteMetrics         bool
	WriteBundle          bool
	WriteSubmissionLog   bool
	MaterializeArtifacts bool
	MetadataOverrides    MetadataOverrides
}

// RunMetrics is the compact summary emitted for every backtest run.
type RunMetrics struct {
	RunID             string                         `json:"run_id"`
	DatasetID         string                         `json:"dataset_id"`
	DatasetPath       string                         `json:"dataset_path"`
	TraderPath        string                         `json:"trader_path"`
	Day               *int64                         `json:"day"`
	Matching          MatchingConfig                 `json:"matching"`
	TickCount         int                            `json:"tick_count"`
	OwnTradeCount     int                            `json:"own_trade_count"`
	FinalPnLTotal     float64                        `json:"final_pnl_total"`
	FinalPnLByProduct *orderedmap.Map[float64]       `json:"final_pnl_by_product"`
	GeneratedAt       string                         `json:"generated_at"`
}

// ArtifactSet bundles the raw bytes of every optional output file. Callers
// decide which subset to persist based on the run request flags.
type ArtifactSet struct {
	MetricsJSON      []byte
	BundleJSON       []byte
	SubmissionLog    []byte
	ActivityCSV      []byte
	PnLByProductCSV  []byte
	CombinedLog      []byte
	TradesCSV        []byte
}

// RunOutput is what the runner returns to callers. RunDir points at the
// directory where any persisted artifacts were written (or would have been
// written, if no flags enabled persistence).
type RunOutput struct {
	RunID      string
	RunDir     string
	Metrics    RunMetrics
	ResultJSON []byte
	Artifacts  *ArtifactSet
}
