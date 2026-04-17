package dataset

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// writePair writes a prices/trades CSV pair to dir and returns the prices path.
func writePair(t *testing.T, dir, day, prices, trades string) string {
	t.Helper()
	priPath := filepath.Join(dir, "prices_round_0_day_"+day+".csv")
	trdPath := filepath.Join(dir, "trades_round_0_day_"+day+".csv")
	if err := os.WriteFile(priPath, []byte(prices), 0o644); err != nil {
		t.Fatalf("write prices: %v", err)
	}
	if err := os.WriteFile(trdPath, []byte(trades), 0o644); err != nil {
		t.Fatalf("write trades: %v", err)
	}
	return priPath
}

func TestLoadPriceCSV_Basic(t *testing.T) {
	dir := t.TempDir()
	prices := strings.Join([]string{
		"day;timestamp;product;bid_price_1;bid_volume_1;bid_price_2;bid_volume_2;bid_price_3;bid_volume_3;ask_price_1;ask_volume_1;ask_price_2;ask_volume_2;ask_price_3;ask_volume_3;mid_price;profit_and_loss",
		"-1;0;KELP;10;5;9;3;;;12;4;13;2;;;11.0;0.0",
		"-1;100;KELP;11;4;10;2;;;13;3;;;;;12.0;0.0",
	}, "\n")
	trades := strings.Join([]string{
		"timestamp;buyer;seller;symbol;currency;price;quantity",
		"0;A;B;KELP;SEASHELLS;11;2",
	}, "\n")
	priPath := writePair(t, dir, "-1", prices, trades)

	ds, err := Load(priPath)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if ds.SchemaVersion != "1.0" || ds.CompetitionVersion != "p4" {
		t.Fatalf("unexpected schema metadata: %+v", ds)
	}
	if len(ds.Ticks) != 2 {
		t.Fatalf("expected 2 ticks, got %d", len(ds.Ticks))
	}
	if len(ds.Products) != 1 || ds.Products[0] != "KELP" {
		t.Fatalf("expected KELP products list, got %v", ds.Products)
	}
	firstTick := ds.Ticks[0]
	if firstTick.Timestamp != 0 || firstTick.Day == nil || *firstTick.Day != -1 {
		t.Fatalf("first tick day/timestamp wrong: %+v", firstTick)
	}
	kelp, ok := firstTick.Products.Get("KELP")
	if !ok {
		t.Fatalf("KELP snapshot missing")
	}
	if len(kelp.Bids) != 2 || kelp.Bids[0].Price != 10 || kelp.Bids[0].Volume != 5 {
		t.Fatalf("unexpected bids: %+v", kelp.Bids)
	}
	if len(kelp.Asks) != 2 || kelp.Asks[0].Price != 12 {
		t.Fatalf("unexpected asks: %+v", kelp.Asks)
	}
	trade, ok := firstTick.MarketTrades.Get("KELP")
	if !ok || len(trade) != 1 || trade[0].Price != 11 || trade[0].Quantity != 2 {
		t.Fatalf("expected tape trade on first tick, got %+v ok=%v", trade, ok)
	}
	// Source format metadata should mark this as imc_csv for replay-mode inference.
	src, ok := ds.Metadata.Get("source_format")
	if !ok || strings.TrimSpace(string(src)) != `"imc_csv"` {
		t.Fatalf("expected source_format=imc_csv, got %q ok=%v", string(src), ok)
	}
}

func TestLoadPriceCSV_RejectsUnpaired(t *testing.T) {
	dir := t.TempDir()
	prices := "day;timestamp;product;bid_price_1;bid_volume_1;bid_price_2;bid_volume_2\n"
	path := filepath.Join(dir, "prices_x.csv")
	if err := os.WriteFile(path, []byte(prices), 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}
	if _, err := Load(path); err == nil {
		t.Fatalf("expected error for missing trades companion")
	}
}

func TestLoadPriceCSV_RejectsBadHeader(t *testing.T) {
	dir := t.TempDir()
	prices := "totally;wrong;header\n"
	trades := "timestamp;buyer;seller;symbol;currency;price;quantity\n"
	priPath := writePair(t, dir, "-2", prices, trades)
	if _, err := Load(priPath); err == nil {
		t.Fatalf("expected header error")
	}
}

func TestLoadUnsupportedExtension(t *testing.T) {
	if _, err := Load("data.txt"); err == nil {
		t.Fatalf("expected unsupported-format error")
	}
}

func TestParsePriceInt64_RoundsFloats(t *testing.T) {
	got, err := parsePriceInt64("9992.5")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if got != 9993 {
		t.Fatalf("expected 9993, got %d", got)
	}
}
