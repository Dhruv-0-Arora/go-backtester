// Package cli provides the command-line entry point for the Go backtester.
//
// The flag surface is a compatible subset of the reference Rust CLI. Key
// options:
//
//	--trader <file.py>         path to the trader module (auto-detected by default)
//	--dataset <path-or-alias>  dataset file, directory, or alias such as "tutorial"
//	--day <int>                run only the specified day from the selected bundle
//	--run-id <string>          deterministic run identifier
//	--trade-match-mode <mode>  "all" (default) | "worse" | "none"
//	--queue-penetration <f>    tape fill scaling factor (default 1.0)
//	--price-slippage-bps <f>   basis-point slippage per own fill (default 0)
//	--output-root <dir>        where to write per-run artifact directories
//	--persist                  write the full artifact set under runs/<id>/
//	--artifact-mode <mode>     none | submission | diagnostic | full
//	--flat                     write multi-run outputs into a single flat directory
//	--carry                    carry positions/state across consecutive day datasets
//	--products <mode>          off | summary (default) | full
package cli

import (
	"context"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/darora1/imc-prosperity-4/backtester/internal/dataset"
	"github.com/darora1/imc-prosperity-4/backtester/internal/engine"
	"github.com/darora1/imc-prosperity-4/backtester/internal/model"
)

// Run parses os.Args and executes the CLI. It is separated from main so
// tests can invoke it with a pre-built argv if needed.
func Run() error {
	fs := flag.NewFlagSet("backtester", flag.ContinueOnError)
	trader := fs.String("trader", "", "path to the Python trader file (.py)")
	datasetArg := fs.String("dataset", "", "dataset path or alias (defaults to the latest populated round)")
	dayArg := fs.String("day", "", "optional day filter (e.g. -1)")
	runID := fs.String("run-id", "", "explicit run identifier")
	matchMode := fs.String("trade-match-mode", "all", "trade-match mode: all|worse|none")
	queuePen := fs.Float64("queue-penetration", 1.0, "queue penetration factor for tape fills")
	slippage := fs.Float64("price-slippage-bps", 0.0, "basis-point slippage applied to own fills")
	outputRoot := fs.String("output-root", "", "directory where per-run artifacts are stored")
	persist := fs.Bool("persist", false, "persist the full artifact set")
	artifactMode := fs.String("artifact-mode", "", "artifact mode: none|submission|diagnostic|full")
	carry := fs.Bool("carry", false, "carry state across consecutive day datasets")
	flatLayout := fs.Bool("flat", false, "write multi-run outputs into a single flat directory")
	productsMode := fs.String("products", "summary", "product table mode: off|summary|full")

	if err := fs.Parse(os.Args[1:]); err != nil {
		return err
	}

	traderPath, traderAuto, err := resolveTrader(*trader)
	if err != nil {
		return err
	}
	datasetSel, err := resolveDataset(*datasetArg)
	if err != nil {
		return err
	}
	root := *outputRoot
	if root == "" {
		root = defaultOutputRoot()
	}

	dayFilter, err := parseDayArg(*dayArg)
	if err != nil {
		return err
	}

	runIDSeed := strings.TrimSpace(*runID)
	if runIDSeed == "" {
		runIDSeed = fmt.Sprintf("backtest-%d", time.Now().UnixMilli())
	}

	plans, err := buildRunPlan(datasetSel, dayFilter, runIDSeed, *carry)
	if err != nil {
		return err
	}
	flatLayoutActive := *flatLayout && len(plans) > 1
	var flatDir string
	if flatLayoutActive {
		flatDir = filepath.Join(root, runIDSeed)
		if err := resetFlatOutputDir(flatDir); err != nil {
			return err
		}
	}
	mode := resolveArtifactMode(*artifactMode, *persist)
	persistFlag, writeMetrics, writeBundle, writeSubmissionLog, materialize := artifactModeSettings(mode)
	matching := model.MatchingConfig{
		TradeMatchMode:   *matchMode,
		QueuePenetration: *queuePen,
		PriceSlippageBps: *slippage,
	}

	var rows []summaryRow
	var outputs []*model.RunOutput
	for _, plan := range plans {
		runIDStr := plan.runID
		req := &model.RunRequest{
			TraderFile:           traderPath,
			DatasetFile:          plan.datasetFile,
			DatasetOverride:      plan.datasetOverride,
			Day:                  plan.day,
			Matching:             matching,
			RunID:                &runIDStr,
			OutputRoot:           root,
			Persist:              persistFlag,
			WriteMetrics:         writeMetrics,
			WriteBundle:          writeBundle,
			WriteSubmissionLog:   writeSubmissionLog,
			MaterializeArtifacts: materialize,
			MetadataOverrides:    plan.metadata,
		}
		output, err := engine.Run(context.Background(), req)
		if err != nil {
			return fmt.Errorf("run for dataset %s failed: %w", plan.datasetFile, err)
		}

		runDirLabel := displayPath(output.RunDir)
		if flatLayoutActive {
			if err := writeFlatRunArtifacts(flatDir, plan.artifactPrefix, output); err != nil {
				return err
			}
			_ = os.RemoveAll(output.RunDir)
			runDirLabel = fmt.Sprintf("%s/%s-*", displayPath(flatDir), plan.artifactPrefix)
		}
		rows = append(rows, summaryRow{
			dataset:           plan.summaryLabel,
			day:               output.Metrics.Day,
			tickCount:         output.Metrics.TickCount,
			ownTradeCount:     output.Metrics.OwnTradeCount,
			finalPnLTotal:     output.Metrics.FinalPnLTotal,
			finalPnLByProduct: output.Metrics.FinalPnLByProduct.Clone(),
			runDir:            runDirLabel,
		})
		outputs = append(outputs, output)
	}

	var bundleDir string
	if flatLayoutActive {
		if err := writeFlatManifest(flatDir, traderPath, traderAuto, datasetSel, rows); err != nil {
			return err
		}
		bundleDir = displayPath(flatDir)
	}

	printSummary(summaryArgs{
		trader:       traderPath,
		traderAuto:   traderAuto,
		dataset:      datasetSel,
		artifactMode: mode,
		products:     parseProductsMode(*productsMode),
		bundleDir:    bundleDir,
		flatLayout:   flatLayoutActive,
		rows:         rows,
	})
	return nil
}

// defaultOutputRoot returns runs/ next to the current working directory.
func defaultOutputRoot() string {
	cwd, err := os.Getwd()
	if err != nil {
		return "runs"
	}
	return filepath.Join(cwd, "runs")
}

func parseDayArg(raw string) (*int64, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" || strings.EqualFold(raw, "all") {
		return nil, nil
	}
	v, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return nil, fmt.Errorf("--day must be an integer or 'all', got %q", raw)
	}
	return &v, nil
}

func resolveTrader(requested string) (string, bool, error) {
	if requested != "" {
		abs, err := filepath.Abs(requested)
		if err != nil {
			return "", false, fmt.Errorf("failed to resolve trader %s: %w", requested, err)
		}
		if info, err := os.Stat(abs); err != nil || info.IsDir() {
			return "", false, fmt.Errorf("trader %s is not a regular file", requested)
		}
		return abs, false, nil
	}
	for _, root := range candidateTraderRoots() {
		path, err := latestTraderCandidate(root)
		if err != nil {
			return "", false, err
		}
		if path != "" {
			return path, true, nil
		}
	}
	return "", false, fmt.Errorf("no trader file found automatically; pass --trader <file.py> or place a Trader class in scripts/ or traders/")
}

func candidateTraderRoots() []string {
	cwd, err := os.Getwd()
	if err != nil {
		return nil
	}
	out := []string{}
	for _, rel := range []string{"scripts", filepath.Join("traders", "submissions"), "traders"} {
		candidate := filepath.Join(cwd, rel)
		if info, err := os.Stat(candidate); err == nil && info.IsDir() {
			out = append(out, candidate)
		}
	}
	return out
}

func latestTraderCandidate(root string) (string, error) {
	var best string
	var bestTime time.Time
	var walk func(dir string) error
	walk = func(dir string) error {
		entries, err := os.ReadDir(dir)
		if err != nil {
			return fmt.Errorf("failed to read trader directory %s: %w", dir, err)
		}
		for _, entry := range entries {
			path := filepath.Join(dir, entry.Name())
			if entry.IsDir() {
				if err := walk(path); err != nil {
					return err
				}
				continue
			}
			if !strings.HasSuffix(entry.Name(), ".py") {
				continue
			}
			bytes, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			if !strings.Contains(string(bytes), "class Trader") {
				continue
			}
			info, err := entry.Info()
			if err != nil {
				return err
			}
			if best == "" || info.ModTime().After(bestTime) ||
				(info.ModTime().Equal(bestTime) && path < best) {
				best = path
				bestTime = info.ModTime()
			}
		}
		return nil
	}
	if err := walk(root); err != nil {
		return "", err
	}
	return best, nil
}

func resolveArtifactMode(raw string, persist bool) string {
	raw = strings.ToLower(strings.TrimSpace(raw))
	if raw != "" {
		return raw
	}
	if persist {
		return "full"
	}
	return "submission"
}

func artifactModeSettings(mode string) (persist, writeMetrics, writeBundle, writeSubmissionLog, materialize bool) {
	switch mode {
	case "none":
		return false, true, false, false, false
	case "diagnostic":
		return false, true, true, false, false
	case "full":
		return true, true, true, true, true
	default: // "submission" or unknown
		return false, true, false, true, false
	}
}

func parseProductsMode(raw string) productsMode {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "off":
		return productsOff
	case "full":
		return productsFull
	default:
		return productsSummary
	}
}

// collectDatasetFiles returns every supported dataset inside a directory, or
// the single file itself if `path` is a file.
func collectDatasetFiles(path string) ([]string, error) {
	info, err := os.Stat(path)
	if err != nil {
		return nil, fmt.Errorf("dataset path does not exist: %s", path)
	}
	if !info.IsDir() {
		if _, err := dataset.MaterializeSubmissionJSONIfMissing(path); err != nil {
			return nil, err
		}
		return []string{path}, nil
	}

	// Ensure submission logs get json companions up front.
	entries, err := os.ReadDir(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read dataset directory %s: %w", path, err)
	}
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		full := filepath.Join(path, entry.Name())
		if _, err := dataset.MaterializeSubmissionJSONIfMissing(full); err != nil {
			return nil, err
		}
	}

	type candidate struct {
		rank int
		path string
	}
	selected := map[string]candidate{}
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		full := filepath.Join(path, entry.Name())
		key, ok := datasetCandidateKey(full)
		if !ok {
			continue
		}
		rank := datasetCandidateRank(full)
		if cur, exists := selected[key]; !exists || rank > cur.rank ||
			(rank == cur.rank && full < cur.path) {
			selected[key] = candidate{rank: rank, path: full}
		}
	}
	out := make([]string, 0, len(selected))
	for _, c := range selected {
		out = append(out, c.path)
	}
	sort.Strings(out)
	if len(out) == 0 {
		return nil, fmt.Errorf("no supported datasets found in %s", path)
	}
	return out, nil
}

func datasetCandidateKey(path string) (string, bool) {
	base := strings.ToLower(filepath.Base(path))
	if isSubmissionCandidatePath(path) {
		return "submission", true
	}
	if strings.HasPrefix(base, "trades_") && strings.HasSuffix(base, ".csv") {
		return "", false
	}
	if strings.HasPrefix(base, "prices_") && strings.HasSuffix(base, ".csv") {
		if key := dayKeyFromName(base); key != "" {
			return key, true
		}
		return strings.TrimSuffix(base, filepath.Ext(base)), true
	}
	switch filepath.Ext(base) {
	case ".log":
		return "", false
	case ".json":
		return strings.TrimSuffix(base, filepath.Ext(base)), true
	}
	return "", false
}

func datasetCandidateRank(path string) int {
	if isSubmissionCandidatePath(path) {
		base := strings.ToLower(filepath.Base(path))
		switch base {
		case "submission.json":
			return 4
		case "submission.log":
			return 2
		}
		if strings.HasSuffix(base, ".json") {
			return 3
		}
		return 1
	}
	base := strings.ToLower(filepath.Base(path))
	if strings.HasSuffix(base, ".log") {
		return 3
	}
	if strings.HasPrefix(base, "prices_") && strings.HasSuffix(base, ".csv") {
		return 2
	}
	if strings.HasSuffix(base, ".json") {
		return 1
	}
	return 0
}

func dayKeyFromName(name string) string {
	lower := strings.ToLower(name)
	idx := strings.Index(lower, "day_")
	if idx < 0 {
		return ""
	}
	suffix := lower[idx+4:]
	end := 0
	for end < len(suffix) {
		c := suffix[end]
		if !(c >= '0' && c <= '9') && c != '-' {
			break
		}
		end++
	}
	if end == 0 {
		return ""
	}
	return "day_" + suffix[:end]
}

func isSubmissionCandidatePath(path string) bool {
	return isSubmissionLikePath(path) && dayKeyFromName(strings.ToLower(filepath.Base(path))) == ""
}

func isSubmissionLikePath(path string) bool {
	base := strings.ToLower(filepath.Base(path))
	ext := filepath.Ext(base)
	stem := strings.TrimSuffix(base, ext)
	switch ext {
	case ".log":
		return isSubmissionStem(stem)
	case ".json":
		if isSubmissionStem(stem) {
			return true
		}
		logCompanion := strings.TrimSuffix(path, ext) + ".log"
		if _, err := os.Stat(logCompanion); err == nil {
			return isSubmissionStem(strings.TrimSuffix(filepath.Base(logCompanion), ".log"))
		}
	}
	return false
}

func isSubmissionStem(stem string) bool {
	if strings.Contains(stem, "submission") {
		return true
	}
	if stem == "" {
		return false
	}
	for _, r := range stem {
		if r < '0' || r > '9' {
			return false
		}
	}
	return true
}

func displayPath(path string) string {
	cwd, err := os.Getwd()
	if err != nil {
		return filepath.ToSlash(path)
	}
	if rel, err := filepath.Rel(cwd, path); err == nil && !strings.HasPrefix(rel, "..") {
		return filepath.ToSlash(rel)
	}
	return filepath.ToSlash(path)
}

func resetFlatOutputDir(dir string) error {
	if info, err := os.Stat(dir); err == nil && info.IsDir() {
		if err := os.RemoveAll(dir); err != nil {
			return fmt.Errorf("failed to replace flat output directory %s: %w", dir, err)
		}
	}
	return os.MkdirAll(dir, 0o755)
}

func writeFlatRunArtifacts(flatDir, prefix string, output *model.RunOutput) error {
	if output.Artifacts == nil {
		return fmt.Errorf("flat output requires materialized artifacts")
	}
	files := []struct {
		name string
		data []byte
	}{
		{"metrics.json", output.Artifacts.MetricsJSON},
		{"bundle.json", output.Artifacts.BundleJSON},
		{"submission.log", output.Artifacts.SubmissionLog},
		{"activity.csv", output.Artifacts.ActivityCSV},
		{"pnl_by_product.csv", output.Artifacts.PnLByProductCSV},
		{"combined.log", output.Artifacts.CombinedLog},
		{"trades.csv", output.Artifacts.TradesCSV},
	}
	for _, f := range files {
		if len(f.data) == 0 {
			continue
		}
		path := filepath.Join(flatDir, fmt.Sprintf("%s-%s", prefix, f.name))
		if err := os.WriteFile(path, f.data, 0o644); err != nil {
			return fmt.Errorf("failed to write flat artifact %s: %w", path, err)
		}
	}
	return nil
}

func writeFlatManifest(flatDir, traderPath string, traderAuto bool, sel datasetSelection, rows []summaryRow) error {
	manifestRows := make([]map[string]any, 0, len(rows))
	for _, row := range rows {
		entry := map[string]any{
			"dataset":         row.dataset,
			"day":             row.day,
			"tick_count":      row.tickCount,
			"own_trade_count": row.ownTradeCount,
			"final_pnl":       row.finalPnLTotal,
			"run_dir":         row.runDir,
		}
		if row.finalPnLByProduct != nil {
			entry["final_pnl_by_product"] = row.finalPnLByProduct
		}
		manifestRows = append(manifestRows, entry)
	}
	manifest := map[string]any{
		"trader":      traderPath,
		"trader_auto": traderAuto,
		"dataset":     sel.label,
		"runs":        manifestRows,
	}
	return writeJSON(filepath.Join(flatDir, "manifest.json"), manifest)
}
