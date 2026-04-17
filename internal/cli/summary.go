package cli

import (
	"fmt"
	"path/filepath"
	"sort"

	"github.com/darora1/imc-prosperity-4/backtester/internal/orderedmap"
)

// summaryRow is one entry in the final CLI summary table. It is also used as
// input to the flat-bundle manifest.
type summaryRow struct {
	dataset           string
	day               *int64
	tickCount         int
	ownTradeCount     int
	finalPnLTotal     float64
	finalPnLByProduct *orderedmap.Map[float64]
	runDir            string
}

// productsMode controls how the per-product table is rendered beneath the
// main summary.
type productsMode int

const (
	productsSummary productsMode = iota
	productsFull
	productsOff
)

type summaryArgs struct {
	trader       string
	traderAuto   bool
	dataset      datasetSelection
	artifactMode string
	products     productsMode
	bundleDir    string
	flatLayout   bool
	rows         []summaryRow
}

// printSummary writes the human-readable output block described in the
// reference README.
func printSummary(args summaryArgs) {
	traderLabel := filepath.Base(args.trader)
	suffix := ""
	if args.traderAuto {
		suffix = " [auto]"
	}
	fmt.Printf("trader: %s%s\n", traderLabel, suffix)

	datasetSuffix := ""
	if args.dataset.autoSelected {
		datasetSuffix = " [default]"
	}
	fmt.Printf("dataset: %s%s\n", args.dataset.label, datasetSuffix)
	fmt.Println("mode: fast")

	label := "log-only"
	switch args.artifactMode {
	case "none":
		label = "metrics-only"
	case "diagnostic":
		label = "metrics+pnl-series"
	case "full":
		label = "saved"
	}
	fmt.Printf("artifacts: %s\n", label)
	if args.bundleDir != "" {
		if args.flatLayout {
			fmt.Printf("bundle: %s [flat multi-run output]\n", args.bundleDir)
		} else {
			fmt.Printf("bundle: %s [manifest+combined logs only]\n", args.bundleDir)
		}
	}

	fmt.Printf("%-12s %6s %8s %11s %12s  RUN_DIR\n",
		"SET", "DAY", "TICKS", "OWN_TRADES", "FINAL_PNL")
	for _, row := range args.rows {
		fmt.Printf("%-12s %6s %8d %11d %12.2f  %s\n",
			row.dataset,
			renderDay(row.day),
			row.tickCount,
			row.ownTradeCount,
			row.finalPnLTotal,
			row.runDir,
		)
	}
	if len(args.rows) > 1 {
		var tickSum, tradeSum int
		var pnlSum float64
		for _, row := range args.rows {
			tickSum += row.tickCount
			tradeSum += row.ownTradeCount
			pnlSum += row.finalPnLTotal
		}
		fmt.Printf("%-12s %6s %8d %11d %12.2f  %s\n",
			"TOTAL", "-", tickSum, tradeSum, pnlSum, "-")
	}
	printProductTable(args.rows, args.products)
}

func renderDay(day *int64) string {
	if day == nil {
		return "all"
	}
	return fmt.Sprintf("%d", *day)
}

// printProductTable emits the secondary per-product PnL table. In summary
// mode the top 8 contributors are shown with an OTHER(+N) rollup; full mode
// shows every product; off mode suppresses the table entirely.
func printProductTable(rows []summaryRow, mode productsMode) {
	if mode == productsOff {
		return
	}
	type productTotal struct {
		product string
		total   float64
		values  []float64
	}
	productIndex := map[string]int{}
	var all []productTotal
	for runIdx, row := range rows {
		if row.finalPnLByProduct == nil {
			continue
		}
		row.finalPnLByProduct.ForEach(func(product string, value float64) {
			idx, ok := productIndex[product]
			if !ok {
				idx = len(all)
				productIndex[product] = idx
				all = append(all, productTotal{
					product: product,
					values:  make([]float64, len(rows)),
				})
			}
			all[idx].values[runIdx] += value
			all[idx].total += value
		})
	}
	if len(all) == 0 {
		return
	}
	// For summary mode, cap to 8 and add a rollup.
	if mode == productsSummary && len(all) > 8 {
		sort.Slice(all, func(i, j int) bool {
			return absFloat(all[i].total) > absFloat(all[j].total)
		})
		top := all[:8]
		others := all[8:]
		rollup := productTotal{product: fmt.Sprintf("OTHER(+%d)", len(others)), values: make([]float64, len(rows))}
		for _, item := range others {
			for i, v := range item.values {
				rollup.values[i] += v
			}
			rollup.total += item.total
		}
		all = append(top, rollup)
	} else {
		sort.Slice(all, func(i, j int) bool {
			return absFloat(all[i].total) > absFloat(all[j].total)
		})
	}

	fmt.Println()
	productWidth := len("PRODUCT")
	for _, row := range all {
		if len(row.product) > productWidth {
			productWidth = len(row.product)
		}
	}
	colWidths := make([]int, len(rows))
	for i, row := range rows {
		label := row.dataset
		w := len(label)
		if w < 10 {
			w = 10
		}
		colWidths[i] = w
	}

	fmt.Printf("%-*s", productWidth, "PRODUCT")
	for i, row := range rows {
		fmt.Printf(" %*s", colWidths[i], row.dataset)
	}
	fmt.Println()
	for _, row := range all {
		fmt.Printf("%-*s", productWidth, row.product)
		for i, v := range row.values {
			fmt.Printf(" %*.2f", colWidths[i], v)
		}
		fmt.Println()
	}
}

func absFloat(v float64) float64 {
	if v < 0 {
		return -v
	}
	return v
}
