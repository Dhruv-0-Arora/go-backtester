package cli

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/darora1/imc-prosperity-4/backtester/internal/dataset"
	"github.com/darora1/imc-prosperity-4/backtester/internal/model"
	"github.com/darora1/imc-prosperity-4/backtester/internal/orderedmap"
)

// datasetSelection is the resolved form of the --dataset argument. Multiple
// roots appear only for composite aliases (e.g. future multi-file bundles).
type datasetSelection struct {
	roots                          []string
	label                          string
	autoSelected                   bool
	excludeSubmissionWhenDayFilter bool
}

// plannedRun is a single (dataset_file, day) pair turned into a runnable
// engine request with stable artifact naming.
type plannedRun struct {
	datasetFile     string
	datasetOverride *model.NormalizedDataset
	day             *int64
	runID           string
	artifactPrefix  string
	summaryLabel    string
	metadata        model.MetadataOverrides
}

// resolveDataset converts the user's --dataset argument to a datasetSelection.
// Recognised aliases mirror the reference CLI: tutorial, round1..round8, etc.
// An unknown value is treated as a filesystem path.
func resolveDataset(requested string) (datasetSelection, error) {
	datasetsRoot := filepath.Join(mustGetwd(), "datasets")
	if _, err := os.Stat(datasetsRoot); err != nil {
		datasetsRoot = filepath.Join(mustGetwd(), "prosperity_rust_backtester", "datasets")
	}
	name := strings.ToLower(strings.TrimSpace(requested))
	if name == "" {
		name = "latest"
	}

	if strings.HasSuffix(name, "-submission") {
		round := strings.TrimSuffix(name, "-submission")
		root := filepath.Join(datasetsRoot, round)
		if info, err := os.Stat(root); err != nil || !info.IsDir() {
			return datasetSelection{}, fmt.Errorf("dataset round not found: %s", root)
		}
		sub, err := roundSubmissionEntry(root)
		if err != nil {
			return datasetSelection{}, err
		}
		return datasetSelection{
			roots: []string{sub},
			label: fmt.Sprintf("%s-sub", round),
		}, nil
	}

	switch name {
	case "latest":
		root, err := latestRoundRoot(datasetsRoot)
		if err != nil {
			return datasetSelection{}, err
		}
		return datasetSelection{
			roots:                          []string{root},
			label:                          filepath.Base(root),
			autoSelected:                   true,
			excludeSubmissionWhenDayFilter: true,
		}, nil
	case "tutorial", "tut", "tutorial-round", "tut-round":
		return datasetSelection{
			roots:                          []string{filepath.Join(datasetsRoot, "tutorial")},
			label:                          "tutorial",
			excludeSubmissionWhenDayFilter: true,
		}, nil
	case "round1", "r1", "round2", "r2", "round3", "r3", "round4", "r4",
		"round5", "r5", "round6", "r6", "round7", "r7", "round8", "r8":
		round := normalizeRoundAlias(name)
		root := filepath.Join(datasetsRoot, round)
		if info, err := os.Stat(root); err != nil || !info.IsDir() {
			return datasetSelection{}, fmt.Errorf("dataset round not found: %s", root)
		}
		_, hasSubmission := roundSubmissionEntryCheck(root)
		return datasetSelection{
			roots:                          []string{root},
			label:                          round,
			excludeSubmissionWhenDayFilter: hasSubmission,
		}, nil
	case "submission", "tutorial-submission", "tut-sub", "sub":
		root, err := latestRoundRoot(datasetsRoot)
		if err != nil {
			return datasetSelection{}, err
		}
		sub, err := roundSubmissionEntry(root)
		if err != nil {
			return datasetSelection{}, err
		}
		return datasetSelection{
			roots: []string{sub},
			label: "tut-sub",
		}, nil
	case "tutorial-1", "tut-1", "tut-d-1":
		return roundDayAlias(filepath.Join(datasetsRoot, "tutorial"), -1, "tut-d-1")
	case "tutorial-2", "tut-2", "tut-d-2":
		return roundDayAlias(filepath.Join(datasetsRoot, "tutorial"), -2, "tut-d-2")
	}

	abs, err := filepath.Abs(requested)
	if err != nil {
		return datasetSelection{}, fmt.Errorf("failed to resolve dataset %s: %w", requested, err)
	}
	if _, err := os.Stat(abs); err != nil {
		return datasetSelection{}, fmt.Errorf("dataset path does not exist: %s", abs)
	}
	return datasetSelection{
		roots: []string{abs},
		label: shortDatasetLabel(abs),
	}, nil
}

func normalizeRoundAlias(name string) string {
	if strings.HasPrefix(name, "r") && len(name) == 2 {
		return "round" + name[1:]
	}
	return name
}

func roundDayAlias(root string, day int64, label string) (datasetSelection, error) {
	files, err := collectDatasetFiles(root)
	if err != nil {
		return datasetSelection{}, err
	}
	wanted := fmt.Sprintf("day_%d", day)
	for _, f := range files {
		if dayKeyFromName(strings.ToLower(filepath.Base(f))) == wanted {
			return datasetSelection{roots: []string{f}, label: label}, nil
		}
	}
	return datasetSelection{}, fmt.Errorf("day %d dataset not found in %s", day, root)
}

func latestRoundRoot(datasetsRoot string) (string, error) {
	candidates := []string{filepath.Join(datasetsRoot, "tutorial")}
	for i := 1; i <= 8; i++ {
		candidates = append(candidates, filepath.Join(datasetsRoot, fmt.Sprintf("round%d", i)))
	}
	var last string
	for _, c := range candidates {
		info, err := os.Stat(c)
		if err != nil || !info.IsDir() {
			continue
		}
		entries, err := os.ReadDir(c)
		if err != nil {
			continue
		}
		hasDataset := false
		for _, e := range entries {
			if e.IsDir() {
				continue
			}
			if _, ok := datasetCandidateKey(filepath.Join(c, e.Name())); ok {
				hasDataset = true
				break
			}
		}
		if hasDataset {
			last = c
		}
	}
	if last == "" {
		return "", fmt.Errorf("no populated round directories found under %s", datasetsRoot)
	}
	return last, nil
}

func roundSubmissionEntry(root string) (string, error) {
	if sub, ok := roundSubmissionEntryCheck(root); ok {
		return sub, nil
	}
	return "", fmt.Errorf("submission dataset not found in %s", root)
}

func roundSubmissionEntryCheck(root string) (string, bool) {
	files, err := collectDatasetFiles(root)
	if err != nil {
		return "", false
	}
	var candidates []string
	for _, f := range files {
		if isSubmissionCandidatePath(f) {
			candidates = append(candidates, f)
		}
	}
	sort.Slice(candidates, func(i, j int) bool {
		return datasetCandidateRank(candidates[i]) > datasetCandidateRank(candidates[j])
	})
	if len(candidates) == 0 {
		return "", false
	}
	return candidates[0], true
}

func shortDatasetLabel(path string) string {
	dayKey := dayKeyFromName(strings.ToLower(filepath.Base(path)))
	if dayKey != "" {
		return dayLabelFromKey(dayKey)
	}
	if isSubmissionLikePath(path) {
		return "SUB"
	}
	stem := strings.TrimSuffix(filepath.Base(path), filepath.Ext(path))
	if len(stem) > 20 {
		stem = stem[:20]
	}
	return strings.ReplaceAll(stem, "_", "-")
}

func dayLabelFromKey(key string) string {
	raw := strings.TrimPrefix(key, "day_")
	if raw == "" {
		return key
	}
	if raw == "0" {
		return "D=0"
	}
	if raw[0] == '-' {
		return "D" + raw
	}
	return "D+" + raw
}

// planTarget is one (dataset, day) pair before plan construction. A named
// type is used because both the carry and the standard planners consume it.
type planTarget struct {
	file string
	day  *int64
}

func buildRunPlan(sel datasetSelection, day *int64, runIDSeed string, carry bool) ([]plannedRun, error) {
	var targets []planTarget
	seen := map[string]bool{}
	for _, root := range sel.roots {
		files, err := collectDatasetFiles(root)
		if err != nil {
			return nil, err
		}
		inputIsFile := isFile(root)
		for _, file := range files {
			if seen[file] {
				continue
			}
			seen[file] = true
			if sel.excludeSubmissionWhenDayFilter && day != nil && isSubmissionLikePath(file) {
				continue
			}
			ds, err := dataset.Load(file)
			if err != nil {
				return nil, err
			}
			days := collectRequestedDays(ds, day)
			if len(days) == 0 {
				if inputIsFile {
					return nil, fmt.Errorf("day %d not found in dataset %s", int64PtrValue(day), file)
				}
				continue
			}
			for _, d := range days {
				targets = append(targets, planTarget{file: file, day: d})
			}
		}
	}
	if len(targets) == 0 {
		label := "dataset selection"
		if len(sel.roots) > 0 {
			label = sel.roots[0]
		}
		return nil, fmt.Errorf("no runnable datasets found at %s", label)
	}

	sort.SliceStable(targets, func(i, j int) bool {
		rankA := datasetOrderRank(targets[i].file, targets[i].day)
		rankB := datasetOrderRank(targets[j].file, targets[j].day)
		if rankA != rankB {
			return rankA < rankB
		}
		return targets[i].file < targets[j].file
	})

	if carry {
		return buildCarryPlans(targets, runIDSeed)
	}

	multiple := len(targets) > 1
	plans := make([]plannedRun, 0, len(targets))
	for _, t := range targets {
		plans = append(plans, buildSinglePlan(t.file, t.day, runIDSeed, multiple))
	}
	return plans, nil
}

func datasetOrderRank(path string, day *int64) int64 {
	if isSubmissionLikePath(path) {
		return 10_000
	}
	if day != nil {
		return *day
	}
	return 0
}

func collectRequestedDays(ds *model.NormalizedDataset, requested *int64) []*int64 {
	seen := map[int64]struct{}{}
	var days []int64
	for _, tick := range ds.Ticks {
		if tick.Day == nil {
			continue
		}
		if _, ok := seen[*tick.Day]; ok {
			continue
		}
		seen[*tick.Day] = struct{}{}
		days = append(days, *tick.Day)
	}
	sort.Slice(days, func(i, j int) bool { return days[i] < days[j] })
	if requested != nil {
		for _, d := range days {
			if d == *requested {
				v := d
				return []*int64{&v}
			}
		}
		return nil
	}
	if len(days) == 0 {
		return []*int64{nil}
	}
	out := make([]*int64, 0, len(days))
	for _, d := range days {
		v := d
		out = append(out, &v)
	}
	return out
}

func buildSinglePlan(file string, day *int64, runIDSeed string, multiple bool) plannedRun {
	prefix := runSuffix(file, day)
	runID := runIDSeed
	if multiple {
		runID = fmt.Sprintf("%s-%s", runIDSeed, prefix)
	}
	return plannedRun{
		datasetFile:    file,
		day:            day,
		runID:          runID,
		artifactPrefix: prefix,
		summaryLabel:   shortDatasetLabel(file),
	}
}

// buildCarryPlans merges consecutive day datasets in the same round into a
// single dataset-override run so positions and trader state carry over.
// Submission datasets are always kept as standalone plans because they
// already span multiple days.
func buildCarryPlans(targets []planTarget, runIDSeed string) ([]plannedRun, error) {
	var plans []plannedRun
	flush := func(buffer []planTarget) error {
		if len(buffer) == 0 {
			return nil
		}
		if len(buffer) == 1 {
			plans = append(plans, buildSinglePlan(buffer[0].file, buffer[0].day, "", false))
			return nil
		}
		override, err := buildCarryDataset(buffer)
		if err != nil {
			return err
		}
		first := buffer[0].file
		prefix := carryArtifactPrefix(first)
		summary := carrySummaryLabel(first)
		recorded := carryRecordedPath(buffer)
		plan := plannedRun{
			datasetFile:     first,
			datasetOverride: override,
			day:             nil,
			artifactPrefix:  prefix,
			summaryLabel:    summary,
			metadata: model.MetadataOverrides{
				RecordedDatasetPath: &recorded,
			},
		}
		plans = append(plans, plan)
		return nil
	}

	var buffer []planTarget
	var key string
	for _, t := range targets {
		if isSubmissionLikePath(t.file) {
			if err := flush(buffer); err != nil {
				return nil, err
			}
			buffer = nil
			key = ""
			plans = append(plans, buildSinglePlan(t.file, t.day, "", false))
			continue
		}
		next := carryGroupKey(t.file)
		if len(buffer) > 0 && key != next {
			if err := flush(buffer); err != nil {
				return nil, err
			}
			buffer = nil
		}
		key = next
		buffer = append(buffer, t)
	}
	if err := flush(buffer); err != nil {
		return nil, err
	}
	multiple := len(plans) > 1
	for i := range plans {
		if multiple {
			plans[i].runID = fmt.Sprintf("%s-%s", runIDSeed, plans[i].artifactPrefix)
		} else {
			plans[i].runID = runIDSeed
		}
	}
	return plans, nil
}

func carryGroupKey(path string) string {
	if dayKeyFromName(strings.ToLower(filepath.Base(path))) != "" {
		return filepath.Dir(path)
	}
	return path
}

func carryArtifactPrefix(path string) string {
	base := containerLabel(path)
	if base == "" {
		base = strings.TrimSuffix(filepath.Base(path), filepath.Ext(path))
	}
	return sanitizeIdentifier(base + "-carry")
}

func carrySummaryLabel(path string) string {
	base := containerLabel(path)
	if base == "" {
		base = shortDatasetLabel(path)
	}
	return base + "-carry"
}

func carryRecordedPath(targets []planTarget) string {
	first := targets[0].file
	base := first
	if dayKeyFromName(strings.ToLower(filepath.Base(first))) != "" {
		base = filepath.Dir(first)
	}
	labels := make([]string, 0, len(targets))
	for _, t := range targets {
		if t.day == nil {
			labels = append(labels, "all")
		} else {
			labels = append(labels, fmt.Sprintf("%d", *t.day))
		}
	}
	return fmt.Sprintf("%s [carry days: %s]", base, strings.Join(labels, ","))
}

func containerLabel(path string) string {
	parent := filepath.Base(filepath.Dir(path))
	if parent == "" || parent == "." {
		return ""
	}
	return parent
}

func sanitizeIdentifier(value string) string {
	var out strings.Builder
	lastDash := false
	for _, r := range value {
		switch {
		case (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9'):
			out.WriteRune(r)
			lastDash = false
		case r >= 'A' && r <= 'Z':
			out.WriteRune(r + ('a' - 'A'))
			lastDash = false
		default:
			if !lastDash {
				out.WriteByte('-')
				lastDash = true
			}
		}
	}
	return strings.Trim(out.String(), "-")
}

func runSuffix(path string, day *int64) string {
	container := containerLabel(path)
	dayLabel := "all"
	if day != nil {
		dayLabel = fmt.Sprintf("day-%d", *day)
	}
	if container != "" {
		base := container
		if isSubmissionLikePath(path) {
			base = container + "-submission"
		}
		return sanitizeIdentifier(fmt.Sprintf("%s-%s", base, dayLabel))
	}
	stem := strings.TrimSuffix(filepath.Base(path), filepath.Ext(path))
	return sanitizeIdentifier(fmt.Sprintf("%s-%s", stem, dayLabel))
}

func int64PtrValue(v *int64) int64 {
	if v == nil {
		return 0
	}
	return *v
}

func isFile(path string) bool {
	info, err := os.Stat(path)
	return err == nil && !info.IsDir()
}

func mustGetwd() string {
	cwd, err := os.Getwd()
	if err != nil {
		return "."
	}
	return cwd
}

func buildCarryDataset(targets []planTarget) (*model.NormalizedDataset, error) {
	cache := map[string]*model.NormalizedDataset{}
	var schema, competition string
	products := orderedmap.New[struct{}]()
	var ticks []*model.TickSnapshot

	for _, t := range targets {
		ds, ok := cache[t.file]
		if !ok {
			loaded, err := dataset.Load(t.file)
			if err != nil {
				return nil, err
			}
			cache[t.file] = loaded
			ds = loaded
		}
		if schema == "" {
			schema = ds.SchemaVersion
			competition = ds.CompetitionVersion
		} else if schema != ds.SchemaVersion || competition != ds.CompetitionVersion {
			return nil, fmt.Errorf("cannot carry across incompatible datasets: %s", t.file)
		}
		for _, p := range ds.Products {
			products.Set(p, struct{}{})
		}
		for _, tick := range ds.Ticks {
			if t.day != nil && (tick.Day == nil || *tick.Day != *t.day) {
				continue
			}
			ticks = append(ticks, tick)
		}
	}
	sort.SliceStable(ticks, func(i, j int) bool {
		a, b := ticks[i].Day, ticks[j].Day
		if a == nil && b != nil {
			return true
		}
		if a != nil && b == nil {
			return false
		}
		if a != nil && b != nil && *a != *b {
			return *a < *b
		}
		return ticks[i].Timestamp < ticks[j].Timestamp
	})
	normalizeCarryTimestamps(ticks)

	metadata := orderedmap.New[json.RawMessage]()
	metadata.Set("carry", jsonRaw(true))
	metadata.Set("carry_timestamp_mode", jsonRaw("continuous"))
	carried := make([]string, 0, len(targets))
	for _, t := range targets {
		if t.day != nil {
			carried = append(carried, fmt.Sprintf("%s#day=%d", t.file, *t.day))
		} else {
			carried = append(carried, t.file)
		}
	}
	metadata.Set("carried_inputs", jsonRaw(carried))

	if schema == "" {
		return nil, fmt.Errorf("carry dataset missing schema version")
	}

	productList := products.Keys()
	sort.Strings(productList)

	return &model.NormalizedDataset{
		SchemaVersion:      schema,
		CompetitionVersion: competition,
		DatasetID:          sanitizeIdentifier((containerLabel(targets[0].file) + "-carry")),
		Source:             fmt.Sprintf("carry:%s", carryRecordedPath(targets)),
		Products:           productList,
		Metadata:           metadata,
		Ticks:              ticks,
	}, nil
}

// normalizeCarryTimestamps rebases each day so its first tick starts
// immediately after the last tick of the previous day. The same offset is
// applied to market trade timestamps within the day.
func normalizeCarryTimestamps(ticks []*model.TickSnapshot) {
	var nextBase int64
	i := 0
	for i < len(ticks) {
		day := ticks[i].Day
		start := i
		end := i + 1
		for end < len(ticks) && sameDay(ticks[end].Day, day) {
			end++
		}
		dayStart := ticks[start].Timestamp
		dayEnd := ticks[end-1].Timestamp
		step := int64(1)
		for j := start; j+1 < end; j++ {
			delta := ticks[j+1].Timestamp - ticks[j].Timestamp
			if delta > 0 {
				step = delta
				break
			}
		}
		offset := nextBase - dayStart
		for j := start; j < end; j++ {
			ticks[j].Timestamp += offset
			if ticks[j].MarketTrades != nil {
				ticks[j].MarketTrades.ForEach(func(_ string, trades []model.MarketTrade) {
					for k := range trades {
						trades[k].Timestamp += offset
					}
				})
			}
		}
		nextBase += (dayEnd - dayStart) + step
		i = end
	}
}

func sameDay(a, b *int64) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}
	return *a == *b
}

func jsonRaw(v any) json.RawMessage {
	b, err := json.Marshal(v)
	if err != nil {
		return json.RawMessage(`null`)
	}
	return b
}

func writeJSON(path string, v any) error {
	data, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	data = append(data, '\n')
	return os.WriteFile(path, data, 0o644)
}
