// Command storagebench compares storage engines on a deterministic UTXO
// workload.
//
// This is phase 0 of the migration described in docs/storage_design.md. The
// gate to proceed is that MDBX demonstrably wins on the access pattern btcd
// actually produces: point lookups plus batched inserts and deletes, with
// ordered iteration for snapshot export.
//
// The module is deliberately separate from the main btcd module so that
// evaluating a cgo-backed engine does not put cgo on btcd's build.
//
// Usage:
//
//	go run ./storagebench --utxos 200000 --blocks 500
package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"time"
)

// config holds the workload parameters.
type config struct {
	utxos      int
	blocks     int
	creates    int
	spends     int
	checkpoint int
	lookups    int
	history    int
	seed       uint64
	dir        string
	jsonOut    string
	engines    string
	keep       bool
	sorted     bool
}

// sortCommit puts a checkpoint's mutations into key order.
//
// The design requires this: a copy-on-write B-tree that receives 50,000
// randomly ordered keys in one transaction splits pages repeatedly and dirties
// far more of the tree than the same keys applied in order. Benchmarking
// unsorted application would measure a write pattern the design explicitly
// does not produce. An LSM engine is largely indifferent, so applying it to
// both keeps the comparison honest rather than tilting it.
func sortCommit(puts [][2][]byte, dels [][]byte) {
	sort.Slice(puts, func(i, j int) bool {
		return bytes.Compare(puts[i][0], puts[j][0]) < 0
	})
	sort.Slice(dels, func(i, j int) bool {
		return bytes.Compare(dels[i], dels[j]) < 0
	})
}

// phaseResult records the timing of one measured phase.
type phaseResult struct {
	Name       string  `json:"name"`
	Duration   float64 `json:"duration_sec"`
	Operations int     `json:"operations"`
	OpsPerSec  float64 `json:"ops_per_sec"`
}

// engineResult is one engine's complete result set.
type engineResult struct {
	Engine       string        `json:"engine"`
	Phases       []phaseResult `json:"phases"`
	UsedBytes    int64         `json:"used_bytes"`
	AllocBytes   int64         `json:"allocated_bytes"`
	LiveRecords  int           `json:"live_records"`
	BytesPerUtxo float64       `json:"bytes_per_utxo"`
	RecordBytes  int64         `json:"record_bytes"`
	Amplify      float64       `json:"amplification"`
}

// tieringResult reports what the workload implies about hot/cold tiering,
// independent of which engine stored it.
type tieringResult struct {
	Spends             int                `json:"spends"`
	AgeP50             uint64             `json:"age_p50"`
	AgeP90             uint64             `json:"age_p90"`
	AgeP99             uint64             `json:"age_p99"`
	AgeP999            uint64             `json:"age_p999"`
	AgeMax             uint64             `json:"age_max"`
	HitRateByThreshold map[string]float64 `json:"hit_rate_by_threshold"`
}

// report is the complete benchmark output.
type report struct {
	Config  map[string]int `json:"config"`
	Engines []engineResult `json:"engines"`
	Tiering tieringResult  `json:"tiering"`
}

func main() {
	cfg := parseFlags()

	if err := run(cfg); err != nil {
		fmt.Fprintf(os.Stderr, "storagebench: %v\n", err)
		os.Exit(1)
	}
}

func parseFlags() *config {
	cfg := &config{}
	flag.IntVar(&cfg.utxos, "utxos", 200000,
		"number of UTXOs to prefill before the measured phases")
	flag.IntVar(&cfg.blocks, "blocks", 500,
		"number of blocks to connect")
	flag.IntVar(&cfg.creates, "creates", 200,
		"outputs created per block")
	flag.IntVar(&cfg.spends, "spends", 180,
		"outputs spent per block")
	flag.IntVar(&cfg.checkpoint, "checkpoint", 256,
		"blocks folded into one durable commit")
	flag.IntVar(&cfg.lookups, "lookups", 50000,
		"cold point lookups to perform after reopening")
	flag.IntVar(&cfg.history, "history", 900000,
		"synthetic chain height the prefilled set is spread across, "+
			"which bounds the coin ages the run can express")
	flag.Uint64Var(&cfg.seed, "seed", 1,
		"deterministic workload seed")
	flag.StringVar(&cfg.dir, "dir", "",
		"working directory (default: a temporary directory)")
	flag.StringVar(&cfg.jsonOut, "json", "",
		"write the full report to this path as JSON")
	flag.StringVar(&cfg.engines, "engines", "leveldb,mdbx",
		"comma-separated engines to run")
	flag.BoolVar(&cfg.keep, "keep", false,
		"keep the store directories after the run")
	flag.IntVar(&mdbxPageSize, "mdbx-pagesize", mdbxDefaultPageSize,
		"MDBX page size in bytes")
	flag.BoolVar(&cfg.sorted, "sorted", true,
		"sort each commit into key order before applying it, as "+
			"docs/storage_design.md requires")
	flag.Parse()

	return cfg
}

func run(cfg *config) error {
	dir := cfg.dir
	if dir == "" {
		tmp, err := os.MkdirTemp("", "storagebench")
		if err != nil {
			return err
		}
		dir = tmp
		if !cfg.keep {
			defer os.RemoveAll(tmp)
		}
	}

	fmt.Printf("storagebench\n")
	fmt.Printf("  prefill %d UTXOs across %d blocks of history\n",
		cfg.utxos, cfg.history)
	fmt.Printf("  connect %d blocks (+%d/-%d per block), "+
		"checkpoint %d, %d cold lookups\n\n",
		cfg.blocks, cfg.creates, cfg.spends, cfg.checkpoint,
		cfg.lookups)

	rep := &report{
		Config: map[string]int{
			"utxos":      cfg.utxos,
			"blocks":     cfg.blocks,
			"creates":    cfg.creates,
			"spends":     cfg.spends,
			"checkpoint": cfg.checkpoint,
			"lookups":    cfg.lookups,
			"history":    cfg.history,
		},
	}

	var ages []uint64
	for _, name := range splitCSV(cfg.engines) {
		var eng engine
		switch name {
		case "leveldb":
			eng = newLevelDBEngine()
		case "mdbx":
			eng = newMDBXEngine()
		default:
			return fmt.Errorf("unknown engine %q", name)
		}

		result, spendAges, err := benchmarkEngine(eng, cfg,
			filepath.Join(dir, name))
		if err != nil {
			return fmt.Errorf("%s: %w", name, err)
		}
		rep.Engines = append(rep.Engines, *result)

		// The workload is identical across engines, so the age sample
		// only needs collecting once.
		if ages == nil {
			ages = spendAges
		}
	}

	rep.Tiering = analyzeTiering(ages)
	printReport(rep)

	if cfg.jsonOut != "" {
		encoded, err := json.MarshalIndent(rep, "", "  ")
		if err != nil {
			return err
		}
		if err := os.WriteFile(cfg.jsonOut, encoded, 0644); err != nil {
			return err
		}
		fmt.Printf("\nwrote %s\n", cfg.jsonOut)
	}

	return nil
}

// benchmarkEngine runs every phase against one engine and returns its results
// along with the realised spend ages.
func benchmarkEngine(eng engine, cfg *config, dir string) (*engineResult,
	[]uint64, error) {

	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, nil, err
	}
	if err := eng.open(dir); err != nil {
		return nil, nil, err
	}

	fmt.Printf("== %s ==\n", eng.name())

	result := &engineResult{Engine: eng.name()}
	random := newRNG(cfg.seed)
	live := newLiveSet(cfg.utxos + cfg.blocks*cfg.creates)

	var recordBytes int64
	var ordinal uint64

	// Phase 1: prefill. Heights are spread across the configured synthetic
	// history so the initial set has a realistic age profile instead of
	// every coin being the same age.
	start := time.Now()
	batchSize := cfg.creates * cfg.checkpoint
	batch := make([][2][]byte, 0, batchSize)
	for i := 0; i < cfg.utxos; i++ {
		height := uint32(int64(i) * int64(cfg.history) /
			int64(maxInt(1, cfg.utxos)))
		key, value := makeRecord(random, &ordinal, height)
		recordBytes += int64(len(key) + len(value))
		live.add(key, height)
		batch = append(batch, [2][]byte{key[:], value})

		if len(batch) >= batchSize {
			if cfg.sorted {
				sortCommit(batch, nil)
			}
			if err := eng.commit(batch, nil); err != nil {
				return nil, nil, err
			}
			batch = batch[:0]
		}
	}
	if len(batch) > 0 {
		if cfg.sorted {
			sortCommit(batch, nil)
		}
		if err := eng.commit(batch, nil); err != nil {
			return nil, nil, err
		}
	}
	result.Phases = append(result.Phases,
		makePhase("prefill", start, cfg.utxos))

	// Phase 2: block connection. Each block resolves its prevouts through
	// one read view, and each checkpoint is one durable commit carrying
	// every put and delete it accumulated.
	var ages []uint64
	start = time.Now()
	puts := make([][2][]byte, 0, batchSize)
	dels := make([][]byte, 0, cfg.spends*cfg.checkpoint)
	operations := 0

	for block := 0; block < cfg.blocks; block++ {
		height := uint32(cfg.history) + uint32(block) + 1

		for i := 0; i < cfg.creates; i++ {
			key, value := makeRecord(random, &ordinal, height)
			recordBytes += int64(len(key) + len(value))
			live.add(key, height)
			puts = append(puts, [2][]byte{key[:], value})
			operations++
		}

		// Resolve every prevout this block spends inside one view,
		// which is what block connection does.
		err := eng.view(func(g getter) error {
			for i := 0; i < cfg.spends; i++ {
				key, age, ok := live.selectSpend(random, height)
				if !ok {
					break
				}
				if _, err := g.get(key[:]); err != nil {
					return err
				}
				keyCopy := make([]byte, 36)
				copy(keyCopy, key[:])
				dels = append(dels, keyCopy)
				ages = append(ages, age)
				operations += 2
			}

			return nil
		})
		if err != nil {
			return nil, nil, err
		}

		if (block+1)%cfg.checkpoint == 0 {
			if cfg.sorted {
				sortCommit(puts, dels)
			}
			if err := eng.commit(puts, dels); err != nil {
				return nil, nil, err
			}
			puts = puts[:0]
			dels = dels[:0]
		}
	}
	if len(puts) > 0 || len(dels) > 0 {
		if cfg.sorted {
			sortCommit(puts, dels)
		}
		if err := eng.commit(puts, dels); err != nil {
			return nil, nil, err
		}
	}
	result.Phases = append(result.Phases,
		makePhase("connect", start, operations))

	// Phase 3: settle. Deferred background work is charged here rather
	// than being left uncounted.
	start = time.Now()
	if err := eng.settle(); err != nil {
		return nil, nil, err
	}
	result.Phases = append(result.Phases, makePhase("settle", start, 1))

	// Phase 4: cold point lookups. Closing and reopening drops the
	// engine's own caches, which is what a restarted node faces. Lookups
	// are grouped into views of one block's worth of spends.
	if err := eng.close(); err != nil {
		return nil, nil, err
	}
	if err := eng.open(dir); err != nil {
		return nil, nil, err
	}

	lookupRNG := newRNG(cfg.seed ^ 0x5bf03635)
	perView := maxInt(1, cfg.spends)
	start = time.Now()
	hits := 0
	for done := 0; done < cfg.lookups; done += perView {
		remaining := cfg.lookups - done
		if remaining > perView {
			remaining = perView
		}

		err := eng.view(func(g getter) error {
			for i := 0; i < remaining; i++ {
				key, ok := live.randomLive(lookupRNG)
				if !ok {
					return nil
				}
				value, err := g.get(key[:])
				if err != nil {
					return err
				}
				if value != nil {
					hits++
				}
			}

			return nil
		})
		if err != nil {
			return nil, nil, err
		}
	}
	result.Phases = append(result.Phases,
		makePhase("cold-lookup", start, cfg.lookups))
	if hits == 0 && cfg.lookups > 0 {
		return nil, nil, fmt.Errorf("every cold lookup missed, the "+
			"store is not returning data (%d attempted)",
			cfg.lookups)
	}

	// Phase 5: ordered iteration, the snapshot export path.
	start = time.Now()
	count, err := eng.iterate()
	if err != nil {
		return nil, nil, err
	}
	result.Phases = append(result.Phases,
		makePhase("iterate", start, count))
	result.LiveRecords = count

	used, err := eng.usedBytes()
	if err != nil {
		return nil, nil, err
	}
	result.UsedBytes = used

	if err := eng.close(); err != nil {
		return nil, nil, err
	}

	allocated, err := dirSize(dir)
	if err != nil {
		return nil, nil, err
	}
	result.AllocBytes = allocated
	result.RecordBytes = recordBytes
	if count > 0 {
		result.BytesPerUtxo = float64(used) / float64(count)
	}
	if recordBytes > 0 {
		result.Amplify = float64(used) / float64(recordBytes)
	}

	if !cfg.keep {
		os.RemoveAll(dir)
	}

	return result, ages, nil
}

// makeRecord generates one deterministic UTXO key/value pair.
func makeRecord(r *rng, ordinal *uint64, height uint32) ([36]byte, []byte) {
	*ordinal++
	txid := deriveTxid(*ordinal)
	vout := uint32(r.intn(4))
	key := outpointKey(txid, vout)

	class := pickScriptClass(r.intn(100))
	amount := uint64(r.intn(500000000)) + 1
	isCoinbase := r.intn(1000) == 0
	value := encodeUtxo(height, isCoinbase, amount, class, *ordinal)

	return key, value
}

func makePhase(name string, start time.Time, operations int) phaseResult {
	elapsed := time.Since(start)
	seconds := elapsed.Seconds()
	rate := 0.0
	if seconds > 0 {
		rate = float64(operations) / seconds
	}
	fmt.Printf("  %-12s %8.2fs  %12d ops  %12.0f ops/s\n",
		name, seconds, operations, rate)

	return phaseResult{
		Name:       name,
		Duration:   seconds,
		Operations: operations,
		OpsPerSec:  rate,
	}
}

// analyzeTiering reports the realised spend-age distribution and what hit rate
// a hot tier would have achieved at several candidate thresholds.
func analyzeTiering(ages []uint64) tieringResult {
	result := tieringResult{
		Spends:             len(ages),
		HitRateByThreshold: make(map[string]float64),
	}
	if len(ages) == 0 {
		return result
	}

	sorted := make([]uint64, len(ages))
	copy(sorted, ages)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })

	quantile := func(p float64) uint64 {
		idx := int(p * float64(len(sorted)-1))

		return sorted[idx]
	}
	result.AgeP50 = quantile(0.50)
	result.AgeP90 = quantile(0.90)
	result.AgeP99 = quantile(0.99)
	result.AgeP999 = quantile(0.999)
	result.AgeMax = sorted[len(sorted)-1]

	// The candidate windows are the ones the design document discusses.
	for _, threshold := range []uint64{144, 1008, 8064, 52560, 157680} {
		hits := 0
		for _, age := range sorted {
			if age < threshold {
				hits++
			}
		}
		key := fmt.Sprintf("%06d", threshold)
		result.HitRateByThreshold[key] =
			float64(hits) / float64(len(sorted)) * 100
	}

	return result
}

func printReport(rep *report) {
	fmt.Printf("\n== summary ==\n")
	fmt.Printf("  %-10s %10s %10s %10s %8s %10s\n",
		"engine", "used", "records", "B/utxo", "amp", "allocated")
	for _, eng := range rep.Engines {
		fmt.Printf("  %-10s %9.1fM %10d %10.1f %7.2fx %9.1fM\n",
			eng.Engine,
			float64(eng.UsedBytes)/(1024*1024),
			eng.LiveRecords,
			eng.BytesPerUtxo,
			eng.Amplify,
			float64(eng.AllocBytes)/(1024*1024))
	}

	if len(rep.Engines) == 2 {
		fmt.Printf("\n== %s relative to %s ==\n",
			rep.Engines[1].Engine, rep.Engines[0].Engine)
		for i, phase := range rep.Engines[1].Phases {
			if i >= len(rep.Engines[0].Phases) {
				break
			}
			base := rep.Engines[0].Phases[i]
			if phase.Duration <= 0 || base.Duration <= 0 {
				continue
			}
			ratio := base.Duration / phase.Duration
			verdict := "faster"
			if ratio < 1 {
				verdict = "slower"
				ratio = 1 / ratio
			}
			fmt.Printf("  %-12s %5.2fx %s\n",
				phase.Name, ratio, verdict)
		}
	}

	fmt.Printf("\n== tiering (%d spends observed) ==\n", rep.Tiering.Spends)
	fmt.Printf("  realised spend age: P50 %d  P90 %d  P99 %d  "+
		"P99.9 %d  max %d\n",
		rep.Tiering.AgeP50, rep.Tiering.AgeP90, rep.Tiering.AgeP99,
		rep.Tiering.AgeP999, rep.Tiering.AgeMax)

	thresholds := make([]string, 0, len(rep.Tiering.HitRateByThreshold))
	for key := range rep.Tiering.HitRateByThreshold {
		thresholds = append(thresholds, key)
	}
	sort.Strings(thresholds)
	for _, key := range thresholds {
		fmt.Printf("  hot window %8s blocks -> %6.2f%% of spends\n",
			key, rep.Tiering.HitRateByThreshold[key])
	}
}

func splitCSV(value string) []string {
	var parts []string
	current := ""
	for _, char := range value {
		if char == ',' {
			if current != "" {
				parts = append(parts, current)
			}
			current = ""

			continue
		}
		current += string(char)
	}
	if current != "" {
		parts = append(parts, current)
	}

	return parts
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}

	return b
}
