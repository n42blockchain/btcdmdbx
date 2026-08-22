// Command replayblocks rebuilds a chainstate by replaying the blocks held in
// an existing flat block file set.
//
// It exists to measure the storage layer against real mainnet data without
// waiting on the network. The block files produced by this package are
// self-describing, so an existing node's block files can be replayed into a
// fresh database and the resulting throughput and metadata size recorded.
//
// The source files are only ever read.
//
// Blocks are replayed with the same fast-add behaviour a node uses below its
// last checkpoint, which skips checkConnectBlock: signature verification, the
// spend checks and the BIP30 scan. Everything the storage layer does still
// happens, since the block is written and the UTXO set updated either way.
// That is deliberate. Validating every block in full turns the run into a
// measurement of ECDSA throughput -- on this data it fell from 2,190 to under
// 4 blocks per second by height 387,000 while the store was writing well under
// a megabyte a second -- and the storage layer never becomes the bottleneck.
// Pass --fastadd=false to measure validation instead.
package main

import (
	"encoding/binary"
	"errors"
	"flag"
	"fmt"
	"hash/crc32"
	"io"
	"net/http"
	_ "net/http/pprof"
	"os"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"runtime/pprof"
	"sort"
	"sync"
	"time"

	"github.com/btcsuite/btcd/blockchain"
	"github.com/btcsuite/btcd/btcutil/v2"
	"github.com/btcsuite/btcd/chaincfg/v2"
	"github.com/btcsuite/btcd/database"
	_ "github.com/btcsuite/btcd/database/ffldb"
	"github.com/btcsuite/btcd/txscript/v2"
	"github.com/btcsuite/btcd/wire/v2"
)

// castagnoli houses the Castagnoli polynomial used for the CRC-32 checksums
// the block files carry.
var castagnoli = crc32.MakeTable(crc32.Castagnoli)

// blockFileExtension is the suffix every block file carries.
const blockFileExtension = ".fdb"

type config struct {
	src         string
	dst         string
	maxHeight   int
	reportSecs  int
	fastAdd     bool
	cpuProfile  string
	utxoCache   int
	gogc        int
	memLimitGB  int
	sigCache    int
	pprofAddr   string
	utxoWorkers int
	pipeline    bool
	noCheckpts  bool
	fastBelow   int
	workers     int
	depth       int
	logPath     string
	loadOnly    bool
	censusFrom  int
	hashAt      int
	censusTo    int
	censusWin   int
}

// logOut is where progress is written, in addition to standard output.
var logOut *os.File

// logf writes a progress line and flushes it.
//
// Standard output is buffered when the process is launched with its output
// redirected, which leaves a long run looking hung. Writing to an explicit
// file and syncing after every line keeps progress observable.
func logf(format string, args ...interface{}) {
	fmt.Printf(format, args...)
	if logOut != nil {
		fmt.Fprintf(logOut, format, args...)
		logOut.Sync()
	}
}

func main() {
	cfg := parseFlags()

	if err := run(cfg); err != nil {
		fmt.Fprintf(os.Stderr, "replayblocks: %v\n", err)
		os.Exit(1)
	}
}

func parseFlags() *config {
	cfg := &config{}
	flag.StringVar(&cfg.src, "src", "",
		"directory holding the source block files (read only)")
	flag.StringVar(&cfg.dst, "dst", "",
		"directory to build the new database in")
	flag.IntVar(&cfg.maxHeight, "maxheight", 0,
		"stop after this height (0 replays everything available)")
	flag.IntVar(&cfg.reportSecs, "report", 30,
		"seconds between progress reports")
	flag.BoolVar(&cfg.fastAdd, "fastadd", true,
		"use fast-add below the last checkpoint.  Blocks above it are "+
			"always validated in full regardless of this flag; "+
			"--fastadd=false extends full validation to every "+
			"height")
	flag.IntVar(&cfg.gogc, "gogc", 400,
		"GC percent.  The replay holds a very large, mostly static "+
			"heap, where the default of 100 spends a significant "+
			"share of CPU re-marking objects that never die")
	flag.IntVar(&cfg.memLimitGB, "memlimit", 112,
		"soft memory limit in GiB handed to the runtime so a high "+
			"gogc cannot run the process into the ground")
	flag.IntVar(&cfg.fastBelow, "fastbelow", 0,
		"fast-add below this height and validate in full above it, "+
			"overriding the checkpoint boundary; for measuring a "+
			"chosen window")
	flag.BoolVar(&cfg.noCheckpts, "nocheckpoints", false,
		"build the chain without checkpoints, so every block is "+
			"validated in full including scripts; for measuring the "+
			"validation path on heights a checkpoint would otherwise "+
			"cover")
	flag.BoolVar(&cfg.pipeline, "pipeline", true,
		"above the checkpoint, validate each block while the previous "+
			"one is being applied and committed")
	flag.IntVar(&cfg.utxoWorkers, "utxoworkers", 12,
		"goroutines applying a block's utxo changes to the cache; the "+
			"work is DRAM-latency bound, so this scales with memory "+
			"parallelism rather than core count")
	flag.StringVar(&cfg.pprofAddr, "pprofaddr", "127.0.0.1:6161",
		"serve net/http/pprof here so a long run can be profiled "+
			"live at any phase; empty disables it")
	flag.IntVar(&cfg.sigCache, "sigcache", 1000000,
		"signature cache entries for full validation above the "+
			"checkpoint")
	flag.StringVar(&cfg.cpuProfile, "cpuprofile", "",
		"write a CPU profile to this path")
	flag.IntVar(&cfg.hashAt, "hash-at", 0,
		"print the hash of the block at this height and exit")
	flag.IntVar(&cfg.censusFrom, "census-from", 0,
		"with --census-to, only count blocks: print transactions, inputs "+
			"and bytes per window of --census-window blocks, without "+
			"touching the database.  Normalises rates measured on "+
			"different height ranges, since blocks differ widely in "+
			"how much there is to verify")
	flag.IntVar(&cfg.censusTo, "census-to", 0, "last height to count")
	flag.IntVar(&cfg.censusWin, "census-window", 3000, "census window size")
	flag.BoolVar(&cfg.loadOnly, "loadonly", false,
		"open the database, load the chain state, report how long it "+
			"took, and exit")
	flag.StringVar(&cfg.logPath, "log", "",
		"also write progress to this file, flushed after every line")
	flag.IntVar(&cfg.workers, "workers", runtime.NumCPU()/4,
		"goroutines deserializing blocks ahead of the connect loop")
	flag.IntVar(&cfg.depth, "depth", 1024,
		"how many blocks the pipeline may run ahead")
	flag.IntVar(&cfg.utxoCache, "utxocache", 8192,
		"UTXO cache size in MiB. Leaving this at zero makes every "+
			"read and write go to the database, which dominates the "+
			"profile")
	flag.Parse()

	return cfg
}

// blockFileReader walks a directory of flat block files as one stream of
// serialized blocks.
type blockFileReader struct {
	paths   []string
	fileIdx int
	file    *os.File
	net     wire.BitcoinNet
	err     error
}

// errPipelineRead signals that the reader stopped on an error, whose detail is
// retrieved with lastErr.
var errPipelineRead = errors.New("block file read failed")

// lastErr returns the error the reader stopped on.
func (r *blockFileReader) lastErr() error {
	if r.err == nil {
		return errPipelineRead
	}

	return r.err
}

// newBlockFileReader collects the block files in the passed directory in the
// order they were written.
func newBlockFileReader(dir string, net wire.BitcoinNet) (*blockFileReader,
	error) {

	pattern := filepath.Join(dir, "*"+blockFileExtension)
	paths, err := filepath.Glob(pattern)
	if err != nil {
		return nil, err
	}
	if len(paths) == 0 {
		return nil, fmt.Errorf("no %s files in %q", blockFileExtension,
			dir)
	}
	sort.Strings(paths)

	return &blockFileReader{paths: paths, net: net}, nil
}

// next returns the next serialized block, or nil once every file is consumed.
//
// Each record is the network magic, the block length, the block itself, and a
// Castagnoli CRC-32 over all of the preceding bytes. The checksum is verified
// here so a truncated or corrupt file is reported as such rather than
// surfacing later as a nonsensical block.
func (r *blockFileReader) next() ([]byte, error) {
	for {
		if r.file == nil {
			if r.fileIdx >= len(r.paths) {
				return nil, nil
			}
			file, err := os.Open(r.paths[r.fileIdx])
			if err != nil {
				return nil, err
			}
			r.file = file
			r.fileIdx++
		}

		block, err := r.readRecord()
		if err != nil && err != io.EOF && err != io.ErrUnexpectedEOF {
			r.err = err
		}
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			// A block file is padded to its allocated size, so
			// hitting the end of one simply means moving to the
			// next.
			r.file.Close()
			r.file = nil

			continue
		}
		if err != nil {
			return nil, err
		}
		if block == nil {
			r.file.Close()
			r.file = nil

			continue
		}

		return block, nil
	}
}

// skip advances past n records without decoding them.
//
// Resuming an interrupted replay has to reach the point the previous run
// stopped at. Reading every record to get there means re-reading the entire
// block corpus -- hundreds of gigabytes -- to skip work already done. Only the
// eight-byte header of each record is needed to find the next one, so the body
// and checksum are seeked over instead.
//
// The count is approximate by design: it comes from the resumed chain height,
// while the files also hold orphans and side-chain blocks. Skipping slightly
// too few is harmless because the duplicates are recognised and skipped by the
// caller; skipping too many is prevented by never skipping past the count
// given.
func (r *blockFileReader) skip(n int) (int, error) {
	skipped := 0
	for skipped < n {
		if r.file == nil {
			if r.fileIdx >= len(r.paths) {
				return skipped, nil
			}
			file, err := os.Open(r.paths[r.fileIdx])
			if err != nil {
				return skipped, err
			}
			r.file = file
			r.fileIdx++
		}

		var header [8]byte
		_, err := io.ReadFull(r.file, header[:])
		if err != nil {
			r.file.Close()
			r.file = nil

			continue
		}

		net := binary.LittleEndian.Uint32(header[0:4])
		if net == 0 {
			r.file.Close()
			r.file = nil

			continue
		}
		if net != uint32(r.net) {
			return skipped, fmt.Errorf("network mismatch: got "+
				"%08x, want %08x", net, uint32(r.net))
		}

		blockLen := binary.LittleEndian.Uint32(header[4:8])
		if blockLen > wire.MaxBlockPayload {
			return skipped, fmt.Errorf("block of %d bytes exceeds "+
				"the maximum of %d", blockLen,
				wire.MaxBlockPayload)
		}

		// Step over the block body and its checksum.
		if _, err := r.file.Seek(int64(blockLen)+4, io.SeekCurrent); err != nil {
			return skipped, err
		}
		skipped++
	}

	return skipped, nil
}

// readRecord reads one block record from the current file.
func (r *blockFileReader) readRecord() ([]byte, error) {
	var header [8]byte
	if _, err := io.ReadFull(r.file, header[:]); err != nil {
		return nil, err
	}

	net := binary.LittleEndian.Uint32(header[0:4])
	if net == 0 {
		// Trailing zero padding marks the end of the written data.
		return nil, nil
	}
	if net != uint32(r.net) {
		return nil, fmt.Errorf("network mismatch: got %08x, want %08x",
			net, uint32(r.net))
	}

	blockLen := binary.LittleEndian.Uint32(header[4:8])
	if blockLen > wire.MaxBlockPayload {
		return nil, fmt.Errorf("block of %d bytes exceeds the maximum "+
			"of %d", blockLen, wire.MaxBlockPayload)
	}

	block := make([]byte, blockLen)
	if _, err := io.ReadFull(r.file, block); err != nil {
		return nil, err
	}

	var stored [4]byte
	if _, err := io.ReadFull(r.file, stored[:]); err != nil {
		return nil, err
	}

	hasher := crc32.New(castagnoli)
	hasher.Write(header[:])
	hasher.Write(block)
	if got := hasher.Sum32(); got != binary.BigEndian.Uint32(stored[:]) {
		return nil, fmt.Errorf("checksum mismatch in %q: got %08x, "+
			"want %08x", r.paths[r.fileIdx-1], got,
			binary.BigEndian.Uint32(stored[:]))
	}

	return block, nil
}

// parsedBlock carries one block, or the error that stopped the pipeline.
type parsedBlock struct {
	block *btcutil.Block
	err   error
}

// startPipeline reads, checksums and deserializes blocks on separate
// goroutines so that work overlaps with the serial connect loop.
//
// Connecting blocks is inherently serial: a transaction can spend an output
// created earlier in the same block, so the UTXO state has to be applied in
// order. Everything before that is not. Reading a record, verifying its
// checksum and deserializing it depend on nothing but the bytes themselves, so
// they are moved off the critical path entirely.
//
// Parsing is fanned out across workers while reading stays on one goroutine,
// since the records are variable length and have to be framed in order. Each
// record is tagged with its position and the results are reordered before
// being handed to the caller, so the connect loop still sees strict chain
// order.
func startPipeline(reader *blockFileReader, workers, depth int,
	sanity func(*btcutil.Block) error) (<-chan parsedBlock, func()) {

	type rawBlock struct {
		seq  uint64
		data []byte
	}

	quit := make(chan struct{})
	raw := make(chan rawBlock, depth)
	parsed := make(chan struct {
		seq uint64
		out parsedBlock
	}, depth)
	out := make(chan parsedBlock, depth)

	// Reader: frames records in file order and hands them out numbered.
	go func() {
		defer close(raw)

		var seq uint64
		for {
			data, err := reader.next()
			if err != nil {
				select {
				case raw <- rawBlock{seq: seq, data: nil}:
				case <-quit:
				}

				return
			}
			if data == nil {
				return
			}

			select {
			case raw <- rawBlock{seq: seq, data: data}:
			case <-quit:
				return
			}
			seq++
		}
	}()

	// Parsers: deserialize in parallel.
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			for item := range raw {
				var result parsedBlock
				if item.data == nil {
					result.err = errPipelineRead
				} else {
					block, err := btcutil.NewBlockFromBytes(
						item.data,
					)
					result.block = block
					result.err = err

					// Precompute every hash validation will
					// ask for.  Sanity checking recomputes
					// the merkle root, which hashes every
					// transaction; btcutil caches those
					// hashes on first use, so computing
					// them here moves that work off the
					// serial connect loop and onto the
					// workers.  The channel handoff is the
					// memory barrier that publishes the
					// cached values.
					if err == nil {
						block.Hash()
						for _, tx := range block.Transactions() {
							tx.Hash()
							tx.WitnessHash()
						}

						// Run the context-free sanity checks
						// here too, so the serial loop can
						// skip them via BFSanityDone.
						result.err = sanity(result.block)
					}
				}

				select {
				case parsed <- struct {
					seq uint64
					out parsedBlock
				}{seq: item.seq, out: result}:
				case <-quit:
					return
				}
			}
		}()
	}

	go func() {
		wg.Wait()
		close(parsed)
	}()

	// Reorderer: restores chain order before the connect loop sees them.
	go func() {
		defer close(out)

		pending := make(map[uint64]parsedBlock)
		var next uint64

		for item := range parsed {
			pending[item.seq] = item.out

			for {
				ready, ok := pending[next]
				if !ok {
					break
				}
				delete(pending, next)
				next++

				select {
				case out <- ready:
				case <-quit:
					return
				}
			}
		}
	}()

	var once sync.Once

	return out, func() { once.Do(func() { close(quit) }) }
}

// dirSize sums the on-disk size of a directory tree.
func dirSize(dir string) int64 {
	var total int64
	filepath.Walk(dir, func(_ string, info os.FileInfo, err error) error {
		if err == nil && !info.IsDir() {
			total += info.Size()
		}

		return nil
	})

	return total
}

func run(cfg *config) error {
	if cfg.src == "" || cfg.dst == "" {
		return fmt.Errorf("both --src and --dst are required")
	}

	if cfg.pprofAddr != "" {
		go func() {
			// Profiling endpoint only; failure to bind is not worth
			// stopping a replay over.
			_ = http.ListenAndServe(cfg.pprofAddr, nil)
		}()
	}

	debug.SetGCPercent(cfg.gogc)
	if cfg.memLimitGB > 0 {
		debug.SetMemoryLimit(int64(cfg.memLimitGB) << 30)
	}

	if cfg.cpuProfile != "" {
		f, err := os.Create(cfg.cpuProfile)
		if err != nil {
			return err
		}
		defer f.Close()
		if err := pprof.StartCPUProfile(f); err != nil {
			return err
		}
		defer pprof.StopCPUProfile()
	}

	if cfg.logPath != "" {
		f, err := os.Create(cfg.logPath)
		if err != nil {
			return err
		}
		defer f.Close()
		logOut = f
	}

	params := &chaincfg.MainNetParams

	reader, err := newBlockFileReader(cfg.src, params.Net)
	if err != nil {
		return err
	}

	if cfg.censusTo > 0 {
		return census(reader, cfg.censusFrom, cfg.censusTo, cfg.censusWin)
	}
	if cfg.hashAt > 0 {
		return hashAt(reader, cfg.hashAt)
	}

	// Reuse an existing database when there is one.  A full mainnet replay
	// runs for hours, and losing it to a crash or an interrupt near the end
	// is worse than the small cost of checking.
	db, err := database.Open("ffldb", cfg.dst, params.Net)
	if err != nil {
		db, err = database.Create("ffldb", cfg.dst, params.Net)
		if err != nil {
			return fmt.Errorf("failed to create database: %w", err)
		}
	}
	closed := false
	defer func() {
		if !closed {
			db.Close()
		}
	}()

	timeSource := blockchain.NewMedianTime()

	checkpoints := params.Checkpoints
	if cfg.noCheckpts {
		checkpoints = nil
	}

	openStart := time.Now()
	chain, err := blockchain.New(&blockchain.Config{
		DB:          db,
		ChainParams: params,
		TimeSource:  timeSource,

		// Without this the chain has no checkpoints at all:
		// findPreviousCheckpoint returns nil and the per-checkpoint
		// hash pinning silently never runs.  It is the anchor that
		// makes fast-add below the checkpoint safe, so it is not
		// optional.
		Checkpoints: checkpoints,

		// Full validation above the checkpoint fans signature checks
		// out across NumCPU*3 goroutines; these caches are what make
		// repeated signatures and sighash midstates cheap there.
		SigCache:  txscript.NewSigCache(uint(cfg.sigCache)),
		HashCache: txscript.NewHashCache(uint(cfg.sigCache / 5)),

		UtxoCacheMaxSize: uint64(cfg.utxoCache) * 1024 * 1024,
		UtxoApplyWorkers: cfg.utxoWorkers,
	})
	if err != nil {
		return fmt.Errorf("failed to create chain: %w", err)
	}

	if cfg.loadOnly {
		logf("chain state loaded to height %d in %s\n",
			chain.BestSnapshot().Height,
			time.Since(openStart).Truncate(time.Millisecond))

		return nil
	}

	// Fast-add applies only below the last checkpoint, which is the same
	// boundary a syncing node uses: below it the checkpoint hash pinning
	// anchors the chain, above it every rule runs.
	lastCheckpoint := int32(0)
	if checkpoint := chain.LatestCheckpoint(); checkpoint != nil {
		lastCheckpoint = checkpoint.Height
	}
	fastBelow := int32(0)
	if cfg.fastAdd {
		fastBelow = lastCheckpoint
	}
	if cfg.fastBelow > 0 {
		// An explicit boundary, for measuring full validation over a
		// chosen window; it is not a consensus setting.
		fastBelow = int32(cfg.fastBelow)
	}
	logf("fast-add below height %d, full validation above\n",
		fastBelow)

	// Above the checkpoint each block is validated while the previous one
	// is applied; Flush waits for the last apply so its error is not
	// lost and the chain is quiescent before the cache is flushed.
	pipe := chain.NewConnectPipeline()
	flushPipeline := func() error {
		if !cfg.pipeline {
			return nil
		}

		return pipe.Flush()
	}

	// Drain on every exit path, before the deferred database close runs,
	// so an apply still in flight cannot outlive the store it writes to.
	defer func() { _ = flushPipeline() }()

	mode := "fast-add below checkpoint, full validation above"
	if !cfg.fastAdd {
		mode = "full validation at every height"
	}
	fmt.Printf("replaying from %s\n", cfg.src)
	fmt.Printf("building     %s\n", cfg.dst)
	fmt.Printf("mode         %s\n", mode)
	fmt.Printf("utxo cache   %d MiB\n", cfg.utxoCache)
	fmt.Printf("pipeline     %d parse workers, depth %d\n",
		cfg.workers, cfg.depth)
	if resumeHeight := chain.BestSnapshot().Height; resumeHeight > 0 {
		logf("resuming     from height %d, seeking past records\n",
			resumeHeight)

		skipStart := time.Now()
		skipped, err := reader.skip(int(resumeHeight))
		if err != nil {
			return fmt.Errorf("failed to skip records: %w", err)
		}
		logf("             skipped %d records in %s\n", skipped,
			time.Since(skipStart).Truncate(time.Second))
	}
	fmt.Printf("\n")

	metadataPath := filepath.Join(cfg.dst, "metadata")
	start := time.Now()
	lastReport := start
	reportEvery := time.Duration(cfg.reportSecs) * time.Second

	var processed, skipped int64
	var lastProcessed int64

	// The height of the last block handed to the chain.  With the
	// pipeline the tip lags one block behind, so the tip cannot be used
	// to decide what the next block is or when to stop.
	fedHeight := chain.BestSnapshot().Height

	// Per-stage totals.  Which stage dominates decides what is worth
	// parallelising, so it is measured rather than assumed.
	var waitTime, processTime time.Duration

	sanity := func(b *btcutil.Block) error {
		return blockchain.CheckBlockSanity(b, params.PowLimit, timeSource)
	}
	blocks, stopPipeline := startPipeline(
		reader, cfg.workers, cfg.depth, sanity,
	)
	defer stopPipeline()

	for {
		stageStart := time.Now()
		item, ok := <-blocks
		waitTime += time.Since(stageStart)
		if !ok {
			break
		}
		if item.err != nil {
			if item.err == errPipelineRead {
				return reader.lastErr()
			}

			return fmt.Errorf("failed to parse block: %w", item.err)
		}
		block := item.block

		// The genesis block is created with the database, so replaying
		// it would be reported as a duplicate.
		if block.Hash().IsEqual(params.GenesisHash) {
			skipped++

			continue
		}

		// Sanity ran in the pipeline workers already.
		flags := blockchain.BFSanityDone
		nextHeight := fedHeight + 1
		if nextHeight <= fastBelow {
			flags |= blockchain.BFFastAdd
		}

		stageStart = time.Now()
		var isOrphan bool
		if cfg.pipeline && flags&blockchain.BFFastAdd == 0 {
			err = pipe.ProcessBlock(block, flags)
		} else {
			_, isOrphan, err = chain.ProcessBlock(block, flags)
		}
		processTime += time.Since(stageStart)
		if err != nil {
			// A block the chain already has means this run is
			// resuming over ground the previous one covered.
			var ruleErr blockchain.RuleError
			if errors.As(err, &ruleErr) &&
				ruleErr.ErrorCode == blockchain.ErrDuplicateBlock {

				skipped++
				fedHeight = chain.BestSnapshot().Height

				continue
			}

			return fmt.Errorf("failed to process block %s at "+
				"height %d: %w", block.Hash(), nextHeight, err)
		}
		if isOrphan {
			return fmt.Errorf("block %s is an orphan; the source "+
				"files are not in chain order", block.Hash())
		}
		processed++
		fedHeight = nextHeight

		if now := time.Now(); now.Sub(lastReport) >= reportEvery {
			best := chain.BestSnapshot()
			elapsed := now.Sub(lastReport).Seconds()
			rate := float64(processed-lastProcessed) / elapsed
			meta := dirSize(metadataPath)

			logf("height %7d  %7.1f blocks/s  "+
				"metadata %8.2f GB  elapsed %s\n",
				best.Height, rate,
				float64(meta)/(1<<30),
				now.Sub(start).Truncate(time.Second))

			lastReport = now
			lastProcessed = processed
		}

		if cfg.maxHeight > 0 && fedHeight >= int32(cfg.maxHeight) {

			break
		}
	}

	// Drain the pipeline before the tip is read: the last block is still
	// pending until then.
	if err := flushPipeline(); err != nil {
		return fmt.Errorf("pipeline: %w", err)
	}

	best := chain.BestSnapshot()
	elapsed := time.Since(start)

	// Flush the UTXO cache before anything is measured.
	//
	// The cache holds the live UTXO set in memory up to its configured
	// size and only spills to the database when it fills.  Closing the
	// database does not write it out -- that flushes the metadata cache,
	// which is a different layer.  Skipping this leaves the store holding
	// the block index and the undo journal but almost none of the UTXO
	// set, which both understates the footprint and leaves a chainstate
	// that could not be resumed.
	flushStart := time.Now()
	if err := chain.FlushUtxoCache(blockchain.FlushRequired); err != nil {
		return fmt.Errorf("failed to flush utxo cache: %w", err)
	}
	flushTime := time.Since(flushStart)

	// Close before measuring.  The metadata cache holds pending writes in
	// memory up to its size threshold, so measuring while the database is
	// open reports a fraction of the real footprint.
	if err := db.Close(); err != nil {
		return fmt.Errorf("failed to close database: %w", err)
	}
	closed = true

	meta := dirSize(metadataPath)
	total := dirSize(cfg.dst)

	fmt.Printf("\n=== done ===\n")
	logf("  height           %d\n", best.Height)
	logf("  blocks replayed  %d (%d skipped)\n", processed, skipped)
	logf("  elapsed          %s\n", elapsed.Truncate(time.Second))
	logf("  utxo flush       %s\n", flushTime.Truncate(time.Second))
	logf("  average rate     %.1f blocks/s\n",
		float64(processed)/elapsed.Seconds())
	logf("  metadata size    %.3f GB\n", float64(meta)/(1<<30))
	if ps := pipe.Stats(); cfg.pipeline && ps.Blocks > 0 {
		per := func(d time.Duration) string {
			return fmt.Sprintf("%8s  %6.1f ms/block", d.Truncate(time.Second),
				float64(d.Microseconds())/1000/float64(ps.Blocks))
		}
		logf("  pipeline blocks  %d\n", ps.Blocks)
		logf("  accept           %s\n", per(ps.Accept))
		logf("  prepare          %s\n", per(ps.Prepare))
		logf("  wait scripts     %s\n", per(ps.WaitScripts))
		logf("  wait apply       %s\n", per(ps.WaitApply))
		logf("  scripts (bg)     %s\n", per(ps.Scripts))
		logf("  apply lock wait  %s\n", per(ps.ApplyLock))
		logf("  apply (bg)       %s\n", per(ps.Apply))
		ss := chain.ConnectStageStats()
		logf("   fetch inputs    %s\n", per(time.Duration(ss.Fetch)))
		logf("   serial checks   %s\n", per(time.Duration(ss.Checks)))
		logf("   scripts         %s\n", per(time.Duration(ss.Scripts)))
	}
	totalStage := waitTime + processTime
	if totalStage > 0 {
		// Waiting on the pipeline is the read, checksum and parse cost
		// that could not be hidden behind the connect loop.  Near zero
		// means the pipeline is keeping the loop fed.
		logf("  pipeline wait    %8s  %5.1f%%\n",
			waitTime.Truncate(time.Second),
			float64(waitTime)/float64(totalStage)*100)
		logf("  process          %8s  %5.1f%%\n",
			processTime.Truncate(time.Second),
			float64(processTime)/float64(totalStage)*100)
	}
	logf("  total size       %.3f GB\n", float64(total)/(1<<30))
	if best.Height > 0 {
		logf("  metadata/block   %.1f bytes\n",
			float64(meta)/float64(best.Height))
	}

	return nil
}
