// Command replayblocks rebuilds a chainstate by replaying the blocks held in
// an existing flat block file set.
//
// It exists to measure the storage layer against real mainnet data without
// waiting on the network. The block files produced by this package are
// self-describing, so an existing node's block files can be replayed into a
// fresh database and the resulting throughput and metadata size recorded.
//
// The source files are only ever read. The blocks are validated in full by the
// same code path a syncing node uses, so the resulting chainstate is the one a
// node would have built for itself.
package main

import (
	"encoding/binary"
	"flag"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/btcsuite/btcd/blockchain"
	"github.com/btcsuite/btcd/btcutil/v2"
	"github.com/btcsuite/btcd/chaincfg/v2"
	"github.com/btcsuite/btcd/database"
	_ "github.com/btcsuite/btcd/database/ffldb"
	"github.com/btcsuite/btcd/wire/v2"
)

// castagnoli houses the Castagnoli polynomial used for the CRC-32 checksums
// the block files carry.
var castagnoli = crc32.MakeTable(crc32.Castagnoli)

// blockFileExtension is the suffix every block file carries.
const blockFileExtension = ".fdb"

type config struct {
	src        string
	dst        string
	maxHeight  int
	reportSecs int
	fastAdd    bool
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
	flag.BoolVar(&cfg.fastAdd, "fastadd", false,
		"skip the checks a checkpointed block does not need, which is "+
			"what a syncing node does below its last checkpoint")
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

	params := &chaincfg.MainNetParams

	reader, err := newBlockFileReader(cfg.src, params.Net)
	if err != nil {
		return err
	}

	db, err := database.Create("ffldb", cfg.dst, params.Net)
	if err != nil {
		return fmt.Errorf("failed to create database: %w", err)
	}
	closed := false
	defer func() {
		if !closed {
			db.Close()
		}
	}()

	chain, err := blockchain.New(&blockchain.Config{
		DB:          db,
		ChainParams: params,
		TimeSource:  blockchain.NewMedianTime(),
	})
	if err != nil {
		return fmt.Errorf("failed to create chain: %w", err)
	}

	flags := blockchain.BFNone
	if cfg.fastAdd {
		flags = blockchain.BFFastAdd
	}

	fmt.Printf("replaying from %s\n", cfg.src)
	fmt.Printf("building     %s\n\n", cfg.dst)

	metadataPath := filepath.Join(cfg.dst, "metadata")
	start := time.Now()
	lastReport := start
	reportEvery := time.Duration(cfg.reportSecs) * time.Second

	var processed, skipped int64
	var lastProcessed int64

	for {
		serialized, err := reader.next()
		if err != nil {
			return err
		}
		if serialized == nil {
			break
		}

		block, err := btcutil.NewBlockFromBytes(serialized)
		if err != nil {
			return fmt.Errorf("failed to parse block: %w", err)
		}

		// The genesis block is created with the database, so replaying
		// it would be reported as a duplicate.
		if block.Hash().IsEqual(params.GenesisHash) {
			skipped++

			continue
		}

		_, isOrphan, err := chain.ProcessBlock(block, flags)
		if err != nil {
			return fmt.Errorf("failed to process block %s at "+
				"height %d: %w", block.Hash(),
				chain.BestSnapshot().Height+1, err)
		}
		if isOrphan {
			return fmt.Errorf("block %s is an orphan; the source "+
				"files are not in chain order", block.Hash())
		}
		processed++

		if now := time.Now(); now.Sub(lastReport) >= reportEvery {
			best := chain.BestSnapshot()
			elapsed := now.Sub(lastReport).Seconds()
			rate := float64(processed-lastProcessed) / elapsed
			meta := dirSize(metadataPath)

			fmt.Printf("height %7d  %7.1f blocks/s  "+
				"metadata %8.2f GB  elapsed %s\n",
				best.Height, rate,
				float64(meta)/(1<<30),
				now.Sub(start).Truncate(time.Second))

			lastReport = now
			lastProcessed = processed
		}

		if cfg.maxHeight > 0 &&
			chain.BestSnapshot().Height >= int32(cfg.maxHeight) {

			break
		}
	}

	best := chain.BestSnapshot()
	elapsed := time.Since(start)

	// Close before measuring.  The cache holds pending metadata in
	// memory up to its size threshold, so measuring while the database
	// is open reports a fraction of the real footprint.
	if err := db.Close(); err != nil {
		return fmt.Errorf("failed to close database: %w", err)
	}
	closed = true

	meta := dirSize(metadataPath)
	total := dirSize(cfg.dst)

	fmt.Printf("\n=== done ===\n")
	fmt.Printf("  height           %d\n", best.Height)
	fmt.Printf("  blocks replayed  %d (%d skipped)\n", processed, skipped)
	fmt.Printf("  elapsed          %s\n", elapsed.Truncate(time.Second))
	fmt.Printf("  average rate     %.1f blocks/s\n",
		float64(processed)/elapsed.Seconds())
	fmt.Printf("  metadata size    %.3f GB\n", float64(meta)/(1<<30))
	fmt.Printf("  total size       %.3f GB\n", float64(total)/(1<<30))
	if best.Height > 0 {
		fmt.Printf("  metadata/block   %.1f bytes\n",
			float64(meta)/float64(best.Height))
	}

	return nil
}
