// Copyright (c) 2023 The btcsuite developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package blockchain

import (
	"testing"

	"github.com/btcsuite/btcd/chaincfg/v2"
	"github.com/btcsuite/btcd/wire/v2"
)

// TestConnectPipeline feeds a short main-chain prefix through the pipeline
// and checks the chain ends up exactly where the direct path puts it: same
// tip, every block marked valid, and the same utxo set.
func TestConnectPipeline(t *testing.T) {
	blocks, err := loadBlocks("blk_0_to_4.dat.bz2")
	if err != nil {
		t.Fatalf("Error loading file: %v", err)
	}

	// Direct path, the reference.
	direct, teardownDirect, err := chainSetup("pipeline-direct",
		&chaincfg.MainNetParams)
	if err != nil {
		t.Fatalf("Failed to setup chain instance: %v", err)
	}
	defer teardownDirect()
	direct.TstSetCoinbaseMaturity(1)
	for _, block := range blocks[1:] {
		if _, _, err := direct.ProcessBlock(block, BFNone); err != nil {
			t.Fatalf("direct ProcessBlock: %v", err)
		}
	}

	// Pipelined path.
	chain, teardownFunc, err := chainSetup("pipeline",
		&chaincfg.MainNetParams)
	if err != nil {
		t.Fatalf("Failed to setup chain instance: %v", err)
	}
	defer teardownFunc()
	chain.TstSetCoinbaseMaturity(1)

	pipe := chain.NewConnectPipeline()
	for _, block := range blocks[1:] {
		if err := pipe.ProcessBlock(block, BFNone); err != nil {
			t.Fatalf("pipeline ProcessBlock: %v", err)
		}
	}

	// Before Flush the tip must lag: the last block is still pending.
	if got := chain.BestSnapshot().Height; got >= int32(len(blocks)-1) {
		t.Fatalf("tip %d reached the last block before Flush", got)
	}
	if err := pipe.Flush(); err != nil {
		t.Fatalf("pipeline Flush: %v", err)
	}

	want := direct.BestSnapshot()
	got := chain.BestSnapshot()
	if got.Height != want.Height || got.Hash != want.Hash {
		t.Fatalf("tip mismatch: got %d/%v, want %d/%v", got.Height,
			got.Hash, want.Height, want.Hash)
	}

	// A second Flush must be a no-op.
	if err := pipe.Flush(); err != nil {
		t.Fatalf("second Flush: %v", err)
	}

	// Every block must have been validated, not merely stored, and the
	// status must have reached the database.
	for _, block := range blocks[1:] {
		node := chain.index.LookupNode(block.Hash())
		if node == nil {
			t.Fatalf("block %v missing from index", block.Hash())
		}
		if !chain.index.NodeStatus(node).KnownValid() {
			t.Fatalf("block %v not marked valid", block.Hash())
		}
		if _, dirty := chain.index.dirty[node]; dirty {
			t.Fatalf("block %v status not flushed", block.Hash())
		}
	}

	// The utxo sets must agree on every coinbase output.
	for _, block := range blocks[1:] {
		out := wire.OutPoint{Hash: *block.Transactions()[0].Hash()}
		wantEntry, err := direct.FetchUtxoEntry(out)
		if err != nil {
			t.Fatalf("direct FetchUtxoEntry: %v", err)
		}
		gotEntry, err := chain.FetchUtxoEntry(out)
		if err != nil {
			t.Fatalf("pipeline FetchUtxoEntry: %v", err)
		}
		if (wantEntry == nil) != (gotEntry == nil) {
			t.Fatalf("utxo %v: presence differs", out)
		}
		if wantEntry == nil {
			continue
		}
		if wantEntry.Amount() != gotEntry.Amount() ||
			wantEntry.BlockHeight() != gotEntry.BlockHeight() ||
			wantEntry.IsSpent() != gotEntry.IsSpent() {

			t.Fatalf("utxo %v differs between paths", out)
		}
	}
}
