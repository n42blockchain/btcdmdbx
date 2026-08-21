package main

import (
	"fmt"

	"github.com/btcsuite/btcd/blockchain"
	"github.com/btcsuite/btcd/btcutil/v2"
	"github.com/btcsuite/btcd/chaincfg/v2"
)

// census counts what the blocks in [from, to] contain, per window of the
// given size, reading the corpus in order and treating the record ordinal as
// the height.  It never opens the database.
//
// It exists because blocks/s is a poor unit for comparing runs over different
// height ranges: adjacent 3,000-block windows above the checkpoint differ by
// fifteen percent or more in how many inputs they carry, which is the same
// order as the effects being measured.  Inputs/s normalises that away.
func census(reader *blockFileReader, from, to, window int) error {
	if from > 0 {
		if _, err := reader.skip(from); err != nil {
			return err
		}
	}

	fmt.Printf("%-16s %7s %9s %9s %9s %9s\n", "window", "blocks",
		"txs", "inputs", "witIns", "MB")

	var blocks, txs, inputs, witIns, bytes int
	winStart := from
	height := from
	flush := func() {
		fmt.Printf("%7d-%-8d %7d %9d %9d %9d %9.1f\n", winStart,
			height-1, blocks, txs, inputs, witIns,
			float64(bytes)/(1<<20))
		blocks, txs, inputs, witIns, bytes = 0, 0, 0, 0, 0
		winStart = height
	}

	for height <= to {
		raw, err := reader.next()
		if err != nil {
			return err
		}
		if raw == nil {
			break
		}

		block, err := btcutil.NewBlockFromBytes(raw)
		if err != nil {
			return err
		}
		if block.Hash().IsEqual(chaincfg.MainNetParams.GenesisHash) {
			continue
		}

		height++
		blocks++
		bytes += len(raw)
		for _, tx := range block.Transactions() {
			txs++
			if blockchain.IsCoinBase(tx) {
				continue
			}
			msg := tx.MsgTx()
			inputs += len(msg.TxIn)
			if msg.HasWitness() {
				witIns += len(msg.TxIn)
			}
		}

		if blocks == window {
			flush()
		}
	}
	if blocks > 0 {
		flush()
	}

	return nil
}
