// Copyright (c) 2023 The btcsuite developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package blockchain

import (
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/btcsuite/btcd/btcutil/v2"
	"github.com/btcsuite/btcd/wire/v2"
)

// utxoApplyOp is one cache mutation from connecting a block: either the
// spend of an input or the creation of an output.
type utxoApplyOp struct {
	// txIn is non-nil for spends, and stxoIdx is the slot in the spent
	// txout journal the spend must fill.  The journal's order is consensus
	// serialization order, so every slot is assigned up front and the
	// entry can be written from whichever goroutine handles the shard.
	txIn    *wire.TxIn
	stxoIdx int

	// outpoint and txOut describe a creation when txIn is nil.
	outpoint wire.OutPoint
	txOut    *wire.TxOut
	coinbase bool
}

// connectTransactionsParallel behaves exactly like connectTransactions but
// spreads the cache mutations across worker goroutines.
//
// Correctness rests on one property: all operations on a given outpoint land
// in the same shard queue, in block order.  A transaction may spend an output
// created earlier in the same block; creation and spend share an outpoint, so
// they share a queue and their order is preserved.  The same holds for the
// historical duplicate-coinbase overwrites.  Operations on different
// outpoints carry no ordering constraint between them at all, which is what
// makes the fan-out sound.
//
// The work being spread is dominated by hash map probes over a working set
// far larger than any CPU cache, so it scales with memory-level parallelism
// rather than core count alone.
func (s *utxoCache) connectTransactionsParallel(block *btcutil.Block,
	stxos *[]SpentTxOut, workers int) error {

	height := block.Height()

	// Partition every operation by the shard its outpoint routes to.
	// Scanning the block serially and appending means each queue is
	// already in block order.
	queues := make([][]utxoApplyOp, utxoShardCount)
	spends := 0
	for _, tx := range block.Transactions() {
		isCoinBase := IsCoinBase(tx)
		if !isCoinBase {
			for _, txIn := range tx.MsgTx().TxIn {
				shard := shardIndexFor(&txIn.PreviousOutPoint)
				queues[shard] = append(queues[shard], utxoApplyOp{
					txIn:    txIn,
					stxoIdx: spends,
				})
				spends++
			}
		}

		prevOut := wire.OutPoint{Hash: *tx.Hash()}
		for outIdx, txOut := range tx.MsgTx().TxOut {
			prevOut.Index = uint32(outIdx)
			shard := shardIndexFor(&prevOut)
			queues[shard] = append(queues[shard], utxoApplyOp{
				outpoint: prevOut,
				txOut:    txOut,
				coinbase: isCoinBase,
			})
		}
	}

	// The journal is filled by slot rather than appended, so it is sized
	// to its final length up front.
	var journal []SpentTxOut
	if stxos != nil {
		if cap(*stxos) < spends {
			*stxos = make([]SpentTxOut, spends)
		} else {
			*stxos = (*stxos)[:spends]
		}
		journal = *stxos
	}

	if workers > utxoShardCount {
		workers = utxoShardCount
	}

	var wg sync.WaitGroup
	errs := make([]error, workers)
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()

			for shard := w; shard < utxoShardCount; shard += workers {
				for i := range queues[shard] {
					op := &queues[shard][i]

					var err error
					if op.txIn != nil {
						err = s.spendTxInAt(op.txIn,
							journal, op.stxoIdx)
					} else {
						err = s.addTxOut(op.outpoint,
							op.txOut, op.coinbase,
							height)
					}
					if err != nil {
						errs[w] = err

						return
					}
				}
			}
		}(w)
	}
	wg.Wait()

	for _, err := range errs {
		if err != nil {
			return err
		}
	}

	return nil
}

// spendTxInAt mirrors addTxIn exactly, except the journal entry is written
// into a fixed slot instead of appended, which is what lets spends complete
// out of global order while the journal keeps consensus order.  Any change
// to addTxIn must be reflected here.
func (s *utxoCache) spendTxInAt(txIn *wire.TxIn, journal []SpentTxOut,
	idx int) error {

	entries, err := s.fetchEntries([]wire.OutPoint{txIn.PreviousOutPoint})
	if err != nil {
		return err
	}
	if len(entries) != 1 || entries[0] == nil {
		return AssertError(fmt.Sprintf("missing input %v",
			txIn.PreviousOutPoint))
	}

	entry := entries[0]
	if journal != nil {
		journal[idx] = SpentTxOut{
			Amount:     entry.Amount(),
			PkScript:   entry.PkScript(),
			Height:     entry.BlockHeight(),
			IsCoinBase: entry.IsCoinBase(),
		}
	}

	entry.Spend()

	// A fresh entry was created after the last flush, so the database has
	// never seen it and it can simply be dropped.  A non-fresh entry stays
	// behind as a spent marker so the flush deletes it on disk.
	if entry.isFresh() {
		s.cachedEntries.delete(txIn.PreviousOutPoint)
		atomic.AddUint64(&s.totalEntryMemory, ^(entry.memoryUsage() - 1))
	} else {
		entry = nil
		atomic.AddUint64(&s.totalEntryMemory, ^(entry.memoryUsage() - 1))
	}

	return nil
}
