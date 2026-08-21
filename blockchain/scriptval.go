// Copyright (c) 2013-2016 The btcsuite developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package blockchain

import (
	"fmt"
	"math"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/btcsuite/btcd/btcutil/v2"
	"github.com/btcsuite/btcd/txscript/v2"
	"github.com/btcsuite/btcd/wire/v2"
)

// txValidateItem holds a transaction along with which input to validate.
type txValidateItem struct {
	txInIndex int
	txIn      *wire.TxIn
	tx        *btcutil.Tx
	sigHashes *txscript.TxSigHashes
}

// txValidator provides a type which validates transaction inputs across a
// pool of goroutines.
type txValidator struct {
	utxoView  *UtxoViewpoint
	flags     txscript.ScriptFlags
	sigCache  *txscript.SigCache
	hashCache *txscript.HashCache
}

// validateItem validates a single transaction input.
func (v *txValidator) validateItem(txVI *txValidateItem) error {
	// Ensure the referenced input utxo is available.
	txIn := txVI.txIn
	utxo := v.utxoView.LookupEntry(txIn.PreviousOutPoint)
	if utxo == nil {
		str := fmt.Sprintf("unable to find unspent "+
			"output %v referenced from "+
			"transaction %s:%d",
			txIn.PreviousOutPoint, txVI.tx.Hash(),
			txVI.txInIndex)
		return ruleError(ErrMissingTxOut, str)
	}

	// Create a new script engine for the script pair.
	sigScript := txIn.SignatureScript
	witness := txIn.Witness
	pkScript := utxo.PkScript()
	inputAmount := utxo.Amount()
	vm, err := txscript.NewEngine(
		pkScript, txVI.tx.MsgTx(), txVI.txInIndex,
		v.flags, v.sigCache, txVI.sigHashes,
		inputAmount, v.utxoView,
	)
	if err != nil {
		str := fmt.Sprintf("failed to parse input "+
			"%s:%d which references output %v - "+
			"%v (input witness %x, input script "+
			"bytes %x, prev output script bytes %x)",
			txVI.tx.Hash(), txVI.txInIndex,
			txIn.PreviousOutPoint, err, witness,
			sigScript, pkScript)
		return ruleError(ErrScriptMalformed, str)
	}

	// Execute the script pair.
	if err := vm.Execute(); err != nil {
		str := fmt.Sprintf("failed to validate input "+
			"%s:%d which references output %v - "+
			"%v (input witness %x, input script "+
			"bytes %x, prev output script bytes %x)",
			txVI.tx.Hash(), txVI.txInIndex,
			txIn.PreviousOutPoint, err, witness,
			sigScript, pkScript)
		return ruleError(ErrScriptValidation, str)
	}

	return nil
}

// Validate validates the scripts for all of the passed transaction inputs
// using multiple goroutines.
//
// Items are handed out through an atomic counter rather than channels.  The
// historical design pushed every item and every result through unbuffered
// channels between a dispatcher and NumCPU*3 workers, costing two cross
// thread wakeups per input; on signature-dense blocks the scheduler churn
// from those wakeups was a measurable fraction of total validation time.
// Claiming work with one atomic add removes the churn, and with no channel
// blocking to hide there is no reason to oversubscribe the CPUs either.
func (v *txValidator) Validate(items []*txValidateItem) error {
	if len(items) == 0 {
		return nil
	}

	maxGoRoutines := runtime.NumCPU()
	if maxGoRoutines <= 0 {
		maxGoRoutines = 1
	}
	if maxGoRoutines > len(items) {
		maxGoRoutines = len(items)
	}

	// A validation failure parks the counter past the end of the items,
	// so every worker stops claiming work at its next iteration.  Items
	// already in flight run to completion, exactly as before.
	var next atomic.Uint64
	errs := make([]error, maxGoRoutines)

	var wg sync.WaitGroup
	for w := 0; w < maxGoRoutines; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()

			for {
				i := next.Add(1) - 1
				if i >= uint64(len(items)) {
					return
				}

				if err := v.validateItem(items[i]); err != nil {
					errs[w] = err
					next.Store(uint64(len(items)))

					return
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

// newTxValidator returns a new instance of txValidator to be used for
// validating transaction scripts asynchronously.
func newTxValidator(utxoView *UtxoViewpoint, flags txscript.ScriptFlags,
	sigCache *txscript.SigCache, hashCache *txscript.HashCache) *txValidator {
	return &txValidator{
		utxoView:  utxoView,
		sigCache:  sigCache,
		hashCache: hashCache,
		flags:     flags,
	}
}

// ValidateTransactionScripts validates the scripts for the passed transaction
// using multiple goroutines.
func ValidateTransactionScripts(tx *btcutil.Tx, utxoView *UtxoViewpoint,
	flags txscript.ScriptFlags, sigCache *txscript.SigCache,
	hashCache *txscript.HashCache) error {

	// First determine if segwit is active according to the scriptFlags. If
	// it isn't then we don't need to interact with the HashCache.
	segwitActive := flags&txscript.ScriptVerifyWitness == txscript.ScriptVerifyWitness

	// If the hashcache doesn't yet has the sighash midstate for this
	// transaction, then we'll compute them now so we can re-use them
	// amongst all worker validation goroutines.
	if segwitActive && tx.MsgTx().HasWitness() &&
		!hashCache.ContainsHashes(tx.Hash()) {
		hashCache.AddSigHashes(tx.MsgTx(), utxoView)
	}

	var cachedHashes *txscript.TxSigHashes
	if segwitActive && tx.MsgTx().HasWitness() {
		// The same pointer to the transaction's sighash midstate will
		// be re-used amongst all validation goroutines. By
		// pre-computing the sighash here instead of during validation,
		// we ensure the sighashes
		// are only computed once.
		cachedHashes, _ = hashCache.GetSigHashes(tx.Hash())
	}

	// Collect all of the transaction inputs and required information for
	// validation.
	txIns := tx.MsgTx().TxIn
	txValItems := make([]*txValidateItem, 0, len(txIns))
	for txInIdx, txIn := range txIns {
		// Skip coinbases.
		if txIn.PreviousOutPoint.Index == math.MaxUint32 {
			continue
		}

		txVI := &txValidateItem{
			txInIndex: txInIdx,
			txIn:      txIn,
			tx:        tx,
			sigHashes: cachedHashes,
		}
		txValItems = append(txValItems, txVI)
	}

	// Validate all of the inputs.
	validator := newTxValidator(utxoView, flags, sigCache, hashCache)
	return validator.Validate(txValItems)
}

// checkBlockScripts executes and validates the scripts for all transactions in
// the passed block using multiple goroutines.
func checkBlockScripts(block *btcutil.Block, utxoView *UtxoViewpoint,
	scriptFlags txscript.ScriptFlags, sigCache *txscript.SigCache,
	hashCache *txscript.HashCache) error {

	// First determine if segwit is active according to the scriptFlags. If
	// it isn't then we don't need to interact with the HashCache.
	segwitActive := scriptFlags&txscript.ScriptVerifyWitness == txscript.ScriptVerifyWitness

	// Collect all of the transaction inputs and required information for
	// validation for all transactions in the block into a single slice.
	numInputs := 0
	for _, tx := range block.Transactions() {
		numInputs += len(tx.MsgTx().TxIn)
	}
	// Compute the BIP0143 and BIP0341 sighash midstates for every witness
	// transaction before any input is dispatched.
	//
	// This used to happen inline, one transaction at a time, under the
	// hash cache's exclusive lock: the midstates hash the transaction's
	// prevouts, sequences, outputs, and for taproot the spent amounts and
	// scripts as well, which is several passes of SHA-256 over every
	// transaction in the block on the serial path.  They depend only on
	// the transaction and the read-only utxo view, so they are computed
	// here across all CPUs.  Entries the hash cache already holds -- from
	// the mempool, on a serving node -- are reused; nothing is inserted,
	// since the cache entry for a confirmed transaction was purged at the
	// end of this function anyway.
	transactions := block.Transactions()
	sigHashesByTx := make([]*txscript.TxSigHashes, len(transactions))
	if segwitActive {
		workers := runtime.NumCPU()
		if workers > len(transactions) {
			workers = len(transactions)
		}

		var next atomic.Uint64
		var wg sync.WaitGroup
		for w := 0; w < workers; w++ {
			wg.Add(1)
			go func() {
				defer wg.Done()

				for {
					i := next.Add(1) - 1
					if i >= uint64(len(transactions)) {
						return
					}

					tx := transactions[i]
					if !tx.HasWitness() {
						continue
					}
					if hashCache != nil {
						hashes, ok := hashCache.GetSigHashes(
							tx.Hash(),
						)
						if ok {
							sigHashesByTx[i] = hashes

							continue
						}
					}
					sigHashesByTx[i] = txscript.NewTxSigHashes(
						tx.MsgTx(), utxoView,
					)
				}
			}()
		}
		wg.Wait()
	}

	txValItems := make([]*txValidateItem, 0, numInputs)
	for txIdx, tx := range transactions {
		cachedHashes := sigHashesByTx[txIdx]

		for txInIdx, txIn := range tx.MsgTx().TxIn {
			// Skip coinbases.
			if txIn.PreviousOutPoint.Index == math.MaxUint32 {
				continue
			}

			txVI := &txValidateItem{
				txInIndex: txInIdx,
				txIn:      txIn,
				tx:        tx,
				sigHashes: cachedHashes,
			}
			txValItems = append(txValItems, txVI)
		}
	}

	// Validate all of the inputs.
	validator := newTxValidator(utxoView, scriptFlags, sigCache, hashCache)
	start := time.Now()
	if err := validator.Validate(txValItems); err != nil {
		return err
	}
	elapsed := time.Since(start)

	log.Tracef("block %v took %v to verify", block.Hash(), elapsed)

	// If the HashCache is present, once we have validated the block, we no
	// longer need the cached hashes for these transactions, so we purge
	// them from the cache.
	if segwitActive && hashCache != nil {
		for _, tx := range block.Transactions() {
			if tx.MsgTx().HasWitness() {
				hashCache.PurgeSigHashes(tx.Hash())
			}
		}
	}

	return nil
}
