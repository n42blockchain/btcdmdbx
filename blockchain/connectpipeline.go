// Copyright (c) 2023 The btcsuite developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package blockchain

import (
	"time"

	"github.com/btcsuite/btcd/btcutil/v2"
)

// ConnectPipeline connects a sequence of blocks with the validation of each
// block overlapped with the application of the one before it.
//
// ProcessBlock runs every stage of a block serially under the chain lock:
// contextual checks, script validation, applying the transactions to the utxo
// cache, and the database commit.  Above the last checkpoint that leaves the
// connect goroutine waiting on a parallel script stage and then doing the
// serial apply and commit while the validators sit idle.  The pipeline
// splits the work into two halves that can run concurrently for consecutive
// blocks:
//
//	accept(N)  validate(N)
//	                      accept(N+1)  apply(N) | validate(N+1)
//	                                              accept(N+2)  apply(N+1) | validate(N+2)
//
// A block is accepted as soon as it arrives, and the apply of the block
// before it is started only then, so that it runs alongside this block's
// validation instead of alongside nothing.
//
// validate(N+1) needs the utxo state as it will be once apply(N) has landed,
// and apply(N) is still in flight.  It gets that state from an overlay: the
// viewpoint produced by validate(N) holds exactly N's delta -- every input N
// spent, marked spent, and every output N created -- so fetches consult it
// before the cache.  Everything N did not touch is read from the cache, which
// by then already contains apply(N-1): only one apply is ever in flight, and
// the pipeline waits for it before accepting the next block.
//
// Concurrent access this relies on: the utxo cache's shards are independently
// locked and the flush holds each shard it walks; the block index and the
// threshold-state caches carry their own locks; and the database serves read
// views during a write transaction.
//
// A ConnectPipeline is not safe for concurrent use; feed it from one
// goroutine and call Flush when done.
type ConnectPipeline struct {
	b *BlockChain

	// prevView is the viewpoint of the last validated block, used as the
	// overlay for the next one.
	prevView *UtxoViewpoint

	// inflight completes when the outstanding apply finishes, carrying its
	// error.  Nil when nothing is in flight.
	inflight chan error

	// pending is the last validated block, not yet applied.  Its apply is
	// started when the next block arrives, so that it runs while the next
	// block is being validated.
	pendingNode  *blockNode
	pendingBlock *btcutil.Block
	pendingFlags BehaviorFlags

	stats PipelineStats
}

// PipelineStats accumulates where the pipeline spent its time, so the
// overlap can be measured rather than assumed.  Accept, Validate and
// WaitApply are on the calling goroutine and sum to roughly the time spent
// inside ProcessBlock; ApplyLock and Apply are on the background goroutine
// and overlap with the caller.
type PipelineStats struct {
	// Blocks is the number of blocks that went through validation.
	Blocks int

	// Accept is time under the chain lock storing and indexing blocks.
	Accept time.Duration

	// Validate is time in checkConnectBlock, off the lock.
	Validate time.Duration

	// WaitApply is time the caller spent blocked on an in-flight apply;
	// zero means the apply was always hidden behind validation.
	WaitApply time.Duration

	// ApplyLock is time the apply goroutine waited for the chain lock.
	ApplyLock time.Duration

	// Apply is time the apply goroutine held the chain lock.
	Apply time.Duration
}

// NewConnectPipeline returns a pipeline over the chain.
func (b *BlockChain) NewConnectPipeline() *ConnectPipeline {
	return &ConnectPipeline{b: b}
}

// waitInflight waits for the outstanding apply, if any, and returns its
// error.
func (p *ConnectPipeline) waitInflight() error {
	if p.inflight == nil {
		return nil
	}

	err := <-p.inflight
	p.inflight = nil

	return err
}

// ProcessBlock checks, stores and indexes the block, validates its
// transactions against the overlay of the previous block, and then queues it
// to be applied once the previous block's application has finished.
//
// The flags are honoured as ProcessBlock honours them.  A block that carries
// BFFastAdd skips transaction validation exactly as it would on the direct
// path; the pipeline is only worth its overhead above the last checkpoint.
func (p *ConnectPipeline) ProcessBlock(block *btcutil.Block,
	flags BehaviorFlags) error {

	b := p.b

	// Only one apply may be outstanding: the next block's validation reads
	// the cache for everything its overlay does not cover, and that read
	// must see every block before the overlay already applied.
	t0 := time.Now()
	if err := p.waitInflight(); err != nil {
		return err
	}
	t1 := time.Now()
	p.stats.WaitApply += t1.Sub(t0)

	// Accept under the lock while nothing is being applied; the checks
	// here depend only on the block index, never on the chain tip.
	b.chainLock.Lock()
	_, isOrphan, node, err := b.processBlock(block, flags, false)
	b.chainLock.Unlock()
	p.stats.Accept += time.Since(t1)
	if err != nil {
		return err
	}
	if isOrphan {
		return ruleError(ErrPreviousBlockUnknown, "pipeline cannot "+
			"connect an orphan block")
	}

	// Now start applying the previous block.  From here until the apply
	// completes the chain lock is held by the apply, and this block is
	// validated against the overlay rather than the chain tip.
	p.startPending()

	// Validate off the lock.  This is the stage that fans out across the
	// CPUs, and the stage that is now overlapped with the apply of the
	// block before.
	fastAdd := flags&BFFastAdd == BFFastAdd
	var view *UtxoViewpoint
	if !fastAdd && !b.index.NodeStatus(node).KnownValid() {
		view = NewUtxoViewpoint()
		view.overlay = p.prevView
		view.SetBestHash(&block.MsgBlock().Header.PrevBlock)

		t2 := time.Now()
		err := b.checkConnectBlock(node, block, view, nil)
		p.stats.Validate += time.Since(t2)
		p.stats.Blocks++
		if err != nil {
			if _, ok := err.(RuleError); ok {
				b.index.SetStatusFlags(node, statusValidateFailed)
			}

			// The previous block is still being applied; let it land
			// and surface its error first, then the index state.
			if werr := p.waitInflight(); werr != nil {
				return werr
			}
			b.chainLock.Lock()
			_ = b.index.flushToDB()
			b.chainLock.Unlock()

			return err
		}

		// Marking the node valid is what makes connectBestChain skip its
		// own validation pass when the apply runs.
		b.index.SetStatusFlags(node, statusValid)
	}

	// The overlay for the next block is this block's delta.  A block that
	// skipped validation has no delta to offer, and the next block must
	// then wait for the cache, which the in-flight wait above guarantees.
	p.prevView = view
	p.pendingNode = node
	p.pendingBlock = block
	p.pendingFlags = flags

	return nil
}

// startPending starts applying the pending block, if there is one, in the
// background.  connectBestChain sees the node already marked valid and goes
// straight to the cache update and the commit.
func (p *ConnectPipeline) startPending() {
	if p.pendingNode == nil {
		return
	}
	b := p.b
	node, block, flags := p.pendingNode, p.pendingBlock, p.pendingFlags
	p.pendingNode, p.pendingBlock = nil, nil

	done := make(chan error, 1)
	p.inflight = done
	stats := &p.stats
	go func() {
		t0 := time.Now()
		b.chainLock.Lock()
		defer b.chainLock.Unlock()
		t1 := time.Now()

		_, err := b.connectBestChain(node, block, flags)

		// The caller only reads these after receiving from done, so
		// the channel orders the writes.
		stats.ApplyLock += t1.Sub(t0)
		stats.Apply += time.Since(t1)
		done <- err
	}()
}

// Stats returns the accumulated timing.  Call it after Flush, or at least
// between calls to ProcessBlock, since the apply goroutine updates it.
func (p *ConnectPipeline) Stats() PipelineStats {
	return p.stats
}

// Flush applies the pending block, waits for it, and returns the first
// error.  It must be called before the chain is used for anything else.
func (p *ConnectPipeline) Flush() error {
	err := p.waitInflight()
	if err == nil {
		p.startPending()
		err = p.waitInflight()
	}
	p.prevView = nil
	p.pendingNode, p.pendingBlock = nil, nil

	// The last block's valid status was set after its index entry was
	// written; make sure it reaches the database.
	b := p.b
	b.chainLock.Lock()
	if ferr := b.index.flushToDB(); err == nil && ferr != nil {
		err = ferr
	}
	b.chainLock.Unlock()

	return err
}
