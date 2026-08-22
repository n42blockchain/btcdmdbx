// Copyright (c) 2023 The btcsuite developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package blockchain

import (
	"time"

	"github.com/btcsuite/btcd/btcutil/v2"
)

// ConnectPipeline connects a sequence of blocks with the stages of
// consecutive blocks overlapped.
//
// ProcessBlock runs every stage of a block serially under the chain lock:
// contextual checks, loading the inputs, the cheap per-transaction checks,
// script validation, applying the transactions to the utxo cache, and the
// database commit.  Above the last checkpoint script validation is the bulk
// of the wall time and already fans out across every CPU; everything else is
// serial and sits on the same goroutine before or after it.  The pipeline
// pulls the serial stages of the next block alongside the scripts of this
// one:
//
//	accept(N)  prepare(N)  scripts(N) ........................
//	                       accept(N+1)  prepare(N+1)  | apply(N)  scripts(N+1) .......
//	                                                  accept(N+2)  prepare(N+2)  | apply(N+1)  ...
//
// prepare(N+1) loads N+1's inputs and runs the checks that need no scripts.
// It needs the utxo state as it will be once N has been applied, and N has
// not even finished its scripts.  It gets that state from an overlay: the
// viewpoint produced by prepare(N) holds exactly N's delta -- every input N
// spent, marked spent, and every output N created -- so fetches consult it
// before the cache.  Everything N did not touch is read from the cache, which
// by then already contains apply(N-1): only one apply is ever in flight, and
// the pipeline waits for it before accepting the next block.
//
// scripts(N) reads only N's own viewpoint, so it runs alongside prepare(N+1)
// (which reads that viewpoint as its overlay, and never writes it) and
// alongside apply(N-1).  apply(N) starts once scripts(N) has passed, and
// scripts(N+1) starts right after, so the validators are never idle while
// the caller does serial work.
//
// Concurrent access this relies on: the utxo cache's shards are independently
// locked and the flush holds each shard it walks; the block index and the
// threshold-state caches carry their own locks; and the database serves read
// views during a write transaction.
//
// A ConnectPipeline is not safe for concurrent use; feed it from one
// goroutine and call Flush when done.  After an error the pipeline must not
// be used again: the blocks after the failing one have been accepted into
// the index but will never be applied.
type ConnectPipeline struct {
	b *BlockChain

	// inflight completes when the outstanding apply finishes, carrying its
	// error.  Nil when nothing is in flight.
	inflight chan error

	// scripted is the block whose scripts are being validated in the
	// background, and scriptsDone carries the result.  Its view is the
	// overlay for the block being prepared.
	scripted    *pipelineBlock
	scriptsDone chan error

	stats PipelineStats
}

// pipelineBlock is a block that has been accepted and prepared.
type pipelineBlock struct {
	node  *blockNode
	block *btcutil.Block
	flags BehaviorFlags

	// view holds the block's delta once prepared; nil when the block
	// skipped validation.
	view    *UtxoViewpoint
	scripts connectScripts
}

// PipelineStats accumulates where the pipeline spent its time, so the
// overlap can be measured rather than assumed.  Accept, Prepare, WaitScripts
// and WaitApply are on the calling goroutine and sum to roughly the time
// spent inside ProcessBlock; Scripts, ApplyLock and Apply are on background
// goroutines and overlap with the caller.
type PipelineStats struct {
	// Blocks is the number of blocks that went through validation.
	Blocks int

	// Accept is time under the chain lock storing and indexing blocks.
	Accept time.Duration

	// Prepare is time loading inputs and running the pre-script checks,
	// off the lock, overlapped with the previous block's scripts.
	Prepare time.Duration

	// WaitScripts is time the caller spent blocked on the previous
	// block's scripts after preparing this one.
	WaitScripts time.Duration

	// WaitApply is time the caller spent blocked on an in-flight apply;
	// zero means the apply was always hidden.
	WaitApply time.Duration

	// Scripts is time the script stage took, on its own goroutine.
	Scripts time.Duration

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

// waitScripts waits for the background script validation, if any, and
// returns the block it validated.  On a rule failure the block is marked
// invalid in the index.
func (p *ConnectPipeline) waitScripts() (*pipelineBlock, error) {
	if p.scripted == nil {
		return nil, nil
	}

	err := <-p.scriptsDone
	pb := p.scripted
	p.scripted, p.scriptsDone = nil, nil
	if err != nil {
		if _, ok := err.(RuleError); ok {
			p.b.index.SetStatusFlags(pb.node, statusValidateFailed)
		}

		return nil, err
	}

	// Marking the node valid is what makes connectBestChain skip its own
	// validation pass when the apply runs.
	if pb.view != nil {
		p.b.index.SetStatusFlags(pb.node, statusValid)
	}

	return pb, nil
}

// startScripts starts validating the block's scripts in the background.
func (p *ConnectPipeline) startScripts(pb *pipelineBlock) {
	done := make(chan error, 1)
	p.scripted, p.scriptsDone = pb, done
	if pb.view == nil {
		// Nothing to validate; the block carried BFFastAdd or was
		// already known valid.
		done <- nil

		return
	}

	b := p.b
	stats := &p.stats
	go func() {
		t0 := time.Now()
		err := b.runConnectScripts(pb.node, pb.block, pb.view, pb.scripts)

		// The caller only reads this after receiving from done, so the
		// channel orders the write.
		stats.Scripts += time.Since(t0)
		done <- err
	}()
}

// startApply starts applying the block in the background.  connectBestChain
// sees the node already marked valid and goes straight to the cache update
// and the commit.
func (p *ConnectPipeline) startApply(pb *pipelineBlock) {
	b := p.b
	done := make(chan error, 1)
	p.inflight = done
	stats := &p.stats
	go func() {
		t0 := time.Now()
		b.chainLock.Lock()
		defer b.chainLock.Unlock()
		t1 := time.Now()

		_, err := b.connectBestChain(pb.node, pb.block, pb.flags)

		stats.ApplyLock += t1.Sub(t0)
		stats.Apply += time.Since(t1)
		done <- err
	}()
}

// ProcessBlock checks, stores and indexes the block, prepares it against the
// overlay of the previous block while that block's scripts are still being
// validated, then queues the previous block to be applied and this block's
// scripts to be validated.
//
// The flags are honoured as ProcessBlock honours them.  A block that carries
// BFFastAdd skips transaction validation exactly as it would on the direct
// path; the pipeline is only worth its overhead above the last checkpoint.
func (p *ConnectPipeline) ProcessBlock(block *btcutil.Block,
	flags BehaviorFlags) error {

	b := p.b

	// Only one apply may be outstanding: this block's prepare reads the
	// cache for everything its overlay does not cover, and that read must
	// see every block before the overlay already applied.
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
	t2 := time.Now()
	p.stats.Accept += t2.Sub(t1)
	if err != nil {
		return err
	}
	if isOrphan {
		return ruleError(ErrPreviousBlockUnknown, "pipeline cannot "+
			"connect an orphan block")
	}

	// Prepare off the lock, against the previous block's delta, while
	// that block's scripts run.
	pb := &pipelineBlock{node: node, block: block, flags: flags}
	fastAdd := flags&BFFastAdd == BFFastAdd
	if !fastAdd && !b.index.NodeStatus(node).KnownValid() {
		// A previous block that skipped validation has no delta to
		// overlay; it has to be in the cache before this one is
		// prepared, so apply it now and wait.
		if p.scripted != nil && p.scripted.view == nil {
			prev, err := p.waitScripts()
			if err != nil {
				p.fail()

				return err
			}
			p.startApply(prev)
			if err := p.waitInflight(); err != nil {
				return err
			}
		}

		view := NewUtxoViewpoint()
		if p.scripted != nil {
			view.overlay = p.scripted.view
		}
		view.SetBestHash(&block.MsgBlock().Header.PrevBlock)

		scripts, err := b.prepareConnectBlock(node, block, view, nil)
		p.stats.Prepare += time.Since(t2)
		p.stats.Blocks++
		if err != nil {
			if _, ok := err.(RuleError); ok {
				b.index.SetStatusFlags(node, statusValidateFailed)
			}
			p.fail()

			return err
		}
		pb.view, pb.scripts = view, scripts
	}

	// The previous block's scripts must pass before it is applied; this
	// block's scripts start right after so the validators stay busy while
	// the caller goes back for the next block.
	t3 := time.Now()
	prev, err := p.waitScripts()
	p.stats.WaitScripts += time.Since(t3)
	if err != nil {
		p.fail()

		return err
	}
	if prev != nil {
		p.startApply(prev)
	}
	p.startScripts(pb)

	return nil
}

// fail drains the background stages after an error so that their goroutines
// finish and the index state reaches the database.
func (p *ConnectPipeline) fail() {
	_ = p.waitInflight()
	_, _ = p.waitScripts()
	b := p.b
	b.chainLock.Lock()
	_ = b.index.flushToDB()
	b.chainLock.Unlock()
}

// Stats returns the accumulated timing.  Call it after Flush, or at least
// between calls to ProcessBlock, since the background goroutines update it.
func (p *ConnectPipeline) Stats() PipelineStats {
	return p.stats
}

// Flush finishes the block in the background stages, waits for it, and
// returns the first error.  It must be called before the chain is used for
// anything else.
func (p *ConnectPipeline) Flush() error {
	err := p.waitInflight()
	if err == nil {
		var last *pipelineBlock
		last, err = p.waitScripts()
		if err == nil && last != nil {
			p.startApply(last)
			err = p.waitInflight()
		}
	}
	if err != nil {
		p.fail()

		return err
	}

	// The last block's valid status was set after its index entry was
	// written; make sure it reaches the database.
	b := p.b
	b.chainLock.Lock()
	err = b.index.flushToDB()
	b.chainLock.Unlock()

	return err
}
