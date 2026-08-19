# MDBX Storage Notes

Design notes for moving btcd's chainstate onto MDBX, drawn from a review of
rBTC, N42's Rust full-node kernel (~100k lines, `redb` by default with an
optional `mdbx` backend).

Status as of 2026-08-19: this repository still ships only `ffldb` (flat block
files plus a leveldb metadata store). Nothing here has been implemented yet.
The numbers quoted below are rBTC's measurements on its own storage layer,
not btcd's; they are recorded to show what a decision backed by data looks
like, not as predictions for this codebase.

The concrete design these notes lead to is in `storage_design.md`.

## 1. One write transaction, or nothing

This is the constraint everything else hangs off. rBTC's own MDBX backend
(`src/mdbx_utxo.rs`) is marked experimental, and its header says why:

> This backend is experimental and intended for storage evaluation.
> Production active-chain execution remains on the unified redb chain store
> until MDBX also owns undo and execution-tip metadata in the same
> transaction.

Their production path instead guarantees:

> Active UTXOs, per-block undo, and the execution tip now share one physical
> database and one write transaction; a successful commit exposes all three
> and an aborted commit exposes none.

btcd today splits this across engines: blocks live in flat files, metadata in
leveldb, and the UTXO set is flushed separately (the `Flushing UTXO cache ...
to disk` step at shutdown). Moving only the UTXO set to MDBX would leave the
spend journal and the chain tip in a different engine, so a crash between the
two commits leaves a state neither engine can detect as inconsistent.

Rule to follow: the UTXO set, the per-block undo data, and the chain tip must
commit in a single MDBX write transaction, or the migration is not worth
doing. The acceptance invariant to aim for is rBTC's:

> always an old complete checkpoint or a new complete checkpoint, never a
> mixed UTXO/undo/tip state.

## 2. Hot/cold UTXO tiering

rBTC splits the UTXO set across two tables, `utxo_hot` and `utxo_cold`, keyed
identically (32-byte txid in wire order plus little-endian `vout`). Which
tier a record sits in is storage policy only — it never changes consensus
data, and tier metadata is excluded from snapshot identity.

The boundary was chosen from a full mainnet replay (genesis to height
935,000, 3,257,609,051 spends observed):

| Metric | Value |
| --- | --- |
| Spend-age P50 | 42 blocks |
| Spend-age P90 | 8,299 blocks |
| Spend-age P99 | 122,194 blocks |
| Spend-age P99.9 | 323,668 blocks |
| Selected window | 157,680 blocks (~3 years) |
| Historical spend hits in hot | 99.38% |
| Share of records kept hot | 65.96% |
| Expected hot-first probes per spend | 1.006 |

An independent sample over heights 935,001–959,730 (179,211,528 spends)
confirmed 99.42% hits and P99 of 129,338 blocks.

btcd's `utxocache` is an in-memory layer only; the durable set has no notion
of hot and cold. The tiering strategy transfers directly and is close to free
at 1.006 probes per spend.

## 3. Migrations must be resumable

rBTC's retiering tool (`--retier-utxos-window-blocks BLOCKS`) scans merged
tiers in key order and commits at most 65,536 records per transaction. The
critical detail:

> Its cursor and counters share the same transaction as every move, so a
> restart resumes without a giant transaction or ambiguous partial result.

Their live run moved 68,387,004 of 166,269,013 rows in 1,029 seconds, and a
follow-up scan after 42 new blocks moved only 43,427 newly aged rows.

An `ffldb` to MDBX converter for this repository should take the same shape:
bounded transactions, cursor persisted inside the same transaction as the
data it describes, idempotent restart.

## 4. Checkpoint batching during IBD

Rather than one commit per block, rBTC folds contiguous blocks into a single
outpoint-sorted mutation — 256 blocks by default, tunable up to 1,008 — while
still retaining an addressable undo record per block inside that one
transaction. Outputs created and spent within the same checkpoint never reach
the database at all. Once only a single new tip block is available it is
committed alone.

Script validation is decoupled from this: resolved prevouts become immutable
jobs on a bounded worker pool, submitted in 16-transaction packets under one
lock, with a single checkpoint-wide barrier before commit and an ordered
reduction that reports the earliest failing block and transaction.

## 5. Benchmark before switching engines

rBTC carries `tests/storage_bench.rs`: an opt-in benchmark over a
deterministic generated UTXO workload, comparing redb, MDBX, and SQLite, with
the resulting selection written into `docs/ARCHITECTURE.md` alongside the
reasoning (portability, ordered copy-on-write B-trees, ACID, concurrent
readers).

Before this repository commits to MDBX it should have an equivalent
benchmark demonstrating a win over the current leveldb metadata store on a
representative workload — point lookups plus batched deletes and inserts,
with ordered iteration for snapshots.

## 6. Differential testing against Bitcoin Core

rBTC runs `core_block_differential.rs`, `core_consensus_vectors.rs`,
`core_replacement_differential.rs`, and `core26_historical_*.rs` against a
vendored `bitcoinconsensus`, comparing results transaction by transaction.

btcd has the script and transaction reference vectors under
`txscript/data/`, but nothing that cross-checks a running chain against Core.
A storage rewrite is exactly the kind of change where such a harness pays for
itself.

## 7. Keep cgo at the edge

rBTC sets `unsafe_code = "forbid"` crate-wide and vendors `libmdbx` purely to
add a safe `copy_compact` binding, keeping every `unsafe` inside the
dependency.

MDBX is a C library, so the Go equivalent is to confine cgo to one thin
package implementing the `database.DB` interface, and to keep it out of
`blockchain/` entirely. This also preserves the ability to build without cgo
by falling back to `ffldb`.

## 8. A transferable optimization

rBTC's BIP30 handling matches the approach already in `blockchain/validate.go`
(skip the redundant scan after the authenticated BIP34 anchor, keep both
historical duplicate-coinbase exceptions for exact undo). It then goes one
step further:

> The resulting fresh-output proof lets the tentative overlay avoid a second
> durable lookup for each created outpoint.

That is, once a block is known to be past the BIP34 anchor, every output it
creates is provably fresh, so the overlay can skip the existence check on
insert. This applies to btcd's UTXO viewpoint independently of which storage
engine is underneath.

## Open questions

- Does MDBX's single-writer model interact badly with btcd's concurrent
  index writers (`blockchain/indexers/`), which currently share the leveldb
  transaction?
- Where do the flat block files land? rBTC keeps blocks and chainstate in one
  store; btcd's `ffldb` pruning logic is built around numbered `.fdb` files
  and would need rethinking (see `database/ffldb/blockio.go`).
- Can the existing `utxocache` flush path be re-expressed as the checkpoint
  batching in section 4 without changing its consensus behaviour?
