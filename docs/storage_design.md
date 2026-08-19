# Storage Design: MDBX Chainstate and a Prunable Static Block Set

Proposed storage architecture for this fork. Two halves, split by mutability:

- **Mutable hot state** — the UTXO set, per-block undo data, and the chain tip
  — moves to MDBX in a single write transaction.
- **Append-only data** — serialized blocks — moves to an immutable, numbered
  set of compressed segment files with an explicit pruning protocol.

Reference implementation reviewed: rBTC, N42's Rust full-node kernel
(`src/utxo.rs`, `src/mdbx_utxo.rs`, `src/archive.rs`, `src/ledger.rs`,
`docs/ARCHITECTURE.md`). See `docs/mdbx_storage_notes.md` for the general
lessons; this document is the concrete design.

Status: proposal. Nothing below is implemented.

---

## Part A — UTXO set on MDBX

### A.1 Keep btcd's record encoding, do not copy rBTC's

This is the first and most counter-intuitive conclusion of the review.

rBTC's UTXO record (`src/utxo.rs`) is a flat, fixed-offset layout: 29 bytes of
header followed by the raw `scriptPubKey`.

```
value_sats   u64 LE     8
height       u32 LE     4
is_coinbase  u8         1
last_touched u64 LE     8
creation_mtp u32 LE     4
script_len   u32 LE     4
script_pubkey          var  (raw wire bytes, uncompressed)
```

btcd already stores the Bitcoin Core encoding (`blockchain/chainio.go`,
`serializeUtxoEntry`): a VLQ header code packing `height << 1 | coinbase`,
followed by a compressed txout — VLQ-transformed amount plus a script
compressed to a 1-byte class tag and 20 or 32 payload bytes for the standard
templates.

For a typical P2PKH output:

| | rBTC | btcd |
| --- | --- | --- |
| header | 25 B fixed | 1–4 B VLQ |
| amount | in header | 1–4 B VLQ |
| script | 4 B len + 25 B raw | 1 B tag + 20 B hash |
| **total** | **~54 B** | **~26 B** |

btcd's encoding is roughly half the size. The two extra fields rBTC carries do
not change this conclusion:

- `creation_mtp` exists for BIP68 evaluation. btcd resolves median-time-past
  from the block index instead and does not need it in the record.
- `last_touched` is a wall-clock timestamp for tiering, and rBTC's own
  architecture document already retires it: *"The legacy in-process aging
  interface uses a 60-day wall-clock window, but it is not a production
  boundary … The operational replacement uses complete-replay consensus coin
  age in blocks."* Coin age in blocks is `tip_height - creation_height`, and
  the creation height is already in btcd's header code.

**Decision: the on-disk UTXO record format does not change.** What is borrowed
from rBTC is the *tiering* and the *transaction discipline*, not the encoding.
This also keeps the migration a pure key-space move rather than a re-encode of
every record.

### A.2 Key format

btcd currently builds outpoint keys as `txid || VLQ(vout)` (`outpointKey` in
`blockchain/chainio.go`) — a variable-width key whose ordering is only
incidentally correct.

Proposed: a fixed 36-byte key, `txid` in wire order followed by `vout` as
**4-byte big-endian**.

```
| 32 bytes txid (wire order) | 4 bytes vout (BE) |
```

Big-endian rather than rBTC's little-endian `vout`, because BE makes the key
lexicographically ordered by outpoint. That gives three things MDBX can
exploit: all outputs of one transaction are contiguous, a whole transaction's
outputs can be dropped with one cursor range delete, and snapshot iteration is
in a canonical order without a sort step.

This is a disk format change and needs the migration in section C.

### A.3 Tables

```
utxo_hot   36-byte key -> serializeUtxoEntry output
utxo_cold  36-byte key -> serializeUtxoEntry output
undo       4-byte height (BE) -> serialized spend journal
meta       fixed keys: schema version, tip hash, tip height,
                       retier cursor, tier counters
```

`utxo_hot` and `utxo_cold` hold identical record formats. Which tier a record
lives in is storage policy only: it never affects consensus, and tier
membership must be excluded from any UTXO set hash or snapshot identity.

### A.4 Tiering policy

Split on consensus coin age in blocks, not wall-clock time:

```
age = tip_height - creation_height
age <  threshold  -> utxo_hot
age >= threshold  -> utxo_cold
```

rBTC's mainnet replay to height 935,000 (3,257,609,051 spends observed) gives
the shape of the curve: P50 spend age 42 blocks, P90 8,299, P99 122,194,
P99.9 323,668. Their selected threshold of 157,680 blocks (~3 years) yielded
99.38% of spends hit in hot while keeping only 65.96% of records there, at
1.006 expected hot-first probes per spend.

Those numbers describe rBTC's store, not this one, and the threshold must be
re-derived here before it is trusted. But the shape — a long-tailed spend-age
distribution where a three-year window captures over 99% of spends — is a
property of Bitcoin, not of the implementation, so the design should assume a
tiering win is available and measure the exact boundary later.

Read path: probe `utxo_hot`, fall back to `utxo_cold`. Write path: every newly
created output goes to `utxo_hot` unconditionally. Nothing moves tiers during
block connection — retiering is strictly an offline operation.

### A.5 The single-transaction invariant

**Every block connection commits `utxo_hot`, `utxo_cold`, `undo`, and the tip
in `meta` inside one MDBX write transaction.**

This is the reason rBTC's own MDBX backend is still marked experimental — its
header states production stays on redb *"until MDBX also owns undo and
execution-tip metadata in the same transaction."* Splitting them across
engines produces a crash state that neither engine can detect as inconsistent,
which is strictly worse than the performance problem the migration is trying
to solve.

Acceptance invariant, adopted verbatim from rBTC: *always an old complete
checkpoint or a new complete checkpoint, never a mixed UTXO/undo/tip state.*

### A.6 Checkpoint batching during IBD

During initial block download, fold N contiguous blocks into one MDBX
transaction (default 256, tunable to 1,008) rather than committing per block:

- Outputs created and spent inside the same checkpoint never touch MDBX.
- Per-block undo records remain individually addressable inside that one
  transaction, so serving and reorg handling are unaffected.
- Mutations are applied in sorted key order to keep B-tree page splits down.
- Once the node is at the tip and only one new block is available, it commits
  alone.

This maps onto btcd's existing `utxocache` flush point rather than replacing
it: the cache becomes the checkpoint accumulator, and its flush becomes the
checkpoint commit that now also carries undo and tip.

---

## Part B — Blocks as a prunable static file set

### B.1 What btcd has today

`database/ffldb` already stores blocks in numbered flat files (`%09d.fdb`,
512 MiB each) with leveldb metadata, and already reclaims space
metadata-first. rBTC's architecture document explicitly credits this shape:
its ledger follows *"the same metadata-first physical-reclamation invariant
used by btcd's flat-file store and geth/N42 freezer tables."*

So this half of the design is an evolution of ffldb, not a replacement. Three
things are added: compression, per-segment authentication, and an explicit
pruning protocol with restart-safe recovery.

### B.2 Segment container format

One segment covers a fixed height range. Layout, modelled on rBTC's
`archive.rs`:

```
magic     8 bytes    e.g. "BTCDBLK1"
version   u16
manifest  length-prefixed, strictly decoded
frames    zstd frames over the length-prefixed block stream
```

Manifest fields:

```
format_version  u16
first_height    u32
block_count     u32
records_bytes   u64      exact uncompressed stream length
records_sha256  32 B     digest of the UNCOMPRESSED stream
piece_size      u32      fixed, 4 MiB
piece_sha256    [32 B]   digest of each COMPRESSED transfer piece
```

The split of digests is the important part and worth stating plainly:

- **Identity is the uncompressed digest.** A different zstd version, level, or
  thread count produces different compressed bytes but the same segment
  identity. Compression stays an implementation detail.
- **Transfer integrity is the compressed piece digests.** A downloaded segment
  is verified piece by piece before anything is decompressed, so a corrupt or
  hostile peer cannot feed the decompressor.

Bounds enforced on import, all of them fail-closed:

| Bound | Value |
| --- | --- |
| decompression ceiling | 1 GiB per segment |
| serialized block ceiling | 4,000,000 bytes |
| max blocks per segment | 100,000 |
| piece size | 4 MiB fixed |

Compression level 1. Segment encoding sits on the IBD hot path and retention
is byte-bounded, so level 9 buys compression ratio on data that will rotate
out anyway. rBTC reached the same conclusion for the same reason.

### B.3 Retention policy

Two ceilings, both enforced, whichever binds first:

```
max_blocks  default 1008     (~1 week at 10 min/block)
max_bytes   default 1 GiB
```

The policy is persisted as a versioned, owner-only, strictly decoded
`ledger-policy.json`, published atomically. A future schema version must fail
closed rather than be rewritten. An interrupted publication must reopen to
either the prior complete policy or the new complete policy — never a blend.

Independently of the ceilings, **at least 288 blocks below the tip are always
retained** as reorg headroom.

### B.4 Pruning protocol

Physical reclamation is metadata-first and restart-safe. Order is not
negotiable:

1. Compute the retained set from both ceilings.
2. Write a versioned pruning intent; fsync it.
3. Atomically publish the reduced live index; fsync.
4. Unlink every segment file absent from the published index.
5. fsync the directory.
6. Remove the intent.

Restart recovery, by state:

| State found | Action |
| --- | --- |
| intent present, index not yet published | repeat the index transition |
| index published, files still present | finish cleanup, then remove intent |
| index published, intent gone | nothing to do |
| a contiguous segment renamed but not indexed | adopt it if provably contiguous, else unlink |

Two rules that fall out of this and must be enforced in code:

- **Never delete from wall-clock age alone.** Deletion follows the index, and
  the index follows the retention computation. A file's mtime is not an input.
- **A published index can never re-adopt the prefix it just dropped.** Once
  the reduced index is durable, the old prefix is gone even if the files are
  still on disk.

### B.5 Reorg truncation

A reorg that crosses a segment boundary durably records the truncation
boundary *before* deleting newer segments or atomically rewriting the crossing
segment. A restart mid-operation repeats it safely, because the recorded
boundary is the authority, not the file set.

### B.6 Downloaded segments

When a segment arrives from a peer or an archive mirror and its entire
validated prefix commits, verify its compressed piece hashes and atomically
rename the file into the ring. Do not decompress and re-encode it — that
wastes CPU and produces a byte-different file with the same identity, which
makes debugging harder. Only the partial-prefix case takes the full
decode/re-encode path.

### B.7 Offline tooling

Keep verification strictly separate from the node's open/recovery path, and
keep read-only plans separate from mutating applies:

| Command | Lock | Opens DB | Effect |
| --- | --- | --- | --- |
| `--verify-storage` | shared | no | audits every segment container |
| `--verify-chain` | exclusive | yes | cross-checks headers/chainstate/segments |
| prune plan | shared | no | emits a plan plus a SHA-256 token |
| prune apply | exclusive | yes | refuses a stale token, then mutates |

The plan/apply token split is worth copying: the plan hashes all its inputs
and exact outputs, and apply refuses to run if the token no longer matches
current state. It turns "the world changed between plan and apply" from a
silent corruption into a clean refusal.

Segment audit runs in two sequential passes per file — first the 4 MiB
compressed piece hashes, then a 64 KiB streaming decompressor checking exact
record length, SHA-256, block framing, and count — under explicit segment and
byte budgets, so an audit cannot turn into an unbounded directory walk. Its
output must distinguish "verified complete" from "budget exhausted".

---

## Part C — Migration

### C.1 Root manifest first

Before any of this, add an owner-only root manifest in the data directory that
binds one Bitcoin network and assigns an explicit version to every persistent
subsystem. Existing directories are treated as legacy v0, fully preflighted
under the current rules, and only then atomically published as v1.

Once present it is validated before any mutable database open. This is what
stops an older binary from silently opening — or worse, downgrading — newer
state, and it is cheap to add now and expensive to retrofit later.

### C.2 Phases

| Phase | Deliverable | Gate to proceed |
| --- | --- | --- |
| 0 | Storage benchmark over a deterministic UTXO workload, MDBX vs leveldb | MDBX demonstrably wins on point lookups + batched writes |
| 1 | MDBX driver behind `database.DB`, cgo confined to one package | full `database` interface test suite passes |
| 2 | UTXO + undo + tip in one write transaction | crash-injection tests at every commit point |
| 3 | hot/cold tiering + resumable retier tool | threshold derived from this codebase's own replay |
| 4 | segment container + pruning protocol | fault injection at all fsync points |
| 5 | piece-addressed archive distribution | — |

Phase 0 is not optional. rBTC published its selection reasoning with numbers;
this fork should not switch engines on the strength of a name.

### C.3 Key-space migration

The 36-byte BE key from A.2 is a format break. The converter should follow
rBTC's retiering tool shape:

- bounded transactions, at most 65,536 records each;
- the read cursor and progress counters persisted **inside the same
  transaction** as the records they describe, so a restart resumes exactly
  where it stopped;
- idempotent: re-running after a completed pass is a no-op, and starting from
  a different tip safely begins a fresh scan.

rBTC's equivalent run moved 68,387,004 of 166,269,013 rows in 1,029 seconds,
which is the right order of magnitude to plan for on mainnet.

---

## Open risks

- **MDBX is single-writer.** btcd's index writers (`blockchain/indexers/`)
  currently share the leveldb transaction. Either they join the same MDBX
  write transaction — which serializes them behind block connection — or they
  move to their own environment and lose atomicity with the chainstate. This
  needs deciding before phase 1, not during it.
- **cgo.** MDBX is a C library. Confining it to one package keeps
  `blockchain/` clean and preserves a cgo-free build falling back to ffldb,
  but cross-compilation and the `CGO_ENABLED=0` path both need explicit CI
  coverage.
- **Map size.** MDBX requires an explicit map size that cannot shrink and
  costs a reopen to grow. Pick a mainnet-safe ceiling and a documented growth
  procedure up front.
- **Two pruning notions.** Section B prunes block bytes; the existing
  `--prune` flag prunes ffldb files. They must not both be live at once, and
  the transition needs a decision on what an existing pruned database means
  under the new layout.
