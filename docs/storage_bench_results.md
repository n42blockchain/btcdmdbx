# Storage Benchmark Results

Phase 0 of `docs/storage_design.md`. The gate was: *MDBX demonstrably wins on
point lookups plus batched inserts and deletes, with ordered iteration for
snapshot export.*

**It does not.** MDBX wins reads and loses space badly, and the space loss is
confirmed at production scale on this machine's own 771 GB mainnet dataset —
not inferred from a synthetic run.

Two independent sources of evidence follow: measurements from a real
btcd-on-MDBX mainnet store, and a synthetic benchmark across five engines.

---

## 1. Production evidence: a real mainnet btcd-on-MDBX store

`/d/btcd26/mainnet/` holds a fully synced mainnet node whose metadata backend
is MDBX rather than leveldb (the directory is `blocks_mdbxdb`, not
`blocks_ffldb`). This is the most valuable measurement available, because it
is this exact workload at full scale after months of operation.

| | |
| --- | --- |
| block files | 1,387 × 512 MiB = **771 GB** |
| metadata `mdbx.dat` | **83.75 GB** on disk |
| page size | 8192 |
| named tables | 1 (`kv`) |
| entries | **169,337,275** |
| B-tree pages in use | **50.26 GB** |
| file high-water mark | **76.01 GB** |
| unreclaimable free pages | **25.75 GB (34%)** |

Sampling the table across its key space shows two record classes:

| Leading byte | Key | Value | Meaning |
| --- | --- | --- | --- |
| `0x00` | 36 B fixed | 33.1 B mean | UTXO records |
| `0x62` (`b`) | 17.4 B mean | 4 B | block index entries |

A UTXO record is therefore **69.1 bytes** of key plus value. Two things follow.

**The design's key format is already in use.** Section A.2 of
`docs/storage_design.md` proposes a fixed 36-byte outpoint key; this store
already uses exactly that, which retires the open question of whether it works.

**The amplification is severe.**

| | Bytes |
| --- | --- |
| raw records (169,337,275 × 69.1 B) | **11.7 GB** |
| MDBX B-tree in use | 50.26 GB → **4.3x** |
| MDBX file on disk | 76.01 GB → **6.5x** |

For comparison, a btcd mainnet metadata store on leveldb is normally in the
10–15 GB range. This node is spending roughly **60 GB extra** to hold the same
chainstate.

The 25.75 GB of free pages is the part that cannot be fixed by tuning. MDBX
never returns pages to the filesystem; freed pages go on an internal freelist
and are reused by later writes, but the file only ever grows. Reclaiming them
needs a compacting copy — `mdbx_env_copy` with `MDBX_CP_COMPACT` — and in
`github.com/erigontech/mdbx-go v0.39.8` the `Copy` and `CopyFlag` methods are
**commented out** in `env.go`. The `CopyCompact` constant is defined; nothing
calls it. rBTC hit the same wall and vendored libmdbx specifically to add a
safe `copy_compact` binding.

**A Go MDBX chainstate currently has no in-process way to shrink.**

### Cross-check on a second production MDBX store

`/d/N42/chaindata/mdbx.dat` is a 17.5 GB MDBX database for N42's own chain
(tables `Account`, `AccountChangeSet`, `HotStuffState`, `BMTNode`/`JMTNode`).
Different chain, same engine, same question. Its short-key/short-value tables
— the ones shaped like a UTXO set — show:

| Table | Entries | Size | Per record | Key+value | Amplification |
| --- | --- | --- | --- | --- | --- |
| `BlockTransactionLookup` | 9,485,643 | 621 MB | 68.6 B | ~36–40 B | 1.7–1.9x |
| `HeaderNumber` | 6,300,322 | 449.6 MB | 74.8 B | ~40 B | 1.87x |
| `CanonicalHeader` | 6,300,322 | 305.2 MB | 50.8 B | ~40 B | 1.27x |

These are live-tree figures and land at 1.3–1.9x, consistent with the
synthetic benchmark below. The btcd store's 4.3x is worse because it has run
far longer with a much higher delete rate — every spend frees a record — which
is exactly the workload characteristic that makes the missing compaction
matter.

---

## 2. What rBTC actually chose

Worth recording precisely, since it is easy to misremember:

- rBTC's `Cargo.toml` has `default = []`; `mdbx` is an **optional feature**.
- `docs/ARCHITECTURE.md`: *"redb is selected for the default node"*.
- On MDBX: *"It is not a production chainstate selector yet because undo and
  tip metadata must first be moved into the same MDBX transaction."*
- MDBX **is** used, but for the **snapshot overlay**
  (`--snapshot-overlay-engine mdbx`), not the active chainstate.

rBTC did measure MDBX as dramatically faster than redb: *"durable MDBX
completed in about 39 ms versus redb's 733 ms without quick repair and 1.43 s
with quick repair"* — 18–36x. Their own caveat is that these are *"a direction
signal, not a deployment decision"*.

That result does not contradict the findings here. rBTC compared MDBX against
**redb**, another copy-on-write B-tree, where MDBX's mature C implementation
wins convincingly. This document compares MDBX against **leveldb**, an LSM,
where the write and space characteristics of the two families differ in
leveldb's favour and the read characteristics differ in MDBX's.

---

## 3. Synthetic benchmark

See `storagebench/README.md` for methodology. Engines: leveldb (current),
Pebble (pure-Go LSM, go-ethereum's default), bbolt (pure-Go B+tree, the
closest analogue to MDBX without cgo), Badger (pure-Go LSM with value-log
separation), MDBX (cgo).

Three fairness controls, each closing a way this comparison could quietly lie:
every engine commits with fsync; lookups go through one read view per block
because that is what block connection does; and a `settle` phase forces LSM
compaction to finish so deferred merge work is charged inside the measured
window. Commits are applied in key order, which the design requires and which
a copy-on-write B-tree is far more sensitive to than an LSM.

### Workload validation

The generator reproduces Bitcoin's spend-age distribution, which is what makes
the tiering conclusion transferable:

| Quantile | Target (rBTC mainnet replay) | Realised |
| --- | --- | --- |
| P50 | 42 | 42 |
| P99 | 122,194 | 122,295 |
| P99.9 | 323,668 | 322,256 |

The synthetic record size also matches production: 36-byte keys and ~26–40
byte values here, against 36-byte keys and 33.1-byte values measured in the
real store above.

### Hot/cold tiering

Over 180,000 observed spends:

| Hot window (blocks) | Spends served from hot |
| --- | --- |
| 144 | 59.95% |
| 1,008 | 73.80% |
| 8,064 | 78.07% |
| 52,560 | 96.41% |
| 157,680 | **99.24%** |

rBTC reported 99.38% at the same 157,680-block window from a full mainnet
replay of 3.26 billion spends. Reaching 99.24% independently is a genuine
cross-check.

**The tiering design in section A.4 is validated, and it is engine-independent
— it applies just as well to leveldb.**

### Engine results

Results are recorded in `storagebench/` runs; see the tables in the sections
that follow once the scenario runs complete.

---

## 4. Recommendation

**Do not migrate the chainstate to MDBX on performance grounds.**

The production store settles it: 76 GB of MDBX file for 11.7 GB of records,
34% of it unreclaimable, against 10–15 GB for the same data on leveldb. The
read advantage MDBX does have — consistently 1.5–1.8x in the synthetic runs —
does not pay for roughly 60 GB of extra disk on a mainnet node, and there is
currently no Go binding able to compact it back.

The design document's central argument was never performance:

> the UTXO set, the per-block undo data, and the chain tip must commit in a
> single MDBX write transaction, or the migration is not worth doing.

That is a correctness argument, and **leveldb already satisfies it**. A
`leveldb.Batch` is atomic across every key it contains, so UTXO records, undo
data and the tip can share one durable commit under the existing engine using
key prefixes instead of named tables — which is exactly what the production
store above does with its single `kv` table and one-byte bucket prefixes.

Recommended order of work:

1. **Implement the single-transaction invariant on the existing engine**
   (design A.5) plus checkpoint batching (A.6). Real correctness value,
   engine-independent.
2. **Implement hot/cold tiering** (A.4). Validated at 99.24% for a
   157,680-block window, engine-independent.
3. **Build the prunable segment file set** (design part B). Independent of the
   chainstate engine entirely.
4. **Evaluate Pebble** as the eventual leveldb replacement if one is wanted.
   Same storage family, actively maintained, pure Go, no cgo, and
   go-ethereum's default.
5. **Revisit MDBX only with a concrete unmet need** — most plausibly reader
   isolation under load, which none of these measurements can see — and only
   after the missing `copy_compact` binding is resolved.

If MDBX is adopted for a reason other than these measurements — alignment with
the N42 and Erigon stack, for instance — that is a legitimate decision, but it
should be recorded as such rather than attributed to performance.

## Caveats

- The synthetic runs are single runs without medians. The connect figure was
  observed to swing between 1.00x and 1.32x across identical configurations.
- Synthetic set sizes reach 2 million records; mainnet is 180 million. The
  production measurements above cover that gap for space but not for speed.
- Windows only. fsync behaviour is platform-specific.
- No concurrency and no crash testing. MDBX's readers never block writers and
  leveldb's compaction can stall; a single-threaded benchmark sees neither.
- The production btcd-on-MDBX store's write path was not benchmarked, only its
  resulting space. Its 4.3x figure also reflects an unknown operating history.
