# Storage Engine Findings

Notes from replacing btcd's metadata store and measuring the result against
real mainnet data. Written to be useful to another full-node implementation
facing the same choices, so the reasoning is included rather than just the
conclusions.

Measurements taken 2026-08-19 on one machine: 32 logical cores, 125 GB RAM,
Windows 11. Mainnet data replayed from an existing node's 771 GB of flat block
files.

---

## 1. The largest single finding had nothing to do with the engine

A missing UTXO cache cost a **factor of fourteen**, and it looked like an
engine problem in the profile.

| Configuration | 200,000 blocks | Rate |
| --- | --- | --- |
| UTXO cache unset (0) | 8m10s | 407 blocks/s |
| UTXO cache 8 GiB | **35s** | **5,699 blocks/s** |

The profile before the fix showed no single hot spot — it was spread across
immutable-tree lookups, copy-on-write clones, hash map probing and write
syscalls. Every one of those is what an absent cache *produces*, and none of
them names the cause. It would have been easy to read that profile as "the
storage engine is slow" and start replacing it.

**Transferable point:** before comparing engines, verify the cache in front of
the engine is actually configured. A cache miss ratio near 1.0 makes every
engine look equally bad, and the profile will not say so.

## 2. Measure the storage layer, not signature verification

Replaying with full validation produced this curve:

| Height | Rate |
| --- | --- |
| 131,415 | 2,190 blocks/s |
| 263,430 | 35.8 blocks/s |
| 387,080 | **3.8 blocks/s** |

A 576x collapse — while the store was writing under 1 MB/s and was nowhere
near saturated. The cost was ECDSA, not storage. Projected completion: 3–7
days for a curve shaped entirely by signature verification.

Below the last checkpoint (mainnet height 900,000) a node skips those checks
anyway, so replaying that way is both faster and more faithful. In btcd the
skipped function is `checkConnectBlock`; critically, it validates against a
*throwaway* viewpoint, while the UTXO set update and the block write happen
outside that branch. **The bytes reaching storage are identical either way**,
which is what makes the measurement valid.

**Transferable point:** if the equivalent split does not exist in your
implementation, check that skipping validation does not also skip state
construction, or the space numbers will be wrong.

## 3. Parallelism only helped after the cache was fixed

With the cache absent, reading, checksumming and deserializing were 0.9% of
runtime. Parallelising them was worthless. After the cache fix they were
**11%** of a run 14x shorter, and worth hiding.

The pipeline that hides them: one reader goroutine frames records in order
(they are variable length), parse workers fan out, and results are reordered
before the connect loop. Connecting stays strictly serial — a transaction can
spend an output created earlier in the same block. Pipeline wait dropped from
11% to **0.8%**.

**Transferable point:** the ordering constraint is only on the state
transition. Framing and deserialization depend on nothing but the bytes and
can leave the critical path entirely.

## 4. Engine comparison

Deterministic UTXO workload, mainnet block rates (5,000 outputs created /
4,700 spent per block), spend targets drawn from Bitcoin's real spend-age
distribution rather than uniformly. Every engine commits with fsync, lookups
go through one read view per block, and LSM compaction is forced to complete
inside the measured window.

**Serving node** (one commit per block, two prevout lookups per spend):

| Engine | prefill | connect | cold lookup | B/utxo | disk |
| --- | --- | --- | --- | --- | --- |
| leveldb | 173,023 | 67,842 | 11,574 | 77.6 | 152 MB |
| pebble | 228,377 | 36,650 | 10,926 | 77.3 | 152 MB |
| bbolt | 19,135 | 22,735 | 11,537 | 85.5 | 512 MB |
| badger | **492,325** | **69,896** | 11,204 | 94.6 | 187 MB |
| mdbx | 20,988 | 24,315 | 11,652 | 126.1 | 288 MB |

**Initial sync** (256 blocks per commit, one lookup per spend):

| Engine | prefill | connect | cold lookup | B/utxo | disk |
| --- | --- | --- | --- | --- | --- |
| leveldb | 233,945 | 30,663 | 3,457 | 77.4 | 159 MB |
| pebble | **744,477** | 32,082 | 3,404 | 77.2 | 159 MB |
| badger | 349,814 | 29,210 | 3,682 | 103.6 | 214 MB |
| mdbx | 652,255 | **32,988** | 3,520 | 197.1 | **800 MB** |

bbolt is absent from the second table: at 1.28 million records per
transaction it did not finish a single commit in twenty minutes. It cannot
take checkpoint-sized batches.

Three results worth carrying over:

**Reads stop being an engine decision at scale.** At two million records every
engine lands within 8% of every other on cold lookups — they are all waiting
on the same disk. An earlier one-million-record run showed MDBX 1.79x ahead on
reads; that advantage was the working set fitting in page cache and it does
not survive. Larger sets flatten the engines further, not separate them.

**Batch size dominates B-tree write throughput, and it is paid for in space.**
MDBX went from 20,988 to 652,255 inserts/s between the two scenarios — 31x —
confirming that sorted checkpoint batching is essential for a copy-on-write
B-tree. But the same batching took its footprint from 288 MB to **800 MB**
against a flat 159 MB for the LSM engines.

**Pebble matched leveldb's space while tripling bulk insert.** 77.2 vs 77.4
bytes per UTXO, 744k vs 234k inserts/s, pure Go, no cgo.

## 5. MDBX at production scale

The most valuable measurement was not synthetic. A fully synced mainnet node
on this machine uses MDBX for its chainstate:

| | |
| --- | --- |
| entries | 169,337,275 |
| record size (36-byte key + value) | 69.1 bytes |
| **raw data** | **11.7 GB** |
| B-tree pages in use | 50.26 GB → **4.3x** |
| file on disk | 76.01 GB → **6.5x** |
| unreclaimable free pages | 25.75 GB (**34%**) |

The same data on an LSM store is normally 10–15 GB.

The 34% is the part that cannot be tuned away. MDBX never returns pages to the
filesystem; freed pages go on a freelist and are reused, but the file only
grows. Reclaiming needs `mdbx_env_copy` with `MDBX_CP_COMPACT` — and in
`github.com/erigontech/mdbx-go v0.39.8` the `Copy` and `CopyFlag` methods are
**commented out**. The `CopyCompact` constant exists; nothing calls it.

A UTXO set is the worst case for this: every spend frees a record.

A second MDBX database on this machine (a different chain, 17.5 GB) shows
1.3–1.9x on its short-key tables — much better, and consistent with the
synthetic numbers. The 4.3x reflects a long-running store with a high delete
rate, which is exactly what a chainstate is.

## 6. Where this leaves the choice

For a Go implementation the answer was Pebble: same storage family as leveldb,
same space, 3x bulk insert, pure Go. The migration also surfaced a real
resource leak that the previous engine tolerated — cursor iterators released
only by a GC finalizer — because Pebble refuses to close a snapshot with an
open iterator.

For rBTC the situation differs in one important way: redb and MDBX are both
copy-on-write B-trees, so the comparison there is *within* the family, and
MDBX's mature C implementation winning by 18–36x against redb is entirely
consistent with everything above. None of these measurements contradict that.
What they add is the other axis:

- The B-tree family's space penalty against an LSM is real and, at production
  scale with a high delete rate, large (4.3x live, 6.5x on disk).
- That penalty grows with batch size — the same knob that makes B-tree writes
  fast.
- Without a working compaction path there is no way to give the space back.

If MDBX is adopted for the chainstate, the `copy_compact` binding is not
optional, and an operational plan for when to run it belongs in the design
rather than being discovered later. Keeping MDBX for the snapshot overlay,
where records are written once and not repeatedly freed, avoids the worst of
this entirely.

## 7. Reproducing

The benchmark is `storagebench/` (its own module; `--engines leveldb,pebble,
bbolt,badger,mdbx`). The replay tool is `cmd/replayblocks`, which rebuilds a
chainstate from an existing node's block files and reports rate, metadata
growth and final size. Both are in this repository.
