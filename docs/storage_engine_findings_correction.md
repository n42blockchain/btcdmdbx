# Storage Engine Findings — Correction and Final Results

**Supersedes the figures in `storage_engine_findings.md` dated 2026-08-19.**
Two of the numbers in that note were wrong, and the error ran in MDBX's
disfavour. This note carries the corrections, the completed mainnet replay,
and the decision that followed.

Same machine throughout: 32 logical cores, 125 GB RAM, Windows 11, replaying
an existing node's 771 GB mainnet block corpus (1,387 block files, 951,225
blocks).

---

## Correction 1: the raw-data figure was sampled, and the sample was biased

The earlier note reported a production MDBX chainstate holding **11.7 GB of raw
records** in 50.26 GB of live B-tree pages, and concluded a **4.3x** live
amplification and **6.5x** on disk.

That 11.7 GB came from multiplying a sampled mean record size (69.1 bytes)
by the entry count. The sample was drawn by seeking to positions across the
key space, and it landed almost entirely in the UTXO region — small records —
while missing the undo journal, whose records run 16 KB to 256 KB each.

A full scan of the same store gives:

| | Sampled estimate | **Full scan** |
| --- | --- | --- |
| entries | 169,337,275 | 169,337,275 |
| key bytes | — | 5.82 GB |
| value bytes | — | 27.59 GB |
| **raw total** | **11.7 GB** | **33.42 GB** |

| Value size | Entries | Raw GB |
| --- | --- | --- |
| <64B | 165,623,043 | 12.54 |
| 64B–256B | 3,573,395 | 0.46 |
| 1K–16K | 1,949 | 0.02 |
| **16K–256K** | **135,504** | **19.50** |
| >256K | 3,312 | 0.90 |

58% of the raw bytes sit in 0.08% of the records. Any sample that misses them
understates the total by a factor of three.

Corrected amplification for that store:

| | Reported | **Actual** |
| --- | --- | --- |
| live B-tree (50.26 GB) | 4.3x | **1.50x** |
| on disk (76.01 GB) | 6.5x | **2.27x** |

## Correction 2: that store is not running stock btcd

The more consequential error. In stock btcd, `dbRemoveSpendJournalEntry` is
called **only when a block is disconnected** — a reorg. A node in normal
operation **retains every spend journal entry it has ever written**.

Replaying the identical 1,387 block files through stock btcd produces:

| | The production store | **Stock btcd** |
| --- | --- | --- |
| entries | 169,337,275 | 170,147,271 |
| key bytes | 5.82 GB | 5.85 GB |
| **value bytes** | **27.59 GB** | **100.23 GB** |
| undo records (>16K) | 138,816 | **670,893** |

Same chain, same block count, near-identical key bytes and entry counts — and
**3.6x the value bytes**. The production store has been modified to prune old
undo journals; stock btcd keeps roughly one per block for all 951,225 blocks.

The two stores therefore cannot be compared on size. They do not hold the same
data.

Amplification measured on each store's own contents:

| | Raw | Live B-tree | Amplification | Free pages |
| --- | --- | --- | --- | --- |
| Production store (pruned undo) | 33.42 GB | 50.26 GB | 1.50x | 25.75 GB (34%) |
| **Stock btcd on MDBX** | **106.08 GB** | **122.52 GB** | **1.155x** | **≈ 0** |

**MDBX's amplification on a complete btcd chainstate is 1.155x.** The
"long-lived B-tree delete churn" conclusion drawn from the 4.3x figure does not
survive: 4.3x was an artifact of comparing a full store's page count against a
third of its actual contents.

---

## Completed mainnet replay

Both engines replaying the same corpus, fast-add below the last checkpoint
(mainnet height 900,000), 24 GiB UTXO cache, pipelined reads.

| | Pebble | **MDBX** |
| --- | --- | --- |
| 200,000-block replay | 35s, 5,776 blocks/s | **35s, 5,653 blocks/s** |
| Full replay | **crashed at height 828,851** | **completed, 951,225 blocks** |
| metadata at ~830,000 | 68.18 GB | 72 GB |
| **chain state load** | **> 1 hour, never finished** | **728 ms** |

Two results decided this.

**Write throughput is a tie.** 35 seconds either way over 200,000 blocks.

**Startup load is not close.** Loading the chain state — which every node does
on every start — took MDBX 728 ms for a 200,000-block store. Pebble had not
finished loading an 828,851-block store after an hour, with the process reading
100 MB/s the entire time and never printing its first progress line.

The cause is structural, not a defect in either engine or in the adapter. The
same merged-iterator and cache-iterator code serves both. Pebble's store at
that point held:

```
L5  10,746 files   14.10 GB
L6  45,641 files   78.44 GB
```

**56,000 SST files.** Block index loading performs hundreds of thousands of
seeks, each traversing that file count across levels. MDBX is a single
memory-mapped B-tree: O(log n) to the leaf, no file fan-out. This is the LSM
versus B-tree trade-off appearing in the access pattern that happens to matter
most at startup.

Pebble's crash was a separate matter — a Go 1.26.5 Green Tea GC access
violation under a 73 GB working set (`mgcmark_greenteagc.go`), not a Pebble
defect. It is noted only because MDBX completed the same run without incident.

---

## Decision

**btcd's metadata store is now MDBX. Pebble is removed from the module.**

The reasoning is worth stating precisely, because it is not the reasoning the
first note implied:

- **Not space.** On identical data Pebble is marginally *smaller* (68.18 vs
  72 GB at the same height). The earlier claim that MDBX carries a large space
  penalty was based on the two errors corrected above.
- **Not write throughput.** A tie.
- **Startup load**, by three to four orders of magnitude.
- **Completing the run at all**, though that particular crash was the Go
  runtime's fault rather than Pebble's.

One engine-specific hazard was found and is worth passing on. MDBX tracks
write-transaction ownership in thread-local state, so a Go program **must**
hold the OS thread for the duration of a write transaction. Opening one with a
bare `BeginTxn` and letting the scheduler migrate the goroutine does not fail —
it deadlocks silently. The symptom was twenty minutes of zero I/O after
twenty-five seconds of CPU. Routing writes through `Update`, which locks the
thread, fixes it. Read transactions are exempt when the environment is opened
with `NoTLS`.

## What still stands from the first note

- The **UTXO cache** finding: leaving it unconfigured cost 14x (407 → 5,699
  blocks/s over 200,000 blocks) and produced a profile with no single hot spot.
  Unchanged and engine-independent.
- **Reads are not an engine decision at scale**: all five engines within 8% on
  cold lookups at 2M records, all disk-bound.
- **Measure storage, not ECDSA**: full validation collapsed from 2,190 to under
  4 blocks/s by height 387,000 while the store wrote under 1 MB/s.
- The **synthetic benchmark** results in `storagebench/`, which measured
  engines directly and were never affected by either error.

## What this means for a B-tree chainstate

The corrected numbers are more favourable to the B-tree family than the
original note suggested:

- 1.155x amplification on a complete chainstate, with essentially no free-page
  overhead, in a store built by a single sequential replay.
- The 34% free-page figure in the production store reflects a long-running node
  with a high delete rate. It is real, and it is why a compaction path matters
  — but it is not 4.3x of anything.
- Where the B-tree wins outright is any operation that walks the index. That
  covers startup, and it does not get better for an LSM as the store grows: it
  gets worse, because the file count grows with it.
