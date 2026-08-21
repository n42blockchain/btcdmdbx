# Post-Checkpoint Validation Performance

Research notes on the segment of a mainnet replay above btcd's last
checkpoint (height 810,000 in `chaincfg/params.go`), where every consensus
rule runs including script validation. Below the checkpoint the replay runs
at 300–2,800 blocks/s; above it the measured rate was 11.8–13.7 blocks/s, a
gap of two orders of magnitude that deserved a proper accounting rather than
a shrug at "signatures are expensive".

Machine: 32 logical cores, 125 GB RAM, Windows 11, Go 1.26.5.

## 1. Where the ceiling actually is

Single-core signature verification on this machine, from the btcec
benchmarks:

| Operation | Cost |
| --- | --- |
| ECDSA verify (`btcec/ecdsa`) | 74.5 µs |
| Schnorr verify (`btcec/schnorr`) | 84.3 µs |

Thirty-two cores at 75 µs per verification is roughly 400,000 signature
checks per second. A late-2025 block carries on the order of 4,000 inputs,
and a good fraction of those are multisig inputs that cost more than one
verification, so call it 5,000–8,000 checks per block. That puts the pure
signature-arithmetic ceiling at **50–80 blocks/s**.

The measured 13 blocks/s is therefore **one sixth to one fourth of the
ceiling**. A CPU profile taken during that segment showed 18.5 cores busy
with 80% of samples inside secp256k1 field arithmetic — which looks like
"already at the limit" until the arithmetic is done: 14.8 core-equivalents
of field math at 75 µs per signature is ~200,000 checks/s, yet 13 blocks/s ×
~5,000 checks is ~65,000. Two thirds of the cores' time was going somewhere
other than useful verification, and the profile's flat view hid it.

## 2. Inventory of the serial path

The full-validation connect path in `blockchain` runs these stages for each
block, in this order. Only stage 7 was parallel when this work began.

| # | Stage | Where | Parallel? |
| --- | --- | --- | --- |
| 1 | `checkBlockSanity` — merkle root, tx sanity, PoW | `ProcessBlock` | now in pipeline workers (`BFSanityDone`) |
| 2 | `fetchInputUtxos` — every input's entry copied from the cache into a per-block viewpoint | `checkConnectBlock` | serial |
| 3 | `GetSigOpCost` loop | `checkConnectBlock` | serial, cheap |
| 4 | `CheckTransactionInputs` + `view.connectTransaction` loop — fee/maturity checks, then applying every tx to the viewpoint map | `checkConnectBlock` | serial |
| 5 | BIP0143/BIP0341 sighash midstates for every witness tx | `checkBlockScripts` | **was serial, under an exclusive lock** → now parallel |
| 6 | Building the per-input validation items | `checkBlockScripts` | serial, cheap |
| 7 | Script execution per input | `txValidator.Validate` | parallel (NumCPU×3 goroutines via channels → now NumCPU via atomic counter) |
| 8 | `utxoCache.connectTransactions` — the real state update | `connectBestChain` | now sharded parallel |
| 9 | `connectBlock` — spend journal serialization, block index, MDBX commit | `connectBestChain` | journal encoding now parallel; rest serial |

Stage 4 is worth noting: the block's transactions are applied **twice** —
once into the throwaway viewpoint so that later inputs can see earlier
outputs (stage 4), and again into the real cache (stage 8). The viewpoint
copy is a small per-block map so it is cheap per operation, but it is on the
serial path.

## 3. Costs hiding inside stage 7

Three things were found in the parallel stage itself.

**Dispatch churn.** Every input went through two unbuffered channel
operations — dispatch and result — between one dispatcher goroutine and 96
workers. A block with 8,000 inputs paid 16,000 cross-thread wakeups before
any signature was checked. Profile signature: `runtime.semasleep` 19%,
`runtime.lock2` 21% cumulative. Fixed by handing out work through an atomic
counter; worker count dropped to NumCPU since there is no channel blocking
left to cover.

**Signature cache contention.** `txscript.SigCache` is one map behind one
`sync.RWMutex`, and during sync every signature is new, so every verification
ends in `Add` taking the exclusive lock. Thirty-two goroutines serialise on
that lock once per signature. Fixed by sharding the cache 64 ways on the
leading sighash byte, with a global atomic count so capacity stays exact.

**Allocator contention in sighash computation.** The witness sighash path
allocated a fresh `sha256.New()` digest per signature plus a handful of slice
literals that escape through the `io.Writer` interface; the taproot path went
through `binary.Write`, which reflects and allocates. A single-threaded
benchmark of the witness sighash already showed `runtime.lock2/unlock2` at
20% of CPU — that is the allocator's own locking, and it gets worse with
every additional thread. Fixed with a pooled digest carrying its own scratch
space in `chainhash`, package-level constant slices, and direct field
encoding.

| `BenchmarkCalcWitnessSigHash` (per tx, all inputs) | Before | After |
| --- | --- | --- |
| time | 37.7 µs | 22.6 µs |
| bytes allocated | 18.5 KB | 7.1 KB |

The `hashCache` lock was also held across the entire midstate computation in
`AddSigHashes`, and that call sat on the serial path before dispatch (stage
5). Midstates depend only on the transaction and the read-only viewpoint, so
they are now computed across all CPUs before the items are built; entries a
serving node's mempool already placed in the cache are still reused.

## 4. Compiler-level levers

| Lever | Effect on ECDSA verify | Note |
| --- | --- | --- |
| `GOAMD64=v3` | 89.3 → 82.8 µs (−7%) | free; BMI2/AVX2 baseline |
| `GOAMD64=v4` | 84.6 µs | no gain over v3 |
| PGO (`default.pgo` from a full-validation profile) | measured below | Go 1.26 |

## 5. Measurements

### 5.1 Consecutive windows — and why they were not enough

The first design ran each variant over the next 3,000 blocks above the
checkpoint, resuming from a shared chainstate, with the baseline repeated at
the end to bracket drift. Every run starts with a cold UTXO cache, so the
cold-start penalty is identical; 12 GiB cache, 48 GiB memory limit.

| Window | Variant | Elapsed | blocks/s | Inputs in window | inputs/s |
| --- | --- | --- | --- | --- | --- |
| 810,000–813,000 | base (atomic dispatch) | 6m32s | 7.6 | 23,703,550 | 60,468 |
| 813,000–816,000 | + GOAMD64=v3 | 6m03s | 8.2 | 20,710,695 | 57,054 |
| 816,000–819,000 | + sharded SigCache, parallel zero-alloc sighash | 4m57s | 10.1 | 19,712,826 | 66,373 |
| 819,000–822,000 | base again | 5m50s | 8.6 | 19,925,146 | 56,929 |
| 822,000–825,000 | + PGO | 5m39s | 8.8 | 20,120,534 | 59,353 |
| 825,000–828,000 | shard again | 6m00s | 8.3 | 20,218,661 | 56,163 |

The two baseline windows disagree by 13% in blocks/s; the two shard windows
by 18%. Normalising by input count does not rescue it: the first window has
half the transactions of the third (5.65M vs 11.3M) but more inputs, meaning
large consolidation transactions, and a legacy sighash costs time
proportional to the transaction it is in, so per-input cost itself moves by
a factor comparable to the effects being measured. **Consecutive windows
cannot resolve a 10–25% effect on this workload.** The census tool
(`replayblocks --census-from/--census-to`) exists to make that visible.

What the consecutive windows do establish, because the profile says so
directly rather than the rate: with the sharded cache and parallel sighash in
place, `runtime.semasleep` fell from 19.3% to 8.3% of CPU, `runtime.lock2`
left the top of the profile entirely, and secp256k1 field arithmetic rose to
~82% of samples. The contention is gone; what remains is verification.

### 5.2 Identical window, restored snapshot

The metadata store is snapshotted at 828,000 (106 GB, 36 s to copy) and
mirrored back before every run, so each variant replays exactly blocks
828,001–831,000 from an identical cold start. ffldb reconciles the flat block
files against the restored write cursor on open. The machine is otherwise
idle — one early run that overlapped a compile came in at half speed and was
discarded.

| Variant | Runs (blocks/s) | Mean |
| --- | --- | --- |
| base (atomic dispatch only) | 17.2, 15.9, 12.7ᵖ, 15.3, 17.6 | 15.7 |
| + GOAMD64=v3 | 15.9 | |
| + GOAMD64=v3, replace-built control | 11.6 | |
| + sharded SigCache, parallel zero-alloc sighash | 13.4ᵖ | |
| + PGO | 17.1 | |
| **+ parallel input fetch** | **18.5ᵖ, 19.9** | **19.2** |

ᵖ = run with a 45-second CPU profile captured mid-window.

Two things the table says. First, **the protocol's own noise is ±25%**:
five baseline runs of identical code span 12.7–17.6, because restoring 106
GB through the page cache leaves the block files and the MDBX store in a
different cache state each time. GOAMD64, the sharded cache and PGO are all
inside that band and cannot be resolved by throughput on this machine.
Second, the parallel input fetch is not: both of its runs sit above the
entire baseline range.

### 5.3 Where the wall-clock actually went

Throughput could not separate the variants; the profiles can, because a CPU
profile attributes the connect goroutine's on-CPU time to `ProcessBlock`
while the worker goroutines' time lands on their own stacks. The cumulative
time under `ProcessBlock` divided by the 45-second window is therefore the
fraction of wall time the serial path spent computing.

| 45-second window | base | sharded cache | parallel fetch |
| --- | --- | --- | --- |
| total CPU / busy cores | 595 s / 13.2 | 681 s / 15.1 | 868 s / 19.3 |
| `validateItem` (parallel scripts) | 544 s | 573 s | 725 s |
| **connect goroutine on CPU** (`ProcessBlock` cum) | **24.2 s = 54%** | 17.5 s = 39% | **7.3 s = 16%** |
| of which `fetchUtxosFromCache` | **17.4 s = 39%** | 12.2 s = 27% | — (now parallel) |
| `runtime.semasleep` | — | 50.8 s | 51.9 s |

This is the finding the whole study turned on. Script validation was already
spread across twelve to sixteen cores and finished quickly; the connect
goroutine then spent **two fifths of every second** serially probing the
utxo cache to build the block's viewpoint — one DRAM miss per input, several
thousand inputs per block — before dispatching any signature at all. It is
the same shape of cost that sharding removed from block connection below the
checkpoint, relocated one stage earlier. Issuing those probes from the
configured workers (the shards are independently locked; the database read
path is a transaction per call) cut the serial share from 54% to 16% and put
six more cores to work.

The contention fixes in section 3 did what the profiles said they would —
`semasleep` fell from 19% to 8% and the allocator's locks left the top of
the profile — but they reduced CPU burn, not wall time, because wall time was
never bound there. On a machine with fewer cores it would have been, and the
changes stand on their own; on this one they are hygiene.

## 6. What is left, and the one lever that dwarfs the rest

With the fetch parallelised the connect goroutine is on CPU for a sixth of
wall time and the validators for about sixteen cores' worth, on a 32-core
machine. The gap to full utilisation is structural: every block is a barrier.
Four fan-outs — sighash midstates, input fetch, script checks, cache apply —
each spawn workers, run, and join before the next stage starts, and the
`semasleep` share is those joins. The next step is cross-block pipelining:
fetching and script-checking block N+1 while block N's cache apply and commit
run, which is how the sub-checkpoint path already overlaps its stages. That
is a change to `connectBestChain`'s structure rather than to any one stage,
and it is the right next project.

The lever that dwarfs all of this is policy, not code. The full-validation
segment is 810,000 → tip, ~153,000 blocks, because upstream btcd's
checkpoint list stops at 810,000. Bitcoin Core ships `assumevalid` at roughly
height 905,000 in current releases on exactly the trust model btcd's
checkpoints embody. A checkpoint at 950,000, with the hash taken from a node
that fully validated it, shrinks the segment to ~13,000 blocks — **twenty
times less work than any constant-factor gain can deliver**. That is an
operator trust decision rather than an engineering one, and it is recorded
here rather than made.
