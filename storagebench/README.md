# storagebench

Phase 0 of the storage migration in `docs/storage_design.md`: a deterministic
UTXO workload run against candidate storage engines, so the decision to move
off leveldb rests on measurements rather than on reputation.

This is a separate Go module. MDBX is a C library, and keeping the evaluation
out of the main module means btcd's build does not acquire cgo just because
someone benchmarked an alternative.

## Running

```sh
cd storagebench
go run . --utxos 1000000 --blocks 1000 --history 900000 --json report.json
```

Requires a C toolchain, because the MDBX binding is cgo. To run without one,
restrict the engines:

```sh
go run . --engines leveldb
```

## What it measures

| Phase | Models |
| --- | --- |
| `prefill` | building an initial chainstate |
| `connect` | block connection: batched inserts and deletes, prevouts resolved through one read view per block |
| `settle` | deferred background work, so an LSM engine's compaction is charged inside the measured window |
| `cold-lookup` | point lookups after a reopen, which is what a restarted node faces |
| `iterate` | ordered full scan, the snapshot export path |

Space is reported two ways. `used` is what the engine actually occupies —
for MDBX that is `last_pgno * page_size`, not the file size, because the map
file is preallocated in growth-step increments and would otherwise make a
nearly empty store look enormous. `allocated` is the directory size.

## Fairness

Three things had to be equalised before the numbers meant anything, and each
is a way an engine comparison can quietly lie:

1. **Durability.** Every engine commits with fsync. Comparing a durable engine
   against a buffered one measures nothing.
2. **Read batching.** Lookups go through one read view per block, because that
   is what block connection does. Charging an engine transaction setup on
   every individual lookup measures an access pattern btcd never produces.
3. **Deferred work.** `settle` forces LSM compaction to finish. Stopping the
   clock at the last commit would charge leveldb for none of the merge work
   its writes created.

## Workload realism

Which outputs get spent is not uniform. Spend targets are drawn from Bitcoin's
long-tailed spend-age distribution, pinned to quantiles from a full mainnet
replay (P50 42 blocks, P90 8,299, P99 122,194, P99.9 323,668) and interpolated
logarithmically so the tail keeps its shape.

This matters for more than realism: uniform spending would understate both
engine cache behaviour and the entire case for hot/cold tiering. The run
reports the realised age distribution and the hit rate a hot tier would have
achieved at each candidate window, so the tiering threshold in the design
document can be re-derived here rather than inherited.

`--history` bounds how old a coin the run can express. A short history caps
the realised ages regardless of what the distribution asks for, which shows up
directly in the reported quantiles — if the P99 is far below 122,194, the run
was too shallow to say anything about the tail.

UTXO records are encoded to match btcd's on-disk sizes: a VLQ header code
packing height and the coinbase flag, a VLQ-transformed amount, and a script
compressed to a class tag plus its 20- or 32-byte payload, mixed across output
types in roughly mainnet proportions. Keys are the fixed 36-byte big-endian
outpoint layout the design document proposes.
