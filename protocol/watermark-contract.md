# The event-time and checkpoint contract

## Input contract

`Input` contains partition, offset, event timestamp, kind, key and bounded integer value. Partition/key names are ASCII identifiers of length 1–32. Partitions are application-configured and cannot be granted by source content. Offsets are integers 0–10^9 and values are within ±10^6. Ordinary event times are 0–10^9; trusted progress markers extend to 10^9+30,000 so even the largest admitted event can be drained under maximum window/lateness/lag settings.

`advance` records share the partition's offset namespace and update progress without contributing a value. Their legacy key/value fields must retain fixed defaults. Source offsets may arrive with gaps or out of numeric order; deduplication uses exact persisted identities, not an unsafe “offset less than maximum” shortcut.

## Watermarks and windows

For partition `p`, persist `H_p = max(previous H_p, input timestamp)`. Initially `H_p=-1`. With lag `d`, the global watermark is `W = min_p max(-1, H_p-d)`. Progress is monotone. A silent partition remains in this minimum; no heuristic idle-partition exclusion can discard its future data.

For event time `t`, all aligned starts `s` with `s >= 0`, `s % slide == 0`, and `s <= t < s+size` are memberships. The implementation enumerates starts backward from the most recent aligned start; the offline oracle independently enumerates forward from the earliest possible aligned start. At most 32 windows may contain one event.

Admission is per membership: accept iff `W < s+size+L`. At equality the window is closed. Initial emission occurs iff `W >= s+size`. If a previously emitted window accepts another contribution, revision `r+1` retracts the exact old count/sum then upserts the new count/sum. A finalization at `W >= end+L` retains the final sink value and evicts active state. Negative/zero sums are valid; count is independent of sum.

## Atomic state machine

Tables persist configuration, source payload fingerprints, partition highs, active windows, current sink values, changelog entries and per-input checkpoints. Configuration is stored canonically; a different policy cannot reinterpret a resumed database.

One input obtains `BEGIN IMMEDIATE`, checks its source identity, updates partition progress, applies admissible memberships, emits eligible results/finalizations, and appends its checkpoint. A deterministic uniqueness constraint covers `(key,start,revision,operation)` in the local changelog. Source identity and every sink effect commit together. Indexed closure/emission scans avoid revisiting all already-emitted retained windows. `apply` returns a compact commit receipt; full state is exported only by an explicit consistent read snapshot.

| Boundary | Durable state | Recovery |
| --- | --- | --- |
| Before commit | Entire input transaction rolls back, including sink revisions and progress | Replay the same source input |
| After commit before caller receives success | Complete input transaction remains | Stable source identity suppresses another application |
| Capacity or source conflict | Current input rolls back | Repair capacity/configuration/source issue; earlier commits remain |
| Replayed same offset and payload | No state or changelog change | Return duplicate status |

A read snapshot uses a read transaction, so its exported tables represent one SQLite snapshot. One `Processor` connection belongs to one worker; separate connections serialize writers through SQLite. Database lock waits have a two-second timeout. There is no distributed lease, partition assignment protocol, remote acknowledgement or external transaction.

## Sink and oracle independence

The sink is an in-database materialized projection. An independent consumer reconstructs it from retraction/upsert/finalization events and deduplicates changelog sequence identities; conflicting identities or unmatched retractions fail explicitly.

The offline window oracle retains individual accepted contributions and recomputes sums/counts from history instead of sharing SQL update logic. It applies the specified watermark/lateness contract in source order. It checks aggregate results and final flags independently; a second consumer checks changelog/revision semantics. These references establish local software correctness for tested streams, not broker semantics.

## Resource and complexity contract

Window size, lag and lateness are each at most 10,000; there are at most eight partitions and 32 memberships/event. Source ledger capacity defaults to 5,000 unique inputs (maximum 20,000), active/sink window capacity to 20,000 each (maximum 100,000), and changelog capacity to 100,000 entries (maximum 200,000). Capacity failures roll back the whole current input. Closed active state is evicted, but durable identities and final sink rows are retained until application-level database replacement; no unsafe compaction is implemented.

For `h` memberships and `f` newly emitted/finalized windows, data updates are approximately `O((h+f) log A)` indexed database operations; bounded capacity counts and SQLite transaction costs add overhead. A snapshot is linear in retained state. The independent oracle intentionally retains full bounded test histories and is not a streaming-memory algorithm. JSONL imports bound each line to 65,536 characters, total input to ten million characters, and batch length to 40,000 records.

The guarantee assumes SQLite and its underlying storage honor configured durability. External consumers need their own idempotency/acknowledgement protocol. No claim about remote exactly-once execution, disk failure recovery, Kafka barriers, production throughput, encryption or multi-host coordination is made.
