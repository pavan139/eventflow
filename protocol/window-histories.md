# Recorded window histories

```bash
python -m eventflow demo --output traces/demo.json
python -m eventflow evaluate --output traces/evaluation.json
```

The executed evaluation passed **30/30** scenarios: 18 combinations of slides 10/5/3, allowed lateness 0/5, and no crash/pre-commit/post-commit crash, plus 12 seeded out-of-order streams with independent event times, two keys, signed values and source replay. The seeded cases use size 12, slide 4 and lateness 7. Crashes target partition `left`, offset 2, including an emitted-window late correction in the fixed source fixture.

Every scenario reopens a real temporary SQLite database, finishes the source, replays the entire source, compares sink count/sum/final flags to the independent contribution-history oracle and reconstructs sink revisions from a duplicate-delivered changelog. Dataset policy and seeds are fingerprinted in the report.

The fixed fixture initially emits count **2**, sum **7** for `[0,10)`. A late value 5 changes it to count **3**, sum **12**, with retraction of the old sum and upsert of the new sum. Source replay has no extra effect. Once watermark reaches 15 under lateness 5, the window is finalized; a later value 99 for that window is dropped. These are software semantics on fictional data, not model-quality numbers.

Persistent CLI reproduction:

```bash
python -m eventflow run --input streams/stream.jsonl --config streams/default.json --database results/checkpoint.sqlite --output results/first.json
python -m eventflow run --input streams/stream.jsonl --config streams/default.json --database results/checkpoint.sqlite --output results/replayed.json
```

The first invocation records **10 commits/1 duplicate**. The second records **0 commits/11 duplicates**, with identical checkpoint and sink state. The CLI integration test executes this exact behavior in a fresh database. Examples contain actual executed JSON; local SQLite files are intentionally excluded from version control.

No external service, paid resource, pretrained model, real Kafka deployment or performance benchmark was used. Finite-state capacities and explicit source progress policy define the scope of these results.
