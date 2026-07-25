# Replay and recovery checks

The implementation was verified with **17 passing unit tests**, including:

- Partition-minimum watermark blocking and half-open timestamp boundaries.
- Admissible late correction, exact old-value retraction, inclusive closure and final state eviction.
- Partially late sliding-window memberships checked against a history-based offline oracle.
- Stable offset deduplication, out-of-order offsets and payload conflicts without state mutation.
- Crashes immediately before and after a late sink correction commit; new connections prove rollback versus replay suppression.
- Configuration mismatch, source/active/changelog capacity rollback and admitted-partition validation.
- WAL/FULL durability settings, maximal-timestamp draining and independently replayed sink changes.
- Actual CLI ingestion twice against the same database, proving ten first commits and an unchanged replay.

The separate evaluation executes 30 scenarios and compares SQL state to recomputed individual contributions; it also consumes the exported changelog twice through a separate idempotent consumer. It does not ask a model or the processor to grade its own answer.

Commands run from the repository with `PYTHONPATH=src` and the available Python environment:

```bash
python -m unittest discover -s recovery_tests -v
python -m eventflow demo --output traces/demo.json
python -m eventflow evaluate --output traces/evaluation.json
```

All tests and all 30 scenario checks passed. Ruff syntax/undefined-name checks and formatting were run on owned Python sources and tests. The root-owned verification/CI scripts are maintained separately.

Limitations remain explicit: tests do not simulate physical disk corruption, failed fsync, cross-host concurrency, real broker rebalancing, remote sinks or production throughput. The injected exception approximates interruption at known transaction boundaries; it is not a power-loss laboratory. No universal exactly-once or general test-coverage percentage is claimed.
