# Changing event-time behavior

Write the expected window memberships and watermark before changing `processor.py`. The inclusion rules are half-open for event time and closed at the lateness cutoff. A one-character boundary change can affect only one of an event's sliding memberships, so retain that case in `recovery_tests/`.

```bash
PYTHONPATH=src python -m unittest discover -s recovery_tests -v
python -m ruff check src recovery_tests
python -m ruff format --check src recovery_tests
```

Keep `oracle.py` independent of the SQL accumulator and backward membership enumeration. It should recompute from accepted contributions, not call the function it is supposed to check. Storage changes also need before/after-commit fault tests at a late sink correction, including the source identity ledger and emitted revision.

Use `streams/` for small source/policy fixtures and `traces/` for measured output. Preserve offset identities when testing retries. Do not silently change old fixture payloads under an existing identity; that is precisely the conflict the processor rejects. Describe a new policy or workload in `protocol/window-histories.md`.
