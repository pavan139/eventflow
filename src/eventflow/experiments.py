from __future__ import annotations
from dataclasses import asdict
from pathlib import Path
import random
import tempfile
from .processor import Config, CrashInjected, Input, Processor, fingerprint
from .oracle import offline_windows, replay_changes


def fixture():
    return [
        Input("left", 0, 2, value=3),
        Input("right", 0, 3, value=4),
        Input("left", 1, 12, kind="advance"),
        Input("right", 1, 12, kind="advance"),
        Input("left", 2, 4, value=5),
        Input("left", 2, 4, value=5),
        Input("left", 3, 17, kind="advance"),
        Input("right", 2, 17, kind="advance"),
        Input("right", 3, 5, value=99),
        Input("left", 4, 40, kind="advance"),
        Input("right", 4, 40, kind="advance"),
    ]


def _projection(snapshot):
    return [{key: value for key, value in row.items() if key != "revision"} for row in snapshot["sink"]]


def run_case(records, config, crash_stage=None):
    with tempfile.TemporaryDirectory(prefix="eventflow-") as directory:
        database = str(Path(directory) / "stream.sqlite")
        fired = False

        def fault(stage, record):
            nonlocal fired
            if stage == crash_stage and record.partition == "left" and record.offset == 2 and not fired:
                fired = True
                raise CrashInjected(stage)

        with Processor(database, config, fault) as processor:
            for index, record in enumerate(records):
                try:
                    processor.apply(record)
                except CrashInjected:
                    break
            else:
                index = len(records)
        with Processor(database, config) as recovered:
            for record in records[index:]:
                recovered.apply(record)
            # Replay all source records after recovery, including records that
            # committed before a crash. Stable source identities suppress them.
            before = recovered.snapshot()
            for record in records:
                recovered.apply(record)
            snapshot = recovered.snapshot()
        expected = offline_windows(records, config)
        consumer = replay_changes(snapshot["changes"] + snapshot["changes"])
        return {
            "passed": _projection(snapshot) == expected
            and consumer == snapshot["sink"]
            and before == snapshot
            and (crash_stage is None or fired),
            "oracle_windows": expected,
            "snapshot": snapshot,
            "crash_injected": fired,
            "replay_changed_state": before != snapshot,
        }


def demo():
    result = run_case(fixture(), Config(), "after_commit")
    result["scope"] = "Local SQLite state, source ledger and sink only; no external exactly-once guarantee."
    return result


def evaluate():
    cases = []
    for slide in (10, 5, 3):
        for lateness in (0, 5):
            for crash in (None, "before_commit", "after_commit"):
                config = Config(slide=slide, allowed_lateness=lateness)
                result = run_case(fixture(), config, crash)
                cases.append(
                    {
                        "id": f"slide-{slide}-lateness-{lateness}-crash-{crash}",
                        "passed": result["passed"],
                        "windows": len(result["snapshot"]["sink"]),
                        "changes": len(result["snapshot"]["changes"]),
                    }
                )
    # Out-of-order event-time scenarios retain partition-offset identity order;
    # time is randomized independently, and both partitions end with progress.
    for seed in range(12):
        rng = random.Random(seed)
        records, offsets = [], {"left": 0, "right": 0}
        for _ in range(35):
            partition = rng.choice(("left", "right"))
            record = Input(
                partition, offsets[partition], rng.randrange(40), key=rng.choice(("a", "b")), value=rng.randint(-9, 9)
            )
            records.append(record)
            offsets[partition] += 1
            if rng.random() < 0.2:
                records.append(record)
        for partition in offsets:
            records.append(Input(partition, offsets[partition], 100, kind="advance"))
        config = Config(window_size=12, slide=4, allowed_lateness=7)
        result = run_case(records, config, "after_commit" if seed % 2 else "before_commit")
        cases.append(
            {
                "id": f"out-of-order-seed-{seed}",
                "passed": result["passed"],
                "windows": len(result["snapshot"]["sink"]),
                "changes": len(result["snapshot"]["changes"]),
            }
        )
    payload = {
        "fixture": [asdict(row) for row in fixture()],
        "seeds": list(range(12)),
        "version": "eventflow-experiment-v1",
    }
    return {
        "schema_version": 1,
        "dataset_sha256": fingerprint(payload),
        "provenance": "synthetic local event-time streams",
        "passed": sum(row["passed"] for row in cases),
        "total": len(cases),
        "cases": cases,
        "guarantee_scope": "Atomic local SQLite transaction, replay identity ledger and local sink.",
        "external_services_called": False,
    }
