from dataclasses import asdict, replace
import json
import subprocess
import sys
from pathlib import Path
import tempfile
import unittest
from eventflow.processor import CapacityExceeded, Config, CrashInjected, Input, OffsetConflict, Processor
from eventflow.experiments import evaluate, fixture
from eventflow.oracle import offline_windows, replay_changes


class EventflowTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.database = str(Path(self.directory.name) / "state.sqlite")

    def tearDown(self):
        self.directory.cleanup()

    def test_partition_minimum_blocks_early_emission(self):
        with Processor(self.database) as processor:
            processor.apply(Input("left", 0, 2, value=7))
            processor.apply(Input("left", 1, 30, kind="advance"))
            self.assertEqual(processor.snapshot()["watermark"], -1)
            self.assertEqual(processor.snapshot()["sink"], [])
            processor.apply(Input("right", 0, 12, kind="advance"))
            state = processor.snapshot()
            self.assertEqual(state["watermark"], 10)
            self.assertEqual(state["sink"][0]["total"], 7)
            self.assertFalse(state["sink"][0]["final"])

    def test_late_correction_retracts_then_upserts_and_closure_is_inclusive(self):
        with Processor(self.database) as processor:
            for record in fixture():
                processor.apply(record)
            state = processor.snapshot()
        self.assertEqual(state["sink"][0]["count"], 3)
        self.assertEqual(state["sink"][0]["total"], 12)
        self.assertEqual(state["sink"][0]["revision"], 2)
        self.assertEqual(state["sink"][0]["final"], 1)
        self.assertEqual([row["operation"] for row in state["changes"]], ["upsert", "retract", "upsert", "finalize"])
        self.assertEqual(state["changes"][1]["total"], 7)
        self.assertEqual(state["changes"][2]["total"], 12)
        self.assertEqual(state["checkpoints"][-3]["dropped_windows"], 1)
        self.assertEqual(state["active_windows"], [])
        self.assertEqual(replay_changes(state["changes"] * 2), state["sink"])

    def test_sliding_memberships_can_be_partly_too_late(self):
        config = Config(partitions=("p",), slide=5, out_of_orderness=0, allowed_lateness=5)
        records = [Input("p", 0, 8, value=1), Input("p", 1, 15, kind="advance"), Input("p", 2, 8, value=2)]
        with Processor(self.database, config) as processor:
            for record in records:
                processor.apply(record)
            state = processor.snapshot()
        self.assertEqual(state["checkpoints"][-1]["accepted_windows"], 1)
        self.assertEqual(state["checkpoints"][-1]["dropped_windows"], 1)
        projection = [{k: v for k, v in row.items() if k != "revision"} for row in state["sink"]]
        self.assertEqual(projection, offline_windows(records, config))
        self.assertEqual([(row["start"], row["total"]) for row in state["sink"]], [(0, 1), (5, 3)])

    def test_exact_boundary_is_next_tumbling_window(self):
        config = Config(partitions=("p",), out_of_orderness=0, allowed_lateness=0)
        records = [Input("p", 0, 9), Input("p", 1, 10), Input("p", 2, 20, kind="advance")]
        with Processor(self.database, config) as processor:
            for record in records:
                processor.apply(record)
            state = processor.snapshot()
        self.assertEqual([(row["start"], row["count"]) for row in state["sink"]], [(0, 1), (10, 1)])
        self.assertTrue(all(row["final"] for row in state["sink"]))

    def test_duplicate_and_conflicting_offsets_do_not_mutate_state(self):
        with Processor(self.database) as processor:
            record = Input("left", 2, 4, value=3)
            processor.apply(record)
            processor.apply(Input("left", 1, 3, value=2))
            before = processor.snapshot()
            self.assertEqual(processor.apply(record)["status"], "duplicate")
            self.assertEqual(processor.snapshot(), before)
            with self.assertRaises(OffsetConflict):
                processor.apply(replace(record, value=9))
            self.assertEqual(processor.snapshot(), before)

    def test_precommit_crash_rolls_back_late_sink_revision_and_source_state(self):
        config = Config()
        records = fixture()
        with Processor(self.database, config) as processor:
            for record in records[:4]:
                processor.apply(record)
            before = processor.snapshot()

        def fault(stage, record):
            if stage == "before_commit":
                raise CrashInjected()

        with Processor(self.database, config, fault) as processor:
            with self.assertRaises(CrashInjected):
                processor.apply(records[4])
            self.assertEqual(processor.snapshot(), before)
        with Processor(self.database, config) as recovered:
            recovered.apply(records[4])
            after = recovered.snapshot()
        self.assertEqual(after["sink"][0]["total"], 12)
        self.assertEqual(after["sink"][0]["revision"], 2)
        self.assertEqual(after["offset_count"], before["offset_count"] + 1)

    def test_postcommit_crash_replay_does_not_repeat_sink_correction(self):
        records = fixture()
        with Processor(self.database) as processor:
            for record in records[:4]:
                processor.apply(record)

        def fault(stage, record):
            if stage == "after_commit":
                raise CrashInjected()

        with Processor(self.database, fault=fault) as processor:
            with self.assertRaises(CrashInjected):
                processor.apply(records[4])
            committed = processor.snapshot()
        with Processor(self.database) as recovered:
            self.assertEqual(recovered.apply(records[4])["status"], "duplicate")
            self.assertEqual(recovered.snapshot(), committed)
        self.assertEqual(committed["sink"][0]["total"], 12)
        self.assertEqual(len(committed["changes"]), 3)

    def test_config_changes_cannot_reinterpret_a_checkpoint(self):
        with Processor(self.database):
            pass
        with self.assertRaisesRegex(ValueError, "configuration mismatch"):
            Processor(self.database, Config(slide=5))
        with Processor(self.database) as original:
            self.assertEqual(original.snapshot()["offset_count"], 0)

    def test_capacity_failure_is_an_atomic_rollback(self):
        config = Config(partitions=("p",), max_inputs=1)
        with Processor(self.database, config) as processor:
            processor.apply(Input("p", 0, 1))
            before = processor.snapshot()
            with self.assertRaises(CapacityExceeded):
                processor.apply(Input("p", 1, 30))
            self.assertEqual(processor.snapshot(), before)
            self.assertEqual(processor.apply(Input("p", 0, 1))["status"], "duplicate")

    def test_sliding_window_capacity_cannot_leave_half_an_event(self):
        config = Config(partitions=("p",), slide=5, max_windows=1)
        with Processor(self.database, config) as processor:
            before = processor.snapshot()
            with self.assertRaises(CapacityExceeded):
                processor.apply(Input("p", 0, 8))
            self.assertEqual(processor.snapshot(), before)

    def test_sink_changelog_capacity_rolls_back_emission_and_watermark(self):
        config = Config(partitions=("p",), out_of_orderness=0, allowed_lateness=0, max_changes=1)
        with Processor(self.database, config) as processor:
            processor.apply(Input("p", 0, 1))
            before = processor.snapshot()
            with self.assertRaises(CapacityExceeded):
                processor.apply(Input("p", 1, 10, kind="advance"))
            self.assertEqual(processor.snapshot(), before)

    def test_invalid_partition_and_untrusted_arguments_are_rejected(self):
        with Processor(self.database) as processor:
            with self.assertRaises(ValueError):
                processor.apply(Input("shell", 0, 1))
        for options in (
            {"offset": True},
            {"timestamp": -1},
            {"value": 1000001},
            {"kind": "shell"},
            {"key": "x; DROP TABLE"},
        ):
            args = dict(partition="left", offset=0, timestamp=1)
            args.update(options)
            with self.subTest(options=options), self.assertRaises(ValueError):
                Input(**args)
        for options in (
            {"slide": 0},
            {"window_size": 10001},
            {"allowed_lateness": -1},
            {"slide": 1, "window_size": 33},
            {"partitions": ("p", "p")},
        ):
            with self.subTest(options=options), self.assertRaises(ValueError):
                Config(**options)

    def test_wal_and_full_sync_are_enabled(self):
        with Processor(self.database) as processor:
            self.assertEqual(processor.db.execute("PRAGMA journal_mode").fetchone()[0], "wal")
            self.assertEqual(processor.db.execute("PRAGMA synchronous").fetchone()[0], 2)

    def test_independent_offline_experiments(self):
        report = evaluate()
        self.assertEqual(report["total"], 30)
        self.assertEqual(report["passed"], report["total"])

    def test_changelog_conflicts_and_missing_retractions_fail(self):
        with Processor(self.database) as processor:
            for record in fixture():
                processor.apply(record)
            changes = processor.snapshot()["changes"]
        forged = dict(changes[0])
        forged["total"] += 1
        with self.assertRaises(ValueError):
            replay_changes(changes + [forged])
        with self.assertRaises(ValueError):
            replay_changes([changes[0], changes[2]])

    def test_largest_admitted_event_can_be_drained_with_a_bounded_progress_marker(self):
        config = Config(
            partitions=("p",), window_size=10000, slide=10000, out_of_orderness=10000, allowed_lateness=10000
        )
        with Processor(self.database, config) as processor:
            processor.apply(Input("p", 0, 10**9))
            processor.apply(Input("p", 1, 10**9 + 30000, kind="advance"))
            state = processor.snapshot()
        self.assertEqual(len(state["sink"]), 1)
        self.assertEqual(state["sink"][0]["final"], 1)
        self.assertEqual(state["active_windows"], [])
        with self.assertRaises(ValueError):
            Input("p", 2, 10**9 + 1)
        with self.assertRaises(ValueError):
            Input("p", 2, 10**9 + 30001, kind="advance")

    def test_cli_consumes_and_replays_real_source_against_the_same_database(self):
        with tempfile.TemporaryDirectory() as directory:
            folder = Path(directory)
            source = folder / "input.jsonl"
            output = folder / "state.json"
            source.write_text("".join(json.dumps(asdict(row)) + "\n" for row in fixture()))
            command = [
                sys.executable,
                "-m",
                "eventflow",
                "run",
                "--input",
                str(source),
                "--database",
                str(folder / "checkpoint.sqlite"),
                "--output",
                str(output),
            ]
            result = subprocess.run(command, capture_output=True, timeout=10)
            self.assertEqual(result.returncode, 0)
            first = json.loads(output.read_text())
            self.assertEqual(first["outcomes"], {"committed": 10, "duplicate": 1})
            result = subprocess.run(command, capture_output=True, timeout=10)
            self.assertEqual(result.returncode, 0)
            replay = json.loads(output.read_text())
            self.assertEqual(replay["outcomes"], {"committed": 0, "duplicate": 11})
            self.assertEqual(first["state"], replay["state"])


if __name__ == "__main__":
    unittest.main()
