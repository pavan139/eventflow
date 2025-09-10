"""Event-time windows with one SQLite transaction per source input.

Local effects only: no external broker or remote exactly-once delivery claim.
"""

from __future__ import annotations
from contextlib import contextmanager
from dataclasses import asdict, dataclass
import hashlib
import json
import re
import sqlite3
from typing import Callable

NAME = re.compile(r"[A-Za-z0-9_-]{1,32}\Z")


def fingerprint(value) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


@dataclass(frozen=True)
class Config:
    partitions: tuple[str, ...] = ("left", "right")
    window_size: int = 10
    slide: int = 10
    out_of_orderness: int = 2
    allowed_lateness: int = 5
    max_inputs: int = 5000
    max_windows: int = 20000
    max_changes: int = 100000

    def __post_init__(self):
        if (
            not isinstance(self.partitions, tuple)
            or not 1 <= len(self.partitions) <= 8
            or any(type(p) is not str or not NAME.fullmatch(p) for p in self.partitions)
            or len(set(self.partitions)) != len(self.partitions)
        ):
            raise ValueError("Configure one to eight unique bounded source partitions")
        if any(
            type(n) is not int
            for n in (
                self.window_size,
                self.slide,
                self.out_of_orderness,
                self.allowed_lateness,
                self.max_inputs,
                self.max_windows,
                self.max_changes,
            )
        ):
            raise ValueError("Window and capacity parameters must be integers")
        if not 1 <= self.slide <= self.window_size <= 10000 or (self.window_size + self.slide - 1) // self.slide > 32:
            raise ValueError("Window size must be 1..10000, slide 1..size, with at most32 windows/event")
        if not 0 <= self.out_of_orderness <= 10000 or not 0 <= self.allowed_lateness <= 10000:
            raise ValueError("Lateness and out-of-orderness must be 0..10000")
        if (
            not 1 <= self.max_inputs <= 20000
            or not 1 <= self.max_windows <= 100000
            or not 1 <= self.max_changes <= 200000
        ):
            raise ValueError("Capacity parameters exceed supported bounds")


@dataclass(frozen=True)
class Input:
    partition: str
    offset: int
    timestamp: int
    kind: str = "event"
    key: str = "feature"
    value: int = 1

    def __post_init__(self):
        if (
            type(self.partition) is not str
            or not NAME.fullmatch(self.partition)
            or type(self.key) is not str
            or not NAME.fullmatch(self.key)
        ):
            raise ValueError("Partition and key must be bounded identifiers")
        if any(type(n) is not int for n in (self.offset, self.timestamp, self.value)):
            raise ValueError("Offset, event time and value must be integers")
        if (
            not 0 <= self.offset <= 10**9
            or not 0 <= self.timestamp <= 10**9 + (30000 if self.kind == "advance" else 0)
            or abs(self.value) > 10**6
        ):
            raise ValueError("Input exceeds offset/time/value bounds")
        if self.kind not in ("event", "advance"):
            raise ValueError("Input kind must be event or advance")
        if self.kind == "advance" and (self.key != "feature" or self.value != 1):
            raise ValueError("Progress markers do not carry an aggregation payload")


class CrashInjected(RuntimeError):
    pass


class OffsetConflict(ValueError):
    pass


class CapacityExceeded(RuntimeError):
    pass


class Processor:
    def __init__(
        self,
        database: str,
        config: Config | None = None,
        fault: Callable[[str, Input], None] = lambda stage, record: None,
    ):
        self.config = config or Config()
        self.fault = fault
        self.db = sqlite3.connect(database, timeout=2, isolation_level=None)
        self.db.row_factory = sqlite3.Row
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("PRAGMA synchronous=FULL")
        self.db.executescript("""
          CREATE TABLE IF NOT EXISTS metadata(name TEXT PRIMARY KEY,value TEXT NOT NULL);
          CREATE TABLE IF NOT EXISTS partitions(name TEXT PRIMARY KEY,high_event_time INTEGER NOT NULL);
          CREATE TABLE IF NOT EXISTS offsets(partition TEXT,offset INTEGER,payload_hash TEXT NOT NULL,PRIMARY KEY(partition,offset));
          CREATE TABLE IF NOT EXISTS active(key TEXT,start INTEGER,end INTEGER,count INTEGER,total INTEGER,
            emitted_count INTEGER DEFAULT 0,emitted_total INTEGER DEFAULT 0,revision INTEGER DEFAULT 0,PRIMARY KEY(key,start));
          CREATE INDEX IF NOT EXISTS active_close ON active(end);
          CREATE INDEX IF NOT EXISTS active_emit ON active(end) WHERE revision=0;
          CREATE TABLE IF NOT EXISTS sink(key TEXT,start INTEGER,end INTEGER,count INTEGER,total INTEGER,revision INTEGER,final INTEGER,PRIMARY KEY(key,start));
          CREATE TABLE IF NOT EXISTS changes(sequence INTEGER PRIMARY KEY AUTOINCREMENT,key TEXT,start INTEGER,end INTEGER,
            revision INTEGER,operation TEXT,count INTEGER,total INTEGER,watermark INTEGER,UNIQUE(key,start,revision,operation));
          CREATE TABLE IF NOT EXISTS checkpoints(sequence INTEGER PRIMARY KEY AUTOINCREMENT,partition TEXT,offset INTEGER,
            watermark INTEGER,accepted_windows INTEGER,dropped_windows INTEGER,kind TEXT);
        """)
        try:
            with self.transaction():
                stored = self.db.execute("SELECT value FROM metadata WHERE name='config'").fetchone()
                encoded = json.dumps(asdict(self.config), sort_keys=True)
                if stored and stored["value"] != encoded:
                    raise ValueError("Checkpoint configuration mismatch; use the original window policy")
                if not stored:
                    self.db.execute("INSERT INTO metadata VALUES('config',?)", (encoded,))
                    self.db.executemany("INSERT INTO partitions VALUES(?,-1)", [(p,) for p in self.config.partitions])
        except BaseException:
            self.db.close()
            raise

    @contextmanager
    def transaction(self, write=True):
        self.db.execute("BEGIN IMMEDIATE" if write else "BEGIN")
        try:
            yield
        except BaseException:
            self.db.rollback()
            raise
        else:
            self.db.commit()

    def close(self):
        self.db.close()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

    def watermark(self):
        return min(
            max(-1, row[0] - self.config.out_of_orderness)
            for row in self.db.execute("SELECT high_event_time FROM partitions")
        )

    def starts(self, timestamp):
        latest = timestamp // self.config.slide * self.config.slide
        return [start for start in range(latest, max(-1, timestamp - self.config.window_size), -self.config.slide)]

    def _change(self, window, revision, operation, count, total, watermark):
        if self.db.execute("SELECT COUNT(*) FROM changes").fetchone()[0] >= self.config.max_changes:
            raise CapacityExceeded("Local sink changelog capacity exhausted")
        self.db.execute(
            "INSERT INTO changes(key,start,end,revision,operation,count,total,watermark) VALUES(?,?,?,?,?,?,?,?)",
            (window["key"], window["start"], window["end"], revision, operation, count, total, watermark),
        )

    def _emit(self, window, watermark):
        revision = window["revision"] + 1
        if window["revision"]:
            self._change(window, revision, "retract", window["emitted_count"], window["emitted_total"], watermark)
        elif self.db.execute("SELECT COUNT(*) FROM sink").fetchone()[0] >= self.config.max_windows:
            raise CapacityExceeded("Local sink window capacity exhausted")
        self._change(window, revision, "upsert", window["count"], window["total"], watermark)
        self.db.execute(
            "INSERT INTO sink VALUES(?,?,?,?,?,?,0) ON CONFLICT(key,start) DO UPDATE SET count=excluded.count,total=excluded.total,revision=excluded.revision",
            (window["key"], window["start"], window["end"], window["count"], window["total"], revision),
        )
        self.db.execute(
            "UPDATE active SET emitted_count=count,emitted_total=total,revision=? WHERE key=? AND start=?",
            (revision, window["key"], window["start"]),
        )

    def _flush(self, watermark):
        windows = self.db.execute(
            "SELECT * FROM active WHERE (revision=0 AND end<=?) OR end<=? ORDER BY start,key",
            (watermark, watermark - self.config.allowed_lateness),
        ).fetchall()
        for window in windows:
            if not window["revision"]:
                self._emit(window, watermark)
            if watermark >= window["end"] + self.config.allowed_lateness:
                latest = self.db.execute(
                    "SELECT * FROM active WHERE key=? AND start=?", (window["key"], window["start"])
                ).fetchone()
                self._change(latest, latest["revision"], "finalize", latest["count"], latest["total"], watermark)
                self.db.execute("UPDATE sink SET final=1 WHERE key=? AND start=?", (window["key"], window["start"]))
                self.db.execute("DELETE FROM active WHERE key=? AND start=?", (window["key"], window["start"]))

    def apply(self, record: Input):
        if not isinstance(record, Input) or record.partition not in self.config.partitions:
            raise ValueError("Source partition was not admitted by the processor configuration")
        payload = fingerprint(asdict(record))
        duplicate = False
        with self.transaction():
            previous = self.db.execute(
                "SELECT payload_hash FROM offsets WHERE partition=? AND offset=?", (record.partition, record.offset)
            ).fetchone()
            if previous:
                if previous["payload_hash"] != payload:
                    raise OffsetConflict("Source offset reused with another payload")
                duplicate = True
            else:
                if self.db.execute("SELECT COUNT(*) FROM offsets").fetchone()[0] >= self.config.max_inputs:
                    raise CapacityExceeded("Source identity ledger capacity exhausted")
                self.db.execute("INSERT INTO offsets VALUES(?,?,?)", (record.partition, record.offset, payload))
                self.db.execute(
                    "UPDATE partitions SET high_event_time=MAX(high_event_time,?) WHERE name=?",
                    (record.timestamp, record.partition),
                )
                watermark = self.watermark()
                accepted = dropped = 0
                if record.kind == "event":
                    for start in self.starts(record.timestamp):
                        end = start + self.config.window_size
                        if watermark >= end + self.config.allowed_lateness:
                            dropped += 1
                            continue
                        old = self.db.execute(
                            "SELECT * FROM active WHERE key=? AND start=?", (record.key, start)
                        ).fetchone()
                        if (
                            not old
                            and self.db.execute("SELECT COUNT(*) FROM active").fetchone()[0] >= self.config.max_windows
                        ):
                            raise CapacityExceeded("Active window capacity exhausted")
                        self.db.execute(
                            "INSERT INTO active(key,start,end,count,total) VALUES(?,?,?,1,?) ON CONFLICT(key,start) DO UPDATE SET count=count+1,total=total+excluded.total",
                            (record.key, start, end, record.value),
                        )
                        accepted += 1
                        if old and old["revision"]:
                            changed = self.db.execute(
                                "SELECT * FROM active WHERE key=? AND start=?", (record.key, start)
                            ).fetchone()
                            self._emit(changed, watermark)
                self._flush(watermark)
                self.db.execute(
                    "INSERT INTO checkpoints(partition,offset,watermark,accepted_windows,dropped_windows,kind) VALUES(?,?,?,?,?,?)",
                    (record.partition, record.offset, watermark, accepted, dropped, record.kind),
                )
                self.fault("before_commit", record)
            committed_watermark = self.watermark()
            committed_inputs = self.db.execute("SELECT COUNT(*) FROM checkpoints").fetchone()[0]
        if not duplicate:
            self.fault("after_commit", record)
        return {
            "status": "duplicate" if duplicate else "committed",
            "source": [record.partition, record.offset],
            "watermark": committed_watermark,
            "checkpoint_inputs": committed_inputs,
        }

    def snapshot(self):
        with self.transaction(write=False):
            result = {
                "config": asdict(self.config),
                "config_sha256": fingerprint(asdict(self.config)),
                "watermark": self.watermark(),
                "partitions": {
                    row["name"]: row["high_event_time"]
                    for row in self.db.execute("SELECT * FROM partitions ORDER BY name")
                },
                "offset_count": self.db.execute("SELECT COUNT(*) FROM offsets").fetchone()[0],
                "active_windows": [dict(row) for row in self.db.execute("SELECT * FROM active ORDER BY start,key")],
                "sink": [dict(row) for row in self.db.execute("SELECT * FROM sink ORDER BY start,key")],
                "changes": [dict(row) for row in self.db.execute("SELECT * FROM changes ORDER BY sequence")],
                "checkpoints": [dict(row) for row in self.db.execute("SELECT * FROM checkpoints ORDER BY sequence")],
            }
        return result
