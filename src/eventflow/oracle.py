"""Independent offline oracle: retain contributions, recompute sums from history."""

from __future__ import annotations
from dataclasses import astuple
from .processor import Config, Input


def offline_windows(records: list[Input], config: Config):
    high = {partition: -1 for partition in config.partitions}
    seen, history, projection = {}, {}, {}
    for record in records:
        if record.partition not in high:
            raise ValueError("Unknown oracle source partition")
        identity, payload = (record.partition, record.offset), astuple(record)
        if identity in seen:
            if seen[identity] != payload:
                raise ValueError("Conflicting oracle source identity")
            continue
        seen[identity] = payload
        high[record.partition] = max(high[record.partition], record.timestamp)
        watermark = min(max(-1, timestamp - config.out_of_orderness) for timestamp in high.values())
        if record.kind == "event":
            earliest = max(0, record.timestamp - config.window_size + 1)
            first_aligned = ((earliest + config.slide - 1) // config.slide) * config.slide
            for start in range(first_aligned, record.timestamp + 1, config.slide):
                if start + config.window_size + config.allowed_lateness > watermark:
                    history.setdefault((record.key, start), []).append(record.value)
        for (key, start), contributions in history.items():
            if start + config.window_size <= watermark:
                projection[key, start] = {
                    "key": key,
                    "start": start,
                    "end": start + config.window_size,
                    "count": len(contributions),
                    "total": sum(contributions),
                    "final": int(start + config.window_size + config.allowed_lateness <= watermark),
                }
    return sorted(projection.values(), key=lambda row: (row["start"], row["key"]))


def replay_changes(changes):
    """Independent idempotent projection consumer for the exported local changelog."""
    seen, projection = {}, {}
    for row in changes:
        sequence = row["sequence"]
        if sequence in seen:
            if seen[sequence] != row:
                raise ValueError("Conflicting changelog sequence")
            continue
        seen[sequence] = dict(row)
        identity = (row["key"], row["start"])
        current = projection.get(identity)
        if row["operation"] == "retract":
            if current is None or any(current[name] != row[name] for name in ("count", "total")):
                raise ValueError("Retraction does not match a materialized value")
            del projection[identity]
        elif row["operation"] == "upsert":
            if current is not None:
                raise ValueError("An upsert correction requires its prior retraction")
            projection[identity] = {name: row[name] for name in ("key", "start", "end", "count", "total", "revision")}
            projection[identity]["final"] = 0
        elif row["operation"] == "finalize":
            if current is None or any(current[name] != row[name] for name in ("count", "total", "revision")):
                raise ValueError("Finalization does not match the current revision")
            current["final"] = 1
        else:
            raise ValueError("Unknown changelog operation")
    return sorted(projection.values(), key=lambda row: (row["start"], row["key"]))
