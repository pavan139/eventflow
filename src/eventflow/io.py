"""Bounded local JSON configuration and source ingestion."""

import json
from .processor import Config, Input


def load_config(path):
    if path is None:
        return Config()
    with open(path, encoding="utf-8") as source:
        text = source.read(65537)
    if len(text) > 65536:
        raise ValueError("Configuration exceeds65536 characters")
    row = json.loads(text)
    if not isinstance(row, dict):
        raise ValueError("Configuration must be an object")
    if "partitions" in row:
        if not isinstance(row["partitions"], list):
            raise ValueError("Partitions must be an array")
        row["partitions"] = tuple(row["partitions"])
    try:
        return Config(**row)
    except TypeError as exc:
        raise ValueError("Unknown configuration fields") from exc


def load_inputs(path):
    result = []
    total = 0
    with open(path, encoding="utf-8") as source:
        while True:
            line = source.readline(65537)
            if not line:
                break
            total += len(line)
            if len(line) > 65536 or total > 10000000:
                raise ValueError("Source import character limit exceeded")
            if not line.strip():
                continue
            if len(result) >= 40000:
                raise ValueError("Source import record count exceeded")
            row = json.loads(line)
            if not isinstance(row, dict):
                raise ValueError("Source records must be objects")
            try:
                result.append(Input(**row))
            except TypeError as exc:
                raise ValueError("Invalid source record schema") from exc
    return result
