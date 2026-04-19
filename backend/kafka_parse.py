"""Parse Kafka CLI output for richer panel tables."""

from __future__ import annotations

import re
from typing import Any


def parse_group_lag_rows(stdout: str, *, limit: int = 5) -> tuple[list[list[str]], list[str]]:
    """
    Parse kafka-consumer-groups --describe table lines into rows.
    Returns (rows_for_table, header) where rows are sorted by LAG descending and truncated to ``limit``.
    """
    lines = [ln.rstrip() for ln in stdout.splitlines() if ln.strip()]
    if not lines:
        return [], []
    header = lines[0].split()
    if not header or header[0].upper() != "GROUP":
        # fallback: treat as unstructured
        return [[ln] for ln in lines[:limit]], ["line"]

    rows: list[dict[str, Any]] = []
    for ln in lines[1:]:
        parts = ln.split()
        if len(parts) < 9:
            continue
        try:
            lag = int(parts[5])
        except (ValueError, IndexError):
            continue
        group, topic, partition, cur_off, log_end = parts[0], parts[1], parts[2], parts[3], parts[4]
        consumer_id = parts[6]
        host = parts[7]
        client_id = " ".join(parts[8:])
        rows.append(
            {
                "lag": lag,
                "cells": [group, topic, partition, cur_off, log_end, str(lag), consumer_id, host, client_id],
            }
        )
    rows.sort(key=lambda r: r["lag"], reverse=True)
    slim = [r["cells"] for r in rows[:limit]]
    hdr = ["GROUP", "TOPIC", "PARTITION", "CURRENT-OFFSET", "LOG-END-OFFSET", "LAG", "CONSUMER-ID", "HOST", "CLIENT-ID"]
    return slim, hdr


def parse_consumer_group_names(stdout: str) -> list[str]:
    names = [ln.strip() for ln in stdout.splitlines() if ln.strip()]
    return sorted(set(names), key=lambda s: s.casefold())


def filter_topic_partition_rows(
    rows: list[list[str]],
    *,
    min_partitions: int = 0,
    topic_substring: str = "",
) -> list[list[str]]:
    """rows are [count, topic] from parse_topics_partition_counts."""
    sub = (topic_substring or "").strip().casefold()
    out: list[list[str]] = []
    for row in rows:
        if len(row) != 2:
            continue
        try:
            cnt = int(row[0])
        except ValueError:
            continue
        topic = row[1]
        if cnt <= min_partitions:
            continue
        if sub and sub not in topic.casefold():
            continue
        out.append(row)
    return out
