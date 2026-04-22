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


def parse_all_groups_describe_row_counts(stdout: str) -> dict[str, int]:
    """
    Parse ``kafka-consumer-groups.sh --describe --all-groups`` table lines
    (GROUP TOPIC PARTITION CURRENT-OFFSET ... LAG ...) and count data rows per group.
    Used as a rough relative size signal (not authoritative disk usage).
    """
    counts: dict[str, int] = {}
    for ln in stdout.splitlines():
        ln = ln.strip()
        if not ln:
            continue
        parts = ln.split()
        if len(parts) < 9:
            continue
        if parts[0].upper() == "GROUP" and parts[1].upper() == "TOPIC":
            continue
        g = parts[0]
        try:
            int(parts[5])
        except (ValueError, IndexError):
            continue
        if not parts[2].isdigit():
            continue
        counts[g] = counts.get(g, 0) + 1
    return counts


def consumer_groups_list_split_panel_stdout(stdout: str) -> tuple[str, str]:
    """Split combined list + ``--all-groups`` script output into (list_body, describe_body)."""
    raw = stdout or ""
    m_list = "---GB_STS_LIST---"
    m_desc = "---GB_STS_DESCRIBE_ALL---"
    if m_list not in raw or m_desc not in raw:
        return raw.strip(), ""
    _, b = raw.split(m_list, 1)
    list_part, desc_part = b.split(m_desc, 1)
    return list_part.strip(), desc_part.strip()


_PART_LINE_RE = re.compile(
    r"Topic:\s*(?P<topic>\S+)\s+Partition:\s*(?P<partition>\d+)\s+"
    r"Leader:\s*(?P<leader>\S+)\s+Replicas:\s*(?P<replicas>\S+)\s+Isr:\s*(?P<isr>\S+)\s*$",
    re.I,
)


_TOPIC_SUMMARY_RE = re.compile(
    r"^Topic:\s*(?P<Topic>\S+)\s+TopicId:\s*(?P<TopicId>\S+)\s+PartitionCount:\s*(?P<PartitionCount>\d+)\s+"
    r"ReplicationFactor:\s*(?P<ReplicationFactor>\d+)\s+Configs:\s*(?P<Configs>.+)$"
)


def parse_topic_describe(stdout: str) -> tuple[list[list[str]], list[list[str]]]:
    """
    Parse ``kafka-topics.sh --describe --topic`` output.

    Returns ``(summary_table, partition_table)`` each with header row, or ``[]`` if nothing parsed.
    Summary is key/value rows; partitions are Topic, Partition, Leader, Replicas, Isr.
    """
    summary_kv: dict[str, str] = {}
    part_rows: list[list[str]] = []
    for raw in stdout.splitlines():
        ln = raw.strip()
        if not ln:
            continue
        m = _PART_LINE_RE.search(ln)
        if m:
            part_rows.append(
                [
                    m.group("topic"),
                    m.group("partition"),
                    m.group("leader"),
                    m.group("replicas"),
                    m.group("isr"),
                ]
            )
            continue
        sm = _TOPIC_SUMMARY_RE.match(ln)
        if sm:
            for k, v in sm.groupdict().items():
                if v is not None:
                    summary_kv[k] = v.strip()
            continue
        if "\t" in ln:
            for chunk in ln.split("\t"):
                chunk = chunk.strip()
                if ":" in chunk:
                    k, _, v = chunk.partition(":")
                    summary_kv[k.strip()] = v.strip()
        else:
            for chunk in re.split(r"\s{2,}", ln):
                chunk = chunk.strip()
                if ":" in chunk and not _PART_LINE_RE.search(chunk):
                    k, _, v = chunk.partition(":")
                    summary_kv[k.strip()] = v.strip()

    summary_table: list[list[str]] = []
    if summary_kv:
        summary_table = [["Key", "Value"]] + [[k, summary_kv[k]] for k in sorted(summary_kv.keys(), key=str.casefold)]

    part_table: list[list[str]] = []
    if part_rows:
        part_table = [["Topic", "Partition", "Leader", "Replicas", "Isr"]] + part_rows
    return summary_table, part_table


def parse_topic_end_offsets(stdout: str) -> list[list[str]]:
    """
    Parse ``GetOffsetShell`` lines ``topic:partition:end_offset``.
    Returns table with header, or ``[]`` if nothing parsed.
    """
    rows: list[list[str]] = []
    for raw in stdout.splitlines():
        ln = raw.strip()
        if not ln or ln.startswith("#") or ln.upper().startswith("ERROR"):
            continue
        parts = ln.rsplit(":", 2)
        if len(parts) != 3:
            continue
        topic, part, off = parts[0], parts[1], parts[2]
        if not part.isdigit():
            continue
        if not re.fullmatch(r"-?\d+", off):
            continue
        rows.append([topic, part, off])
    rows.sort(key=lambda r: (r[0].casefold(), int(r[1])))
    if not rows:
        return []
    return [["Topic", "Partition", "End offset"]] + rows


def parse_du_kafka_data_dirs(stdout: str) -> list[list[str]]:
    """
    Parse ``du -sk`` lines under ``/bitnami/kafka/data/*`` (size KB, path).
    Returns a table with header ``Topic, Partition, Size (KB), Path``.
    Directory names are usually ``<topic>-<partition>``.
    """
    header = ["Topic", "Partition", "Size (KB)", "Path"]
    rows: list[list[str]] = []
    for raw in stdout.splitlines():
        ln = raw.strip()
        if not ln:
            continue
        if "\t" in ln:
            sk, path = ln.split("\t", 1)
        else:
            bits = re.split(r"\s+", ln, maxsplit=1)
            if len(bits) != 2:
                continue
            sk, path = bits[0], bits[1]
        sk = sk.strip()
        path = path.strip()
        if not sk.isdigit():
            continue
        basename = path.rsplit("/", 1)[-1]
        m = re.match(r"^(.+)-(\d+)$", basename)
        if m:
            topic, part = m.group(1), m.group(2)
        else:
            topic, part = basename, ""
        rows.append([topic, part, sk, path])
    if not rows:
        return []
    return [header] + rows


def aggregate_du_kafka_data_by_topic(stdout: str) -> list[list[str]]:
    """
    Parse ``du -sk`` under ``/bitnami/kafka/data/*``, sum kilobytes per topic, return
    ``[["Topic", "Size (MB)"], ...]`` sorted by total size descending.
    """
    per_part = parse_du_kafka_data_dirs(stdout)
    if len(per_part) < 2:
        return []
    totals: dict[str, int] = {}
    for row in per_part[1:]:
        if len(row) < 3:
            continue
        topic, sk = row[0], row[2]
        if not str(sk).isdigit():
            continue
        totals[topic] = totals.get(topic, 0) + int(sk)
    rows_sorted = sorted(totals.items(), key=lambda x: x[1], reverse=True)
    return [["Topic", "Size (MB)"]] + [[t, f"{kb / 1024.0:.2f}"] for t, kb in rows_sorted]


def parse_topic_leader_skew(
    describe_stdout: str,
    *,
    topic_sizes_mb: dict[str, str] | None = None,
    min_partitions: int = 2,
    limit: int = 40,
) -> list[list[str]]:
    """
    Parse ``kafka-topics.sh --describe`` output and highlight topics where leaders are unevenly spread.
    Returns ``[]`` when no skew rows found; otherwise header + rows:
    Topic, Partitions, Max leader/broker, Min leader/broker, Delta, Delta %, Size (MB)
    """
    topic_sizes = topic_sizes_mb or {}
    agg: dict[str, dict[str, Any]] = {}
    for raw in (describe_stdout or "").splitlines():
        ln = raw.strip()
        if not ln:
            continue
        m = _PART_LINE_RE.search(ln)
        if not m:
            continue
        topic = m.group("topic")
        leader = m.group("leader")
        replicas = m.group("replicas")
        if not topic:
            continue
        row = agg.setdefault(topic, {"parts": 0, "leader_counts": {}, "replicas": set()})
        row["parts"] += 1
        if re.fullmatch(r"\d+", leader):
            lc = row["leader_counts"]
            lc[leader] = lc.get(leader, 0) + 1
        for b in replicas.split(","):
            bid = b.strip()
            if re.fullmatch(r"\d+", bid):
                row["replicas"].add(bid)

    out_rows: list[list[str]] = []
    for topic, row in agg.items():
        parts = int(row.get("parts") or 0)
        if parts < min_partitions:
            continue
        leaders: dict[str, int] = row.get("leader_counts") or {}
        broker_ids = row.get("replicas") or set(leaders.keys())
        if not broker_ids:
            continue
        counts = [int(leaders.get(b, 0)) for b in sorted(broker_ids, key=int)]
        if not counts:
            continue
        mx = max(counts)
        mn = min(counts)
        delta = mx - mn
        if delta <= 0:
            continue
        pct = (float(delta) * 100.0 / float(parts)) if parts > 0 else 0.0
        out_rows.append(
            [
                topic,
                str(parts),
                str(mx),
                str(mn),
                str(delta),
                f"{pct:.1f}",
                str(topic_sizes.get(topic, "N/A")),
            ]
        )

    out_rows.sort(
        key=lambda r: (
            -int(r[4]),
            -int(r[1]),
            -float(r[6]) if re.fullmatch(r"\d+(\.\d+)?", r[6]) else 0.0,
            r[0].casefold(),
        )
    )
    if not out_rows:
        return []
    hdr = ["Topic", "Partitions", "Max leader/broker", "Min leader/broker", "Delta", "Delta %", "Size (MB)"]
    return [hdr] + out_rows[: max(1, int(limit))]


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
