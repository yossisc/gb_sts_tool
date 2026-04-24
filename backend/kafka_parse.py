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


def parse_consumer_group_assigned_topics(stdout: str) -> dict[str, set[str]]:
    """
    From ``kafka-consumer-groups.sh --describe --all-groups`` assignment/lag lines
    (GROUP TOPIC PARTITION ...), collect distinct topic names per consumer group.
    """
    topics: dict[str, set[str]] = {}
    for ln in stdout.splitlines():
        ln = ln.strip()
        if not ln:
            continue
        parts = ln.split()
        if len(parts) < 9:
            continue
        if parts[0].upper() == "GROUP" and parts[1].upper() == "TOPIC":
            continue
        g, topic = parts[0], parts[1]
        if not parts[2].isdigit():
            continue
        try:
            int(parts[5])
        except (ValueError, IndexError):
            continue
        topics.setdefault(g, set()).add(topic)
    return topics


def topic_total_kb_from_du_stdout(stdout: str) -> dict[str, int]:
    """
    Sum ``du -sk`` kilobytes per Kafka log directory topic (same basis as
    ``aggregate_du_kafka_data_by_topic`` / leadership balance).
    """
    per_part = parse_du_kafka_data_dirs(stdout)
    totals: dict[str, int] = {}
    for row in per_part[1:]:
        if len(row) < 3:
            continue
        topic, sk = row[0], row[2]
        if not str(sk).isdigit():
            continue
        totals[topic] = totals.get(topic, 0) + int(sk)
    return totals


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


def _iter_describe_partitions(describe_stdout: str) -> list[dict[str, Any]]:
    """
    Walk ``kafka-topics --describe`` output in order: topic summary lines set RF,
    partition lines attach to the current topic.
    """
    out: list[dict[str, Any]] = []
    cur_rf = 0
    for raw in (describe_stdout or "").splitlines():
        ln = raw.strip()
        if not ln:
            continue
        tm = _TOPIC_SUMMARY_RE.match(ln)
        if tm:
            try:
                cur_rf = int(tm.group("ReplicationFactor"))
            except (TypeError, ValueError):
                cur_rf = 0
            continue
        pm = _PART_LINE_RE.search(ln)
        if not pm:
            continue
        topic = pm.group("topic")
        leader = pm.group("leader").strip()
        replicas = pm.group("replicas")
        rep_set = {b.strip() for b in replicas.split(",") if re.fullmatch(r"\d+", b.strip())}
        out.append(
            {
                "topic": topic,
                "partition": pm.group("partition"),
                "leader": leader if re.fullmatch(r"\d+", leader) else "",
                "replicas": rep_set,
                "rf": cur_rf,
            }
        )
    return out


def build_leader_balance_insights(
    describe_stdout: str,
    *,
    broker_led: dict[str, int] | None = None,
    topic_sizes_mb: dict[str, str] | None = None,
    skew_limit: int = 40,
    rf1_exclusion_limit: int = 30,
) -> dict[str, Any]:
    """
    Explain uneven ``Partitions led`` totals: eligibility (replica placement) vs true skew.

    Returns JSON-serializable keys (omit empty):
    - ``leader_insights``: ``{ "bullets": [...], "focus_broker": str }``
    - ``leader_metrics_table``: per-broker metrics
    - ``leader_rf1_exclusion_table``: largest RF=1 topics where focus broker never appears in replicas
    - ``leader_skew_table``: topic rows with RF + replica-union context (replaces plain skew-only table)
    """
    topic_sizes = topic_sizes_mb or {}
    parts = _iter_describe_partitions(describe_stdout)
    if not parts:
        return {}

    all_brokers: set[str] = set()
    for p in parts:
        all_brokers |= p["replicas"]
        if p["leader"]:
            all_brokers.add(p["leader"])
    led_pre = broker_led or {}
    for b in led_pre:
        bs = str(b).strip()
        if re.fullmatch(r"\d+", bs):
            all_brokers.add(bs)
    broker_ids = sorted(all_brokers, key=int)

    eligible: dict[str, int] = {b: 0 for b in broker_ids}
    led_from_describe: dict[str, int] = {b: 0 for b in broker_ids}
    rf1_not_in_replica: dict[str, int] = {b: 0 for b in broker_ids}
    rf1_total = 0

    for p in parts:
        rf = int(p.get("rf") or 0)
        reps: set[str] = p["replicas"]
        for b in broker_ids:
            if b in reps:
                eligible[b] += 1
        ld = p.get("leader") or ""
        if ld in led_from_describe:
            led_from_describe[ld] += 1
        if rf == 1:
            rf1_total += 1
            for b in broker_ids:
                if b not in reps:
                    rf1_not_in_replica[b] += 1

    led: dict[str, int] = {}
    if broker_led:
        for k, v in broker_led.items():
            ks = str(k).strip()
            if re.fullmatch(r"\d+", ks):
                try:
                    led[ks] = int(v)
                except (TypeError, ValueError):
                    led[ks] = 0
    if not led:
        led = dict(led_from_describe)
    else:
        for b in broker_ids:
            led.setdefault(b, int(led_from_describe.get(b, 0)))

    total_parts = len(parts)
    focus_broker = min(broker_ids, key=lambda bid: int(led.get(bid, 0)))

    approx_mb_led: dict[str, float] = {b: 0.0 for b in broker_ids}
    by_topic: dict[str, dict[str, Any]] = {}
    for p in parts:
        t = p["topic"]
        row = by_topic.setdefault(t, {"parts": 0, "rf": 0, "replicas": set(), "leader_counts": {}})
        row["parts"] += 1
        row["rf"] = int(p.get("rf") or 0)
        row["replicas"] |= p["replicas"]
        ld = p.get("leader") or ""
        if ld:
            lc = row["leader_counts"]
            lc[ld] = lc.get(ld, 0) + 1

    for t, row in by_topic.items():
        prt = int(row["parts"] or 0)
        if prt <= 0:
            continue
        sz_raw = topic_sizes.get(t, "")
        sz = 0.0
        if isinstance(sz_raw, str) and re.fullmatch(r"\d+(\.\d+)?", sz_raw.strip()):
            sz = float(sz_raw.strip())
        lc: dict[str, int] = row.get("leader_counts") or {}
        for b in broker_ids:
            c = int(lc.get(b, 0))
            approx_mb_led[b] += sz * (float(c) / float(prt))

    summary = _build_leader_balance_summary(
        broker_ids=broker_ids,
        total_parts=total_parts,
        rf1_total=rf1_total,
        led=led,
        approx_mb_led=approx_mb_led,
        rf1_not_in_replica=rf1_not_in_replica,
        by_topic=by_topic,
        topic_sizes=topic_sizes,
    )

    hdr_metrics = [
        "Broker",
        "Partitions led (from describe counts)",
        "Eligible partitions (in replica set)",
        "% of cluster partitions eligible",
        "% of eligible partitions where this broker is leader",
        "RF=1 partitions never placing a replica on this broker",
        "Approx MB as leader (topic size × leader share)",
    ]
    metrics_rows: list[list[str]] = []
    for b in broker_ids:
        el = eligible.get(b, 0)
        lbc = int(led.get(b, 0))
        pct_clu = (100.0 * float(el) / float(total_parts)) if total_parts > 0 else 0.0
        pct_led = (100.0 * float(lbc) / float(el)) if el > 0 else 0.0
        metrics_rows.append(
            [
                b,
                str(lbc),
                str(el),
                f"{pct_clu:.1f}",
                f"{pct_led:.1f}",
                str(rf1_not_in_replica.get(b, 0)),
                f"{approx_mb_led[b]:.2f}",
            ]
        )

    rf1_topics: list[tuple[int, str, str, str]] = []
    for t, row in by_topic.items():
        if int(row.get("rf") or 0) != 1:
            continue
        reps: set[str] = row.get("replicas") or set()
        if focus_broker in reps:
            continue
        prt = int(row["parts"] or 0)
        rep_s = ",".join(sorted(reps, key=int))
        sz = topic_sizes.get(t, "N/A")
        rf1_topics.append((prt, t, rep_s, str(sz)))
    rf1_topics.sort(key=lambda x: (-x[0], x[1].casefold()))
    rf1_tbl: list[list[str]] = []
    if rf1_topics:
        rf1_hdr = [
            "Topic (RF=1, broker "
            + focus_broker
            + " never in replicas)",
            "Partitions",
            "Replica broker(s) used",
            "Size (MB)",
        ]
        rf1_tbl = [rf1_hdr] + [
            [tp, str(pc), rs, sz] for pc, tp, rs, sz in rf1_topics[: max(1, int(rf1_exclusion_limit))]
        ]

    skew_tbl = parse_topic_leader_skew(
        describe_stdout,
        topic_sizes_mb=topic_sizes,
        min_partitions=2,
        limit=int(skew_limit),
    )
    skew_enhanced: list[list[str]] = []
    if skew_tbl:
        old_hdr = skew_tbl[0]
        new_hdr = (
            ["Topic", "Partitions", "RF", "Replica brokers (union)"]
            + old_hdr[2:]
            if len(old_hdr) >= 7
            else old_hdr
        )
        skew_enhanced.append(new_hdr)
        for r in skew_tbl[1:]:
            if len(r) < 7:
                continue
            topic, pcount, mx, mn, delta, pct, sz = r[0], r[1], r[2], r[3], r[4], r[5], r[6]
            tr = by_topic.get(topic) or {}
            rf_s = str(int(tr.get("rf") or 0))
            rep_u = ",".join(sorted(tr.get("replicas") or set(), key=int)) if tr else ""
            skew_enhanced.append([topic, pcount, rf_s, rep_u, mx, mn, delta, pct, sz])

    out: dict[str, Any] = {
        "leader_insights": {"focus_broker": focus_broker},
        "leader_summary": summary,
        "leader_metrics_table": [hdr_metrics] + metrics_rows,
    }
    if rf1_tbl:
        out["leader_rf1_exclusion_table"] = rf1_tbl
    if skew_enhanced:
        out["leader_skew_table"] = skew_enhanced
    return out


def _fmt_size_mb(mb: float) -> str:
    """Render an approximate MB number with GB rollup when large; whole numbers when small."""
    if mb >= 1024.0:
        return f"{mb / 1024.0:.1f} GB"
    if mb >= 10.0:
        return f"{mb:.0f} MB"
    return f"{mb:.1f} MB"


def _topic_size_mb_float(topic_sizes: dict[str, str], topic: str) -> float:
    """Read the size cell from the rebalance scan; returns 0.0 when not parseable."""
    raw = topic_sizes.get(topic, "")
    if not isinstance(raw, str):
        return 0.0
    s = raw.strip()
    m = re.match(r"\d+(\.\d+)?", s)
    if not m:
        return 0.0
    try:
        return float(m.group(0))
    except (TypeError, ValueError):
        return 0.0


def _topic_cluster_imbalance_pct(partitions: int, leader_counts_per_cluster_broker: list[int]) -> float:
    """Cluster-wide leader imbalance for a single topic, expressed as percent of partitions.

    Compares the topic's actual leader counts (one entry per cluster broker, including
    zero for brokers not in the replica set) against the most balanced possible
    distribution of ``partitions`` partitions across ``N`` brokers (q = partitions // N
    on N-r brokers and q+1 on r brokers, where r = partitions % N). The metric is the
    sum of absolute deviations between the sorted actual and sorted ideal vectors,
    halved (so it counts each misplaced partition once), then normalized by partitions.

    Returns 0.0 for a perfectly spread topic and approaches 100% when all partitions
    are pinned to a single broker on a multi-broker cluster. Single-partition topics
    always score 0% because the ideal distribution is also "all on one broker".
    """

    if partitions <= 0 or not leader_counts_per_cluster_broker:
        return 0.0
    n = len(leader_counts_per_cluster_broker)
    if n <= 1:
        return 0.0
    q, r = divmod(partitions, n)
    ideal_sorted = [q] * (n - r) + [q + 1] * r
    actual_sorted = sorted(int(x) for x in leader_counts_per_cluster_broker)
    displacement = sum(abs(a - i) for a, i in zip(actual_sorted, ideal_sorted)) / 2.0
    return 100.0 * displacement / float(partitions)


def _build_leader_balance_summary(
    *,
    broker_ids: list[str],
    total_parts: int,
    rf1_total: int,
    led: dict[str, int],
    approx_mb_led: dict[str, float],
    rf1_not_in_replica: dict[str, int],
    by_topic: dict[str, dict[str, Any]],
    topic_sizes: dict[str, str],
) -> dict[str, Any]:
    """
    Compact leader-balance verdict split into three independent drivers.

    Returns ``{ "broker_count": int, "broker_ids": [...], "drivers": [...] }``.

    Each driver is::

        {
          "id": "topic_spread" | "partition_count" | "rf1",
          "label": "...",
          "status": "balanced" | "mildly-skewed" | "heavily-skewed" |
                    "single-broker" | "unknown",
          "headline": "short status sentence",
          "table": [[ "header...", ... ], [ "row...", ... ], ...],   # optional
          "table_caption": "...",                                    # optional
        }

    Generic for any broker count ≥ 1.
    """
    n = len(broker_ids)
    if n == 0:
        return {
            "broker_count": 0,
            "broker_ids": [],
            "drivers": [
                {
                    "id": "topic_spread",
                    "label": "Largest topics spread",
                    "status": "unknown",
                    "headline": "No brokers detected in describe output.",
                },
                {
                    "id": "partition_count",
                    "label": "Partition count spread",
                    "status": "unknown",
                    "headline": "No brokers detected in describe output.",
                },
                {
                    "id": "rf1",
                    "label": "RF=1 topics",
                    "status": "unknown",
                    "headline": "No brokers detected in describe output.",
                },
            ],
        }

    drivers: list[dict[str, Any]] = []

    # ---------- (a) Largest topics spread ----------
    # Cluster-wide imbalance per topic: compare actual leader counts (across ALL N
    # cluster brokers, including 0 for brokers not in the replica set) vs the most
    # balanced possible distribution of `partitions` across N brokers. Weight the
    # status by topic size so a few huge skewed topics correctly drive the verdict.
    largest_with_size: list[tuple[float, str, dict[str, Any]]] = []
    for t, row in by_topic.items():
        sz = _topic_size_mb_float(topic_sizes, t)
        if sz <= 0:
            continue
        largest_with_size.append((sz, t, row))
    largest_with_size.sort(key=lambda x: -x[0])
    top_n = largest_with_size[:10]

    ts_status = "balanced"
    ts_headline: str
    ts_table: list[list[str]] = []
    ts_caption = ""

    if n == 1:
        ts_status = "single-broker"
        ts_headline = (
            "Single-broker cluster — leader spread does not apply (every partition is led by the only broker)."
        )
    elif not top_n:
        ts_status = "unknown"
        ts_headline = (
            "Topic sizes are not available yet — run the Rebalancing Helper scan below to populate disk usage."
        )
    else:
        ts_table = [["Topic", "Size", "Partitions", "Replica brokers", "Imbalance %", "Misplaced size"]]
        rows_for_sort: list[tuple[float, list[str]]] = []
        total_size_mb = 0.0
        total_misplaced_mb = 0.0
        for sz, t, row in top_n:
            prt = int(row.get("parts") or 0)
            reps = row.get("replicas") or set()
            lc: dict[str, int] = row.get("leader_counts") or {}
            counts_per_cluster = [int(lc.get(b, 0)) for b in broker_ids]
            imb_pct = _topic_cluster_imbalance_pct(prt, counts_per_cluster)
            misplaced_mb = sz * imb_pct / 100.0
            total_size_mb += sz
            total_misplaced_mb += misplaced_mb
            rep_label = (
                ",".join(sorted(reps, key=lambda x: int(x) if str(x).isdigit() else 0)) if reps else ""
            )
            rows_for_sort.append(
                (
                    sz,
                    [
                        t,
                        _fmt_size_mb(sz),
                        str(prt),
                        rep_label,
                        f"{imb_pct:.1f}",
                        _fmt_size_mb(misplaced_mb) if misplaced_mb > 0 else "0",
                    ],
                )
            )
        rows_for_sort.sort(key=lambda x: -x[0])
        for _, r in rows_for_sort:
            ts_table.append(r)

        misplaced_pct = (100.0 * total_misplaced_mb / total_size_mb) if total_size_mb > 0 else 0.0
        ts_caption = (
            f"Top {len(top_n)} topic(s) by size. Imbalance % = how far the topic's leader distribution "
            f"is from the most-even split across all {n} cluster brokers (0% = perfectly spread). "
            f"Misplaced MB = Size × Imbalance %."
        )
        if misplaced_pct <= 5.0:
            ts_status = "balanced"
            ts_headline = (
                f"~{misplaced_pct:.1f}% of top-{len(top_n)} topic bytes are misplaced — "
                f"the largest topics are well-spread across all {n} brokers."
            )
        elif misplaced_pct <= 20.0:
            ts_status = "mildly-skewed"
            ts_headline = (
                f"~{misplaced_pct:.1f}% of top-{len(top_n)} topic bytes are misplaced "
                f"(~{_fmt_size_mb(total_misplaced_mb)} of {_fmt_size_mb(total_size_mb)})."
            )
        else:
            ts_status = "heavily-skewed"
            ts_headline = (
                f"~{misplaced_pct:.1f}% of top-{len(top_n)} topic bytes are misplaced "
                f"(~{_fmt_size_mb(total_misplaced_mb)} of {_fmt_size_mb(total_size_mb)}) — "
                f"the largest topics are not spread evenly across all {n} brokers."
            )

    drivers.append(
        {
            "id": "topic_spread",
            "label": "Largest topics spread",
            "status": ts_status,
            "headline": ts_headline,
            "table": ts_table,
            "table_caption": ts_caption,
        }
    )

    # ---------- (b) Partition count spread ----------
    counts_list = [int(led.get(b, 0)) for b in broker_ids]
    total_led = sum(counts_list)
    mean_count = (total_led / n) if n > 0 else 0.0
    mx_count = max(counts_list) if counts_list else 0
    mn_count = min(counts_list) if counts_list else 0
    counts_spread_pct = (100.0 * (mx_count - mn_count) / mean_count) if mean_count > 0 else 0.0
    max_b_count = max(broker_ids, key=lambda b: int(led.get(b, 0)))
    min_b_count = min(broker_ids, key=lambda b: int(led.get(b, 0)))

    pc_table: list[list[str]] = [["Broker", "Partitions led", "% of cluster"]]
    pc_rows: list[tuple[int, list[str]]] = []
    for b in broker_ids:
        c = int(led.get(b, 0))
        pct = (100.0 * c / float(total_parts)) if total_parts > 0 else 0.0
        pc_rows.append((c, [b, f"{c:,}", f"{pct:.1f}"]))
    pc_rows.sort(key=lambda x: -x[0])
    for _, row in pc_rows:
        pc_table.append(row)

    if n == 1:
        pc_status = "single-broker"
        pc_headline = "Single-broker cluster — partition count spread does not apply."
    elif counts_spread_pct <= 30.0:
        pc_status = "balanced"
        pc_headline = (
            f"Partition count spread Δ {counts_spread_pct:.1f}% — even across {n} brokers "
            f"(broker {max_b_count}: {int(led.get(max_b_count, 0)):,}, broker {min_b_count}: "
            f"{int(led.get(min_b_count, 0)):,})."
        )
    elif counts_spread_pct <= 100.0:
        pc_status = "mildly-skewed"
        pc_headline = (
            f"Partition count spread Δ {counts_spread_pct:.1f}% — broker {max_b_count} leads "
            f"{int(led.get(max_b_count, 0)):,} partitions vs broker {min_b_count}'s "
            f"{int(led.get(min_b_count, 0)):,}. Cosmetic unless monitoring shows hot CPU / disk on broker "
            f"{max_b_count}."
        )
    else:
        pc_status = "heavily-skewed"
        pc_headline = (
            f"Partition count spread Δ {counts_spread_pct:.1f}% — broker {max_b_count} leads "
            f"{int(led.get(max_b_count, 0)):,} partitions vs broker {min_b_count}'s "
            f"{int(led.get(min_b_count, 0)):,}. Use the Rebalancing Helper to even this out."
        )

    drivers.append(
        {
            "id": "partition_count",
            "label": "Partition count spread",
            "status": pc_status,
            "headline": pc_headline,
            "table": pc_table,
            "table_caption": "Partitions led per broker (from describe).",
        }
    )

    # ---------- (c) RF=1 topics ----------
    rf1_pinned: list[tuple[float, str, str, int]] = []
    rf1_topics_total = 0
    rf1_pinned_parts = 0
    for t, row in by_topic.items():
        if int(row.get("rf") or 0) != 1:
            continue
        rf1_topics_total += 1
        reps = row.get("replicas") or set()
        if len(reps) == 1:
            only_b = next(iter(reps))
            sz = _topic_size_mb_float(topic_sizes, t)
            prt = int(row.get("parts") or 0)
            rf1_pinned.append((sz, t, str(only_b), prt))
            rf1_pinned_parts += prt
    rf1_pinned.sort(key=lambda x: -x[0])

    rf1_table: list[list[str]] = []
    rf1_caption = ""
    if rf1_pinned:
        rf1_table = [["Topic", "Partitions", "Size", "Pinned to broker"]]
        for sz, t, b, prt in rf1_pinned[:30]:
            rf1_table.append([t, str(prt), _fmt_size_mb(sz) if sz > 0 else "N/A", b])
        rf1_caption = (
            f"RF=1 topics with all replicas on a single broker — Kafka cannot auto-rebalance these."
        )

    pinned_pct = (100.0 * rf1_pinned_parts / float(total_parts)) if total_parts > 0 else 0.0
    if rf1_topics_total == 0:
        rf1_status = "balanced"
        rf1_headline = "No RF=1 topics in this cluster — replication is in place everywhere."
    elif not rf1_pinned:
        rf1_status = "balanced"
        rf1_headline = (
            f"{rf1_topics_total:,} RF=1 topic(s) exist but each spans multiple brokers — Kafka can still "
            f"reassign leadership."
        )
    elif pinned_pct <= 5.0:
        rf1_status = "mildly-skewed"
        rf1_headline = (
            f"{len(rf1_pinned):,} RF=1 topic(s) pinned to single brokers ({rf1_pinned_parts:,} partitions, "
            f"{pinned_pct:.1f}% of cluster). Manual reassignment needed if you want to rebalance these."
        )
    else:
        rf1_status = "heavily-skewed"
        rf1_headline = (
            f"{len(rf1_pinned):,} RF=1 topic(s) pinned to single brokers ({rf1_pinned_parts:,} partitions, "
            f"{pinned_pct:.1f}% of cluster). This is the main driver of partition-count skew — raise RF ≥ 2 "
            f"or move them with the Rebalancing Helper."
        )

    drivers.append(
        {
            "id": "rf1",
            "label": "RF=1 topics",
            "status": rf1_status,
            "headline": rf1_headline,
            "table": rf1_table,
            "table_caption": rf1_caption,
        }
    )

    return {
        "broker_count": n,
        "broker_ids": list(broker_ids),
        "drivers": drivers,
    }


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
