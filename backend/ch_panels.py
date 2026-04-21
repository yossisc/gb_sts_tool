"""Predefined ClickHouse panel SQL (parameterized time range + event table)."""

from __future__ import annotations

import re
import shlex
from typing import Final

_SAFE_EVENT = re.compile(r"^(beacon_event|mobile_event)$")
_TENANT_UUID = re.compile(r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$")
_TS_FREE = re.compile(r"^[0-9 T:\+\-\.Z]{8,48}$")
_CH_DB = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]{0,62}$")


def _ch_string_literal(val: str) -> str:
    """Single-quoted SQL string literal for ClickHouse (``shlex.quote`` is for shells, not SQL)."""
    return "'" + (val or "").replace("\\", "\\\\").replace("'", "\\'") + "'"


CH_PANEL_CMD_HINT: dict[str, str] = {
    "ch_max_session_ts": "clickhouse-client --password … -q 'SELECT toDateTime(max(session_ts)) …'",
    "ch_events_sessions_by_hour": "clickhouse-client … -q 'SELECT count(*) … GROUP BY day, hour …'",
    "ch_system_clusters": "clickhouse-client … -q \"SELECT * FROM system.clusters WHERE cluster='analytics'\"",
    "ch_system_processes": "clickhouse-client … -q 'SELECT … FROM system.processes …'",
    "ch_beacon_tenant_hour": "clickhouse-client … -q '… beacon_event … tenant_id = …'",
    "ch_mobile_by_hour": "clickhouse-client … -q '… mobile_event …'",
    "ch_beacon_by_hour": "clickhouse-client … -q '… beacon_event …'",
    "ch_explore_databases": "clickhouse-client … -q 'SELECT name, engine FROM system.databases …'",
    "ch_explore_tables": "clickhouse-client … -q 'SELECT … FROM system.tables WHERE database = …'",
}

CH_PANEL_IDS: Final[frozenset[str]] = frozenset(CH_PANEL_CMD_HINT.keys())


def qualified_table(event_table: str) -> str:
    if not _SAFE_EVENT.match((event_table or "").strip()):
        raise ValueError("event_table must be beacon_event or mobile_event")
    return f"glassbox.{event_table.strip()}"


def time_predicate(hours: int, from_ts: str, to_ts: str) -> str:
    """Default: last N hours on ``session_ts``; optional absolute window."""
    f = (from_ts or "").strip()
    t = (to_ts or "").strip()
    if f and t:
        if not _TS_FREE.match(f) or not _TS_FREE.match(t):
            raise ValueError("from_ts / to_ts: use safe ISO-like timestamps only")
        return (
            "session_ts >= parseDateTimeBestEffort("
            + shlex.quote(f)
            + ") AND session_ts < parseDateTimeBestEffort("
            + shlex.quote(t)
            + ")"
        )
    h = int(hours) if hours else 24
    h = max(1, min(h, 336))
    return f"session_ts >= now() - toIntervalHour({h})"


def sanitize_tenant_id(raw: str) -> str:
    v = (raw or "").strip()
    if not v:
        raise ValueError("tenant_id is required for this panel")
    if not _TENANT_UUID.match(v):
        raise ValueError("tenant_id must be a UUID")
    return v


def sanitize_clickhouse_db_name(raw: str) -> str:
    d = (raw or "").strip() or "glassbox"
    if not _CH_DB.match(d):
        raise ValueError("invalid database name")
    return d


def sql_for_panel(
    panel: str,
    *,
    event_table: str = "beacon_event",
    hours: int = 24,
    from_ts: str = "",
    to_ts: str = "",
    tenant_id: str = "",
    database: str = "",
) -> str:
    if panel not in CH_PANEL_IDS:
        raise ValueError("unknown ClickHouse panel")

    if panel == "ch_explore_databases":
        return "SELECT name AS database, engine FROM system.databases ORDER BY name"

    if panel == "ch_explore_tables":
        db = sanitize_clickhouse_db_name(database or "glassbox")
        dq = _ch_string_literal(db)
        return (
            "SELECT database, name AS table, engine, total_rows, "
            "formatReadableSize(total_bytes) AS readable_size "
            f"FROM system.tables WHERE database = {dq} ORDER BY name LIMIT 5000"
        )

    table = qualified_table(event_table)
    tw = time_predicate(hours, from_ts, to_ts)

    if panel == "ch_max_session_ts":
        return f"SELECT toDateTime(max(session_ts)) AS max_session_ts FROM {table}"

    if panel == "ch_events_sessions_by_hour":
        return (
            f"SELECT count(*) AS events, uniq(session_uuid) AS sessions, "
            f"toYYYYMMDD(toDateTime(session_ts)) AS day, toHour(toDateTime(session_ts)) AS hour "
            f"FROM {table} WHERE {tw} GROUP BY day, hour ORDER BY day DESC, hour DESC"
        )

    if panel == "ch_system_clusters":
        return "SELECT * FROM system.clusters WHERE cluster = 'analytics'"

    if panel == "ch_system_processes":
        return (
            "SELECT query_id, user, address, elapsed, query FROM system.processes ORDER BY query_id"
        )

    if panel == "ch_beacon_tenant_hour":
        if event_table != "beacon_event":
            raise ValueError("This panel is for beacon_event only")
        tid = sanitize_tenant_id(tenant_id)
        return (
            "SELECT count(*) AS cnt, host, count(DISTINCT session_uuid) AS uniq_sessions, "
            "toYYYYMMDD(toDateTime(session_ts, 'Etc/GMT')) AS day, "
            "toHour(toDateTime(session_ts, 'Etc/GMT')) AS hour "
            f"FROM {table} WHERE tenant_id = {shlex.quote(tid)} AND ({tw}) "
            "GROUP BY host, day, hour ORDER BY cnt DESC, hour ASC"
        )

    if panel == "ch_mobile_by_hour":
        if event_table != "mobile_event":
            raise ValueError("This panel is for mobile_event only")
        return (
            "SELECT count(*) AS cnt, count(DISTINCT session_uuid) AS uniq_sessions, "
            "toYYYYMMDD(toDateTime(session_ts, 'Etc/GMT')) AS day, "
            "toHour(toDateTime(session_ts, 'Etc/GMT')) AS hour "
            f"FROM {table} WHERE ({tw}) GROUP BY day, hour ORDER BY cnt DESC, hour ASC"
        )

    if panel == "ch_beacon_by_hour":
        if event_table != "beacon_event":
            raise ValueError("This panel is for beacon_event only")
        return (
            "SELECT count(*) AS cnt, count(DISTINCT session_uuid) AS uniq_sessions, "
            "toYYYYMMDD(toDateTime(session_ts)) AS day, toHour(toDateTime(session_ts)) AS hour "
            f"FROM {table} WHERE ({tw}) GROUP BY day, hour ORDER BY day DESC, hour ASC"
        )

    raise ValueError("unknown panel")
