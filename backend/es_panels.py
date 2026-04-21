"""Predefined Elasticsearch HTTP panels (paths relative to http://127.0.0.1:9200)."""

from __future__ import annotations

import json
import re
from typing import Any, Final
from urllib.parse import quote

ES_PANEL_CMD_HINT: dict[str, str] = {
    "es_root": "curl -sS http://127.0.0.1:9200/",
    "es_cluster_health": "curl -sS http://127.0.0.1:9200/_cluster/health?pretty",
    "es_cat_nodes": "curl -sS http://127.0.0.1:9200/_cat/nodes?v",
    "es_cluster_settings": "curl -sS http://127.0.0.1:9200/_cluster/settings?pretty&flat_settings=true",
    "es_cat_allocation": "curl -sS http://127.0.0.1:9200/_cat/allocation?v",
    "es_allocation_explain": "curl -sS -X POST http://127.0.0.1:9200/_cluster/allocation/explain?pretty -H Content-Type: application/json -d '{...}'",
    "es_cat_indices": "curl -sS 'http://127.0.0.1:9200/_cat/indices?format=json' then filter by health/state/name in server",
    "es_cat_indices_named": "curl -sS http://127.0.0.1:9200/_cat/indices/<index>?pretty",
    "es_cat_shards": "curl -sS http://127.0.0.1:9200/_cat/shards?format=json&bytes=b",
    "es_cat_shard_stores": "curl -sS http://127.0.0.1:9200/_cat/shard_stores?format=json&bytes=b",
    "es_cluster_health_shards": "curl -sS http://127.0.0.1:9200/_cluster/health?level=shards&pretty",
    "es_cat_recovery": "curl -sS http://127.0.0.1:9200/_cat/recovery?format=json&active_only=true",
    "es_cat_shards_named": "curl -sS http://127.0.0.1:9200/_cat/shards/<index>?format=json&bytes=b",
    "es_cluster_stats": "curl -sS http://127.0.0.1:9200/_cluster/stats?pretty&human",
    "es_nodes_stats": "curl -sS http://127.0.0.1:9200/_nodes/stats?pretty&human",
    "es_cat_pending_tasks": "curl -sS http://127.0.0.1:9200/_cat/pending_tasks?v",
    "es_cluster_pending_tasks": "curl -sS http://127.0.0.1:9200/_cluster/pending_tasks?pretty",
    "es_cat_thread_pool": "curl -sS http://127.0.0.1:9200/_cat/thread_pool?v",
    "es_ilm_status": "curl -sS -X POST http://127.0.0.1:9200/_ilm/status?pretty -H Content-Type: application/json -d {}",
    "es_cat_templates": "curl -sS http://127.0.0.1:9200/_cat/templates?v&s=name",
    "es_cat_aliases": "curl -sS http://127.0.0.1:9200/_cat/aliases?v",
    "es_cluster_state_metadata": "curl -sS 'http://127.0.0.1:9200/_cluster/state/metadata?pretty&filter_path=metadata.cluster_uuid,metadata.templates,metadata.index_templates'",
    "es_nodes_info": "curl -sS http://127.0.0.1:9200/_nodes?pretty",
}

ES_PANEL_IDS: Final[frozenset[str]] = frozenset(ES_PANEL_CMD_HINT.keys())

OS_PANEL_CMD_HINT: dict[str, str] = {
    k.replace("es_", "os_", 1): v.replace("127.0.0.1:9200", "127.0.0.1:9200")
    for k, v in ES_PANEL_CMD_HINT.items()
}
OS_PANEL_IDS: Final[frozenset[str]] = frozenset(OS_PANEL_CMD_HINT.keys())


def os_panel_to_es_panel(panel: str) -> str:
    """Map UI / API ``os_*`` ids to shared ``es_request_for_panel`` ids."""
    if panel not in OS_PANEL_IDS:
        raise ValueError("unknown OpenSearch panel")
    if not panel.startswith("os_"):
        raise ValueError("OpenSearch panel id must start with os_")
    return "es_" + panel[3:]

_SAFE_INDEX_PATTERN = re.compile(r"^[A-Za-z0-9_.*\-]{1,200}$")
_IDX_SUB = re.compile(r"^[A-Za-z0-9_.*\-]{0,200}$")


def sanitize_index_pattern(raw: str, label: str = "Index pattern") -> str:
    s = (raw or "").strip()
    if not s:
        raise ValueError(f"{label} is required")
    if not _SAFE_INDEX_PATTERN.match(s):
        raise ValueError(f"{label}: use letters, digits, dot, underscore, hyphen, * only")
    return s


def sanitize_idx_substring(raw: str) -> str:
    s = (raw or "").strip()[:200]
    if not s:
        return ""
    if not _IDX_SUB.match(s):
        raise ValueError("Index name filter: letters, digits, . _ - * only")
    return s


def filter_cat_indices_rows(
    rows: list[dict[str, Any]],
    *,
    health: str,
    state: str,
    substring: str,
) -> list[dict[str, Any]]:
    """Filter ``/_cat/indices?format=json`` rows."""
    h = (health or "all").strip().lower()
    st = (state or "all").strip().lower()
    sub = (substring or "").strip().casefold()
    out: list[dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        idx = str(row.get("index") or row.get("i") or "")
        rh = str(row.get("health") or row.get("h") or "").lower()
        rs = str(row.get("status") or row.get("st") or "").lower()
        if h != "all" and rh != h:
            continue
        if st == "open" and rs != "open":
            continue
        if st == "close" and rs != "close":
            continue
        if sub and sub not in idx.casefold():
            continue
        out.append(row)
    return out


def postprocess_cat_indices(stdout: str, health: str, state: str, substring: str) -> str:
    """If stdout is JSON array from cat indices, filter and pretty-print; else return raw."""
    text = (stdout or "").strip()
    if not text:
        return stdout
    try:
        data = json.loads(text)
    except json.JSONDecodeError:
        return stdout
    if not isinstance(data, list):
        return stdout
    filtered = filter_cat_indices_rows(data, health=health, state=state, substring=substring)
    note = (
        f"# filtered rows: {len(filtered)} (health={health or 'all'}, state={state or 'all'}, "
        f"substring={substring or '(none)'})\n"
    )
    return note + json.dumps(filtered, indent=2, default=str)


def es_request_for_panel(
    panel: str,
    *,
    index_pattern: str = "",
    alloc_body: str = "",
) -> tuple[str, str, str | None]:
    """
    Returns ``(method, path_and_query, body)`` for ``elasticsearch_curl``.
    ``body`` is only non-None for POST.
    """
    if panel not in ES_PANEL_IDS:
        raise ValueError("unknown Elasticsearch panel")

    if panel == "es_root":
        return "GET", "/", None
    if panel == "es_cluster_health":
        return "GET", "/_cluster/health?pretty", None
    if panel == "es_cat_nodes":
        return "GET", "/_cat/nodes?v", None
    if panel == "es_cluster_settings":
        return "GET", "/_cluster/settings?pretty&flat_settings=true", None
    if panel == "es_cat_allocation":
        return "GET", "/_cat/allocation?v", None
    if panel == "es_allocation_explain":
        body = (alloc_body or "").strip() or "{}"
        json.loads(body)
        return "POST", "/_cluster/allocation/explain?pretty", body
    if panel == "es_cat_indices":
        return "GET", "/_cat/indices?format=json&bytes=b", None
    if panel == "es_cat_indices_named":
        pat = sanitize_index_pattern(index_pattern, "Index pattern")
        return "GET", f"/_cat/indices/{quote(pat, safe='*,-._~')}?format=json&bytes=b", None

    if panel == "es_cat_shards":
        return "GET", "/_cat/shards?format=json&bytes=b", None
    if panel == "es_cat_shard_stores":
        return "GET", "/_cat/shard_stores?format=json&bytes=b", None
    if panel == "es_cluster_health_shards":
        return "GET", "/_cluster/health?level=shards&pretty", None
    if panel == "es_cat_recovery":
        return "GET", "/_cat/recovery?format=json&active_only=true", None
    if panel == "es_cat_shards_named":
        pat = sanitize_index_pattern(index_pattern, "Index pattern")
        return "GET", f"/_cat/shards/{quote(pat, safe='*,-._~')}?format=json&bytes=b", None
    if panel == "es_cluster_stats":
        return "GET", "/_cluster/stats?pretty&human", None
    if panel == "es_nodes_stats":
        return "GET", "/_nodes/stats?pretty&human", None
    if panel == "es_cat_pending_tasks":
        return "GET", "/_cat/pending_tasks?v", None
    if panel == "es_cluster_pending_tasks":
        return "GET", "/_cluster/pending_tasks?pretty", None
    if panel == "es_cat_thread_pool":
        return "GET", "/_cat/thread_pool?v", None
    if panel == "es_ilm_status":
        return "POST", "/_ilm/status?pretty", "{}"
    if panel == "es_cat_templates":
        return "GET", "/_cat/templates?v&s=name", None
    if panel == "es_cat_aliases":
        return "GET", "/_cat/aliases?v", None
    if panel == "es_cluster_state_metadata":
        return (
            "GET",
            "/_cluster/state/metadata?pretty&filter_path=metadata.cluster_uuid,metadata.templates,metadata.index_templates",
            None,
        )
    if panel == "es_nodes_info":
        return "GET", "/_nodes?pretty", None

    raise ValueError("unknown panel")


def sanitize_shards_substring(raw: str) -> str:
    """Optional client filter for shard / recovery tables (same charset as index substring)."""
    return sanitize_idx_substring(raw)


def _json_array_from_stdout(stdout: str) -> list[Any] | None:
    t = (stdout or "").strip()
    if not t:
        return None
    i = t.find("[")
    if i < 0:
        try:
            v = json.loads(t)
        except json.JSONDecodeError:
            return None
        return v if isinstance(v, list) else None
    try:
        v = json.loads(t[i:])
    except json.JSONDecodeError:
        return None
    return v if isinstance(v, list) else None


def filter_rows_by_substring(rows: list[list[str]], needle: str, *, columns: list[int] | None = None) -> list[list[str]]:
    """Keep header row 0; filter data rows where any selected column contains needle (casefold)."""
    n = (needle or "").strip().casefold()
    if not n or len(rows) < 2:
        return rows
    cols = columns if columns is not None else list(range(len(rows[0])))
    out: list[list[str]] = [rows[0]]
    for r in rows[1:]:
        hit = any(n in str(r[c] if c < len(r) else "").casefold() for c in cols)
        if hit:
            out.append(r)
    return out


def table_from_cat_indices_json_array(data: list[Any]) -> list[list[str]] | None:
    if not data:
        return None
    hdr = ["Index", "Health", "Status", "Docs", "Store size"]
    rows: list[list[str]] = [hdr]
    for row in data:
        if not isinstance(row, dict):
            continue
        idx = str(row.get("index") or row.get("i") or "")
        rows.append(
            [
                idx,
                str(row.get("health") or row.get("h") or ""),
                str(row.get("status") or row.get("st") or ""),
                str(row.get("docs.count") or row.get("dc") or ""),
                str(row.get("store.size") or row.get("ss") or ""),
            ]
        )
    return rows if len(rows) > 1 else None


def table_from_cat_indices_stdout(stdout: str) -> list[list[str]] | None:
    data = _json_array_from_stdout(stdout)
    if not data:
        return None
    return table_from_cat_indices_json_array(data)


def table_from_cat_shards_stdout(stdout: str, substring: str = "") -> list[list[str]] | None:
    data = _json_array_from_stdout(stdout)
    if not data:
        return None
    hdr = ["Index", "Shard", "Pri/rep", "State", "Docs", "Store"]
    rows: list[list[str]] = [hdr]
    for row in data:
        if not isinstance(row, dict):
            continue
        r = [
            str(row.get("index") or row.get("i") or ""),
            str(row.get("shard") or row.get("s") or ""),
            str(row.get("prirep") or row.get("p") or ""),
            str(row.get("state") or row.get("st") or ""),
            str(row.get("docs") or row.get("d") or ""),
            str(row.get("store") or ""),
        ]
        rows.append(r)
    if len(rows) < 2:
        return None
    return filter_rows_by_substring(rows, substring, columns=[0, 1, 2, 3, 4, 5])


def table_from_cat_recovery_stdout(stdout: str, substring: str = "") -> list[list[str]] | None:
    data = _json_array_from_stdout(stdout)
    if not data:
        return None
    hdr = ["Index", "Shard", "Stage", "Time", "Type"]
    rows: list[list[str]] = [hdr]
    for row in data:
        if not isinstance(row, dict):
            continue
        rows.append(
            [
                str(row.get("index") or ""),
                str(row.get("shard") or ""),
                str(row.get("stage") or ""),
                str(row.get("time") or ""),
                str(row.get("type") or ""),
            ]
        )
    if len(rows) < 2:
        return None
    return filter_rows_by_substring(rows, substring, columns=[0, 1, 2, 3, 4])


def table_from_cat_shard_stores_stdout(stdout: str, substring: str = "") -> list[list[str]] | None:
    data = _json_array_from_stdout(stdout)
    if not data:
        return None
    hdr = ["Index", "Shard", "Nodename", "Store", "Ephemeral"]
    rows: list[list[str]] = [hdr]
    for row in data:
        if not isinstance(row, dict):
            continue
        rows.append(
            [
                str(row.get("index") or ""),
                str(row.get("shard") or ""),
                str(row.get("nodename") or row.get("node") or ""),
                str(row.get("store") or ""),
                str(row.get("ephemeral") or ""),
            ]
        )
    if len(rows) < 2:
        return None
    return filter_rows_by_substring(rows, substring, columns=[0, 1, 2, 3, 4])
