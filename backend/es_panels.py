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
    "es_cat_shards": "curl -sS http://127.0.0.1:9200/_cat/shards?v",
    "es_cluster_stats": "curl -sS http://127.0.0.1:9200/_cluster/stats?pretty&human",
    "es_nodes_stats": "curl -sS http://127.0.0.1:9200/_nodes/stats?pretty&human",
    "es_cat_pending_tasks": "curl -sS http://127.0.0.1:9200/_cat/pending_tasks?v",
    "es_cluster_pending_tasks": "curl -sS http://127.0.0.1:9200/_cluster/pending_tasks?pretty",
    "es_cat_thread_pool": "curl -sS http://127.0.0.1:9200/_cat/thread_pool?v",
    "es_ilm_status": "curl -sS http://127.0.0.1:9200/_ilm/status?pretty",
    "es_cat_templates": "curl -sS http://127.0.0.1:9200/_cat/templates?v&s=name",
    "es_cat_aliases": "curl -sS http://127.0.0.1:9200/_cat/aliases?v",
    "es_cluster_state_metadata": "curl -sS 'http://127.0.0.1:9200/_cluster/state/metadata?pretty&filter_path=metadata.cluster_uuid,metadata.templates,metadata.index_templates'",
    "es_nodes_info": "curl -sS http://127.0.0.1:9200/_nodes?pretty",
}

ES_PANEL_IDS: Final[frozenset[str]] = frozenset(ES_PANEL_CMD_HINT.keys())

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
        return "GET", f"/_cat/indices/{quote(pat, safe='*,-._~')}?pretty", None

    if panel == "es_cat_shards":
        return "GET", "/_cat/shards?v", None
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
        return "GET", "/_ilm/status?pretty", None
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
