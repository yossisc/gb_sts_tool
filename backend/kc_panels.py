"""Predefined Kafka Connect HTTP panels (paths relative to http://127.0.0.1:8083)."""

from __future__ import annotations

import json
import re
from typing import Any, Final
from urllib.parse import quote

KC_PANEL_CMD_HINT: dict[str, str] = {
    "kc_root": "curl -sS http://127.0.0.1:8083/",
    "kc_connectors": "curl -sS http://127.0.0.1:8083/connectors",
    "kc_connectors_status": "curl -sS 'http://127.0.0.1:8083/connectors?expand=status'",
    "kc_connectors_info": "curl -sS 'http://127.0.0.1:8083/connectors?expand=info'",
    "kc_connectors_full": "curl -sS 'http://127.0.0.1:8083/connectors?expand=status&expand=info'",
    "kc_connector_status": "curl -sS http://127.0.0.1:8083/connectors/<connector>/status",
    "kc_connector_config": "curl -sS http://127.0.0.1:8083/connectors/<connector>/config",
    "kc_connector_tasks": "curl -sS http://127.0.0.1:8083/connectors/<connector>/tasks",
    "kc_connector_topics": "curl -sS http://127.0.0.1:8083/connectors/<connector>/topics",
    "kc_plugins": "curl -sS http://127.0.0.1:8083/connector-plugins",
    "kc_admin_loggers": "curl -sS http://127.0.0.1:8083/admin/loggers",
}

KC_PANEL_IDS: Final[frozenset[str]] = frozenset(KC_PANEL_CMD_HINT.keys())

_SAFE_CONNECTOR = re.compile(r"^[A-Za-z0-9_.\-]{1,200}$")


def sanitize_connector_name(raw: str, label: str = "Connector") -> str:
    s = (raw or "").strip()
    if not s:
        raise ValueError(f"{label} is required")
    if not _SAFE_CONNECTOR.match(s):
        raise ValueError(f"{label}: use letters, digits, dot, underscore, hyphen only")
    return s


def request_for_panel(panel: str, *, connector: str = "") -> tuple[str, str]:
    """Returns ``(method, path_and_query)`` for Kafka Connect."""
    if panel not in KC_PANEL_IDS:
        raise ValueError("unknown Kafka Connect panel")
    if panel == "kc_root":
        return "GET", "/"
    if panel == "kc_connectors":
        return "GET", "/connectors"
    if panel == "kc_connectors_status":
        return "GET", "/connectors?expand=status"
    if panel == "kc_connectors_info":
        return "GET", "/connectors?expand=info"
    if panel == "kc_connectors_full":
        return "GET", "/connectors?expand=status&expand=info"
    if panel == "kc_plugins":
        return "GET", "/connector-plugins"
    if panel == "kc_admin_loggers":
        return "GET", "/admin/loggers"
    if panel in ("kc_connector_status", "kc_connector_config", "kc_connector_tasks", "kc_connector_topics"):
        name = sanitize_connector_name(connector)
        enc = quote(name, safe="-._~")
        if panel == "kc_connector_status":
            return "GET", f"/connectors/{enc}/status"
        if panel == "kc_connector_config":
            return "GET", f"/connectors/{enc}/config"
        if panel == "kc_connector_tasks":
            return "GET", f"/connectors/{enc}/tasks"
        return "GET", f"/connectors/{enc}/topics"
    raise ValueError("unknown panel")


def _status_bucket(raw: str) -> str:
    s = (raw or "").strip().upper()
    if s in ("RUNNING",):
        return "GREEN"
    if s in ("PAUSED", "UNASSIGNED", "RESTARTING"):
        return "YELLOW"
    if s in ("FAILED", "ERROR", "DESTROYED"):
        return "RED"
    return "YELLOW" if s else "YELLOW"


def _json_from_stdout(stdout: str) -> Any:
    t = (stdout or "").strip()
    if not t:
        return None
    return json.loads(t)


def _filter_rows(rows: list[list[str]], needle: str) -> list[list[str]]:
    n = (needle or "").strip().casefold()
    if not n or len(rows) < 2:
        return rows
    out = [rows[0]]
    for r in rows[1:]:
        if any(n in str(c).casefold() for c in r):
            out.append(r)
    return out


def _table_connectors(data: Any) -> list[list[str]] | None:
    if not isinstance(data, list):
        return None
    rows = [["Connector"]]
    for v in data:
        s = str(v or "").strip()
        if s:
            rows.append([s])
    return rows if len(rows) > 1 else None


def _table_connectors_status(data: Any) -> list[list[str]] | None:
    if not isinstance(data, dict):
        return None
    rows = [["Connector", "Type", "State", "Color", "Worker", "Tasks", "Running", "Failed"]]
    for name, body in sorted(data.items(), key=lambda kv: str(kv[0]).casefold()):
        if not isinstance(body, dict):
            continue
        st = body.get("status") if isinstance(body.get("status"), dict) else {}
        con = st.get("connector") if isinstance(st.get("connector"), dict) else {}
        tasks = st.get("tasks") if isinstance(st.get("tasks"), list) else []
        cstate = str(con.get("state") or "")
        color = _status_bucket(cstate)
        running = 0
        failed = 0
        for t in tasks:
            if not isinstance(t, dict):
                continue
            ts = str(t.get("state") or "").upper()
            if ts == "RUNNING":
                running += 1
            elif ts in ("FAILED", "ERROR"):
                failed += 1
        rows.append(
            [
                str(name),
                str(con.get("type") or ""),
                cstate,
                color,
                str(con.get("worker_id") or ""),
                str(len(tasks)),
                str(running),
                str(failed),
            ]
        )
    return rows if len(rows) > 1 else None


def _table_connectors_info(data: Any) -> list[list[str]] | None:
    if not isinstance(data, dict):
        return None
    rows = [["Connector", "Type", "Class", "Tasks(max)", "Topics", "State", "Color"]]
    for name, body in sorted(data.items(), key=lambda kv: str(kv[0]).casefold()):
        if not isinstance(body, dict):
            continue
        info = body.get("info") if isinstance(body.get("info"), dict) else {}
        cfg = info.get("config") if isinstance(info.get("config"), dict) else {}
        st = body.get("status") if isinstance(body.get("status"), dict) else {}
        con = st.get("connector") if isinstance(st.get("connector"), dict) else {}
        state = str(con.get("state") or "")
        rows.append(
            [
                str(name),
                str(info.get("type") or ""),
                str(cfg.get("connector.class") or ""),
                str(cfg.get("tasks.max") or ""),
                str(cfg.get("topics") or ""),
                state,
                _status_bucket(state),
            ]
        )
    return rows if len(rows) > 1 else None


def _table_connector_status(data: Any) -> list[list[str]] | None:
    if not isinstance(data, dict):
        return None
    name = str(data.get("name") or "")
    con = data.get("connector") if isinstance(data.get("connector"), dict) else {}
    tasks = data.get("tasks") if isinstance(data.get("tasks"), list) else []
    rows = [["Scope", "Id", "State", "Color", "Worker", "Type", "Trace"]]
    cstate = str(con.get("state") or "")
    rows.append(
        [
            "connector",
            name,
            cstate,
            _status_bucket(cstate),
            str(con.get("worker_id") or ""),
            str(con.get("type") or ""),
            "",
        ]
    )
    for t in tasks:
        if not isinstance(t, dict):
            continue
        ts = str(t.get("state") or "")
        tr = str(t.get("trace") or "").strip().splitlines()
        tr_head = tr[0][:200] if tr else ""
        rows.append(
            [
                "task",
                str(t.get("id") or ""),
                ts,
                _status_bucket(ts),
                str(t.get("worker_id") or ""),
                "",
                tr_head,
            ]
        )
    return rows


def _table_connector_config(data: Any) -> list[list[str]] | None:
    if not isinstance(data, dict):
        return None
    rows = [["Key", "Value"]]
    for k in sorted(data.keys(), key=lambda x: str(x).casefold()):
        rows.append([str(k), str(data.get(k) or "")])
    return rows if len(rows) > 1 else None


def _table_connector_tasks(data: Any) -> list[list[str]] | None:
    if not isinstance(data, list):
        return None
    rows = [["Task id", "State", "Color", "Worker", "Class", "Trace"]]
    for t in data:
        if not isinstance(t, dict):
            continue
        state = str(t.get("state") or "")
        tr = str(t.get("trace") or "").strip().splitlines()
        tr_head = tr[0][:200] if tr else ""
        rows.append(
            [
                str(t.get("id") or ""),
                state,
                _status_bucket(state),
                str(t.get("worker_id") or ""),
                str(t.get("class") or ""),
                tr_head,
            ]
        )
    return rows if len(rows) > 1 else None


def _table_connector_topics(data: Any) -> list[list[str]] | None:
    rows = [["Topic"]]
    if isinstance(data, dict):
        if isinstance(data.get("topics"), list):
            for t in data["topics"]:
                s = str(t or "").strip()
                if s:
                    rows.append([s])
            return rows if len(rows) > 1 else None
        if isinstance(data.get("topics"), dict):
            for t in sorted(data["topics"].keys(), key=lambda x: str(x).casefold()):
                rows.append([str(t)])
            return rows if len(rows) > 1 else None
    return None


def _table_plugins(data: Any) -> list[list[str]] | None:
    if not isinstance(data, list):
        return None
    rows = [["Class", "Type", "Version"]]
    for p in data:
        if not isinstance(p, dict):
            continue
        rows.append([str(p.get("class") or ""), str(p.get("type") or ""), str(p.get("version") or "")])
    return rows if len(rows) > 1 else None


def _table_admin_loggers(data: Any) -> list[list[str]] | None:
    if not isinstance(data, dict):
        return None
    rows = [["Logger", "Level"]]
    for name in sorted(data.keys(), key=lambda x: str(x).casefold()):
        v = data.get(name)
        if isinstance(v, dict):
            lvl = str(v.get("level") or v.get("configuredLevel") or "")
        else:
            lvl = str(v or "")
        rows.append([str(name), lvl])
    return rows if len(rows) > 1 else None


def table_for_panel(panel: str, stdout: str, *, panel_filter: str = "") -> list[list[str]] | None:
    """Best-effort parse Kafka Connect JSON into sortable table rows."""
    try:
        data = _json_from_stdout(stdout)
    except json.JSONDecodeError:
        return None
    table: list[list[str]] | None = None
    if panel == "kc_connectors":
        table = _table_connectors(data)
    elif panel == "kc_connectors_status":
        table = _table_connectors_status(data)
    elif panel in ("kc_connectors_info", "kc_connectors_full"):
        table = _table_connectors_info(data)
    elif panel == "kc_connector_status":
        table = _table_connector_status(data)
    elif panel == "kc_connector_config":
        table = _table_connector_config(data)
    elif panel == "kc_connector_tasks":
        table = _table_connector_tasks(data)
    elif panel == "kc_connector_topics":
        table = _table_connector_topics(data)
    elif panel == "kc_plugins":
        table = _table_plugins(data)
    elif panel == "kc_admin_loggers":
        table = _table_admin_loggers(data)
    if table is None:
        return None
    return _filter_rows(table, panel_filter)
