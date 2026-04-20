"""Writable operator-local state under data/sensitive/ (gitignored)."""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

from backend.regions import REGION_CODES

ROOT = Path(__file__).resolve().parents[1]
SENSITIVE_DIR = ROOT / "data" / "sensitive"
LAST_CONNECTED_FILE = SENSITIVE_DIR / "last_connected.json"
# Operator-created file; gitignored via data/sensitive/*
CLICKHOUSE_PASSWORD_FILE = SENSITIVE_DIR / ".ch_password"
POSTGRES_PASSWORD_FILE = SENSITIVE_DIR / ".pg_password"


def ensure_sensitive_dir() -> None:
    SENSITIVE_DIR.mkdir(parents=True, exist_ok=True)
    try:
        os.chmod(SENSITIVE_DIR, 0o700)
    except OSError:
        pass


def read_clickhouse_password() -> str | None:
    """Single-line password for ``clickhouse-client --password`` (file is gitignored)."""
    if not CLICKHOUSE_PASSWORD_FILE.is_file():
        return None
    try:
        raw = CLICKHOUSE_PASSWORD_FILE.read_text(encoding="utf-8").strip()
    except OSError:
        return None
    return raw or None


def read_postgres_password() -> str | None:
    """Single-line ``PGPASSWORD`` for ``psql`` in the PostgreSQL pod (file is gitignored)."""
    if not POSTGRES_PASSWORD_FILE.is_file():
        return None
    try:
        raw = POSTGRES_PASSWORD_FILE.read_text(encoding="utf-8").strip()
    except OSError:
        return None
    return raw or None


def read_last_connected() -> dict[str, Any] | None:
    if not LAST_CONNECTED_FILE.is_file():
        return None
    try:
        raw = LAST_CONNECTED_FILE.read_text(encoding="utf-8")
        return json.loads(raw)
    except (OSError, json.JSONDecodeError):
        return None


def write_last_connected(data: dict[str, Any]) -> None:
    ensure_sensitive_dir()
    text = json.dumps(data, indent=2, sort_keys=True)
    LAST_CONNECTED_FILE.write_text(text, encoding="utf-8")
    try:
        os.chmod(LAST_CONNECTED_FILE, 0o600)
    except OSError:
        pass


def defaults_for_ui() -> dict[str, Any]:
    """Safe defaults for the Home form (no secrets; structure only)."""
    raw = read_last_connected() or {}
    out: dict[str, Any] = {
        "cloud": raw.get("cloud") if raw.get("cloud") in ("aws", "azure") else "aws",
        "region": raw.get("region") if raw.get("region") in REGION_CODES else "eu-west-1",
        "aws_profile": raw.get("aws_profile") or "",
        "aws_cluster_name": raw.get("aws_cluster_name") or "",
        "azure_resource_group": raw.get("azure_resource_group") or "",
        "azure_cluster_name": raw.get("azure_cluster_name") or "",
        "namespace": raw.get("namespace") or "",
        "regions": list(REGION_CODES),
        "clouds": ["aws", "azure"],
    }
    return out
