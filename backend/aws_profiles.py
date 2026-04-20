"""Built-in AWS named profiles for this tool (not read from ~/.aws/credentials)."""

from __future__ import annotations

import configparser
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

# Preset list only — do not scan local AWS config files.
AWS_PROFILE_DEFAULT = "glassbox_AWS_MT"

AWS_PROFILE_OPTIONS: tuple[str, ...] = (
    "glassbox_AWS_MT",
    "glassbox_prod",
    "glassbox_PROD_MT",
    "glassbox_PROD_ST",
    "glassbox_PROD_ST2",
    "glassbox_WF",
    "glassbox_STG",
)


def _credentials_path() -> Path:
    return Path.home() / ".aws" / "credentials"


def read_aws_expiration_from_credentials(path: Path | None = None) -> dict[str, str]:
    """
    Parse ``~/.aws/credentials`` (INI) and return ``profile_name -> aws_expiration`` raw values
    for every section that defines ``aws_expiration``.
    """
    p = path or _credentials_path()
    out: dict[str, str] = {}
    if not p.is_file():
        return out
    cfg = configparser.ConfigParser()
    try:
        cfg.read(p, encoding="utf-8")
    except (configparser.Error, OSError, UnicodeDecodeError):
        return out
    for section in cfg.sections():
        if not cfg.has_option(section, "aws_expiration"):
            continue
        raw = (cfg.get(section, "aws_expiration", fallback="") or "").strip().strip('"').strip("'")
        if raw:
            out[section] = raw
    return out


def _parse_expiration_iso(raw: str) -> datetime | None:
    s = raw.strip()
    if not s:
        return None
    try:
        if s.endswith("Z"):
            return datetime.fromisoformat(s.replace("Z", "+00:00"))
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except (ValueError, TypeError, OSError):
        return None


def _format_remaining_seconds(sec: int) -> str:
    if sec < 0:
        return "expired"
    if sec < 90:
        return f"{sec}s"
    m = sec // 60
    if m < 120:
        return f"{m}m"
    h = sec // 3600
    if h < 48:
        mm = (sec % 3600) // 60
        return f"{h}h {mm}m" if mm else f"{h}h"
    d = sec // 86400
    hh = (sec % 86400) // 3600
    return f"{d}d {hh}h" if hh else f"{d}d"


def build_session_hints(expirations: dict[str, str]) -> dict[str, dict[str, Any]]:
    """For each profile with a parsed expiry, add remaining time labels (UTC)."""
    now = datetime.now(timezone.utc)
    hints: dict[str, dict[str, Any]] = {}
    for name, raw in expirations.items():
        exp = _parse_expiration_iso(raw)
        if exp is None:
            hints[name] = {
                "aws_expiration": raw,
                "remaining_seconds": None,
                "remaining_label": "invalid date",
            }
            continue
        sec = int((exp - now).total_seconds())
        hints[name] = {
            "aws_expiration": raw,
            "expires_at_utc": exp.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "remaining_seconds": sec,
            "remaining_label": _format_remaining_seconds(sec),
        }
    return hints


def list_profiles() -> tuple[list[str], dict[str, str]]:
    """Returns (profiles in display order, meta)."""
    meta = {
        "source": "builtin",
        "default_profile": AWS_PROFILE_DEFAULT,
        "credentials_path": str(_credentials_path()),
    }
    return list(AWS_PROFILE_OPTIONS), meta


def profiles_api_payload() -> dict[str, Any]:
    """Payload for ``GET /api/aws/profiles`` including session hints from credentials."""
    profiles, meta = list_profiles()
    raw = read_aws_expiration_from_credentials()
    session_hints = build_session_hints(raw)
    return {
        "ok": True,
        "profiles": profiles,
        "default_profile": AWS_PROFILE_DEFAULT,
        "meta": meta,
        "session_hints": session_hints,
    }


def is_allowed_profile(name: str) -> bool:
    return name in AWS_PROFILE_OPTIONS
