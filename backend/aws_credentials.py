"""Load ~/.aws/credentials for a named profile into env-style keys (no shell)."""

from __future__ import annotations

import configparser
from pathlib import Path


def load_named_profile_credentials(credentials_path: Path, profile: str) -> dict[str, str]:
    """
    Read ``aws_access_key_id`` / ``aws_secret_access_key`` / ``aws_session_token`` for ``profile``.
    Returns a dict suitable for ``os.environ`` merge (standard AWS_* names plus legacy aliases).
    """
    p = (profile or "").strip()
    if not p:
        return {}
    path = credentials_path.expanduser()
    if not path.is_file():
        return {}
    parser = configparser.ConfigParser()
    try:
        parser.read(path, encoding="utf-8")
    except configparser.Error:
        return {}
    if p not in parser:
        return {}
    sec = parser[p]
    out: dict[str, str] = {}

    def _get(key: str) -> str:
        raw = sec.get(key, fallback="").strip()
        if raw.startswith('"') and raw.endswith('"') and len(raw) >= 2:
            raw = raw[1:-1]
        if raw.startswith("'") and raw.endswith("'") and len(raw) >= 2:
            raw = raw[1:-1]
        return raw

    ak = _get("aws_access_key_id")
    sk = _get("aws_secret_access_key")
    tok = _get("aws_session_token")
    if ak:
        out["AWS_ACCESS_KEY_ID"] = ak
        out["AWS_ACCESS_KEY"] = ak
    if sk:
        out["AWS_SECRET_ACCESS_KEY"] = sk
        out["AWS_SECRET_KEY"] = sk
    if tok:
        out["AWS_SESSION_TOKEN"] = tok
        out["AWS_SECURITY_TOKEN"] = tok
    return out
