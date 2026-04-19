"""Signed cookie session: verified cluster name + Kafka namespace."""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import os
import time
from typing import Any

COOKIE_NAME = "gb_sts_session"
_TTL_DEFAULT = 12 * 3600


_ephemeral_secret: bytes | None = None


def _secret() -> bytes:
    global _ephemeral_secret
    raw = os.environ.get("GB_STS_COOKIE_SECRET", "").strip()
    if raw:
        return raw.encode("utf-8")
    if _ephemeral_secret is None:
        _ephemeral_secret = os.urandom(32)
    return _ephemeral_secret


def pack(
    cluster_substring: str,
    namespace: str,
    ttl_sec: int = _TTL_DEFAULT,
    aws_profile: str | None = None,
    cloud: str | None = None,
    region: str | None = None,
) -> str:
    payload: dict[str, Any] = {
        "c": cluster_substring,
        "n": namespace,
        "exp": int(time.time()) + ttl_sec,
    }
    if aws_profile:
        payload["p"] = aws_profile
    if cloud:
        payload["d"] = cloud
    if region:
        payload["r"] = region
    body = (
        base64.urlsafe_b64encode(json.dumps(payload, separators=(",", ":")).encode("utf-8"))
        .decode("ascii")
        .rstrip("=")
    )
    sig = hmac.new(_secret(), body.encode("utf-8"), hashlib.sha256).hexdigest()[:48]
    return f"{body}.{sig}"


def unpack(token: str | None) -> dict[str, Any] | None:
    if not token or "." not in token:
        return None
    body, sig = token.rsplit(".", 1)
    if len(sig) != 48:
        return None
    expect = hmac.new(_secret(), body.encode("utf-8"), hashlib.sha256).hexdigest()[:48]
    if not hmac.compare_digest(expect, sig):
        return None
    pad = "=" * (-len(body) % 4)
    try:
        raw = base64.urlsafe_b64decode((body + pad).encode("ascii"))
        data = json.loads(raw.decode("utf-8"))
    except (json.JSONDecodeError, ValueError):
        return None
    if int(data.get("exp") or 0) < int(time.time()):
        return None
    if not data.get("c") or not data.get("n"):
        return None
    return data
