#!/usr/bin/env python3
"""
Glassbox STS tool — local web UI for read-only Kafka / ClickHouse / Elasticsearch diagnostics via kubectl exec.

Run: ./run
Open: http://127.0.0.1:<port>/
"""

from __future__ import annotations

import json
import os
import re
import select
import shlex
import socket
import subprocess
import sys
import urllib.parse
from http import HTTPStatus
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

ROOT = Path(__file__).resolve().parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

VERSION = "1.0.14"

from backend import aws_profiles  # noqa: E402
from backend import ch_panels as chp  # noqa: E402
from backend import es_panels as esp  # noqa: E402
from backend import cloud_clusters as cc  # noqa: E402
from backend import kafka_parse as kparse  # noqa: E402
from backend import k8s_exec as kx  # noqa: E402
from backend import regions as reg  # noqa: E402
from backend import sensitive_store as sens  # noqa: E402
from backend import session as sess  # noqa: E402
from backend import stack_components as stack  # noqa: E402

_httpd: ThreadingHTTPServer | None = None


def _bind_host() -> str:
    return os.environ.get("GB_STS_BIND", "127.0.0.1").strip() or "127.0.0.1"


def _port() -> int:
    raw = os.environ.get("GB_STS_PORT", "8791").strip()
    try:
        return int(raw)
    except ValueError:
        return 8791


def _read_json_body(handler: SimpleHTTPRequestHandler) -> dict:
    ln = handler.headers.get("Content-Length")
    if not ln:
        return {}
    try:
        n = int(ln)
    except ValueError:
        return {}
    raw = handler.rfile.read(n)
    try:
        return json.loads(raw.decode("utf-8"))
    except (json.JSONDecodeError, UnicodeDecodeError):
        return {}


def _cookie_value(handler: SimpleHTTPRequestHandler, name: str) -> str | None:
    raw = handler.headers.get("Cookie") or ""
    for part in raw.split(";"):
        part = part.strip()
        if part.startswith(name + "="):
            return urllib.parse.unquote(part.split("=", 1)[1].strip())
    return None


def _session_from(handler: SimpleHTTPRequestHandler) -> dict | None:
    return sess.unpack(_cookie_value(handler, sess.COOKIE_NAME))


def _cluster_server_for_current_context(
    timeout: int = 15, aws_profile: str | None = None, cloud: str | None = None
) -> str:
    r = kx._run(
        [
            "kubectl",
            "config",
            "view",
            "--minify",
            "-o",
            "jsonpath={.clusters[0].cluster.server}",
        ],
        timeout=timeout,
        aws_profile=aws_profile,
        cloud=cloud,
    )
    return (r.stdout or "").strip()


def _cloud_from_session(s: dict | None) -> str:
    if not s:
        return "aws"
    d = s.get("d")
    v = (d or "aws").strip().lower()
    return v if v in ("aws", "azure") else "aws"


def _aws_profile_from_session(s: dict | None) -> str | None:
    if not s:
        return None
    p = s.get("p")
    return str(p).strip() if p else None


# Shown in UI (actual pod command also prefixes ``export JMX_PORT=<random>;``).
PANEL_CMD_HINT: dict[str, str] = {
    "broker_versions": f"{kx.KAFKA_BIN}/kafka-broker-api-versions.sh --bootstrap-server {kx.BOOTSTRAP}",
    "broker_default_configs": f"{kx.KAFKA_BIN}/kafka-configs.sh --bootstrap-server {kx.BOOTSTRAP} --entity-type brokers --entity-name <id> --describe (per broker id seen in topic metadata)",
    "topics_partition_counts": f"{kx.KAFKA_BIN}/kafka-topics.sh --describe --bootstrap-server {kx.BOOTSTRAP} | grep Topic | awk '{{print $2}}' | sort | uniq -c",
    "leader_balance": f"{kx.KAFKA_BIN}/kafka-topics.sh --describe --bootstrap-server {kx.BOOTSTRAP} (aggregate Leader: counts per broker id)",
    "consumer_groups_list": f"{kx.KAFKA_BIN}/kafka-consumer-groups.sh --bootstrap-server {kx.BOOTSTRAP} --list; plus --describe --all-groups (truncated) to estimate Group size (MB) from assignment row counts",
    "group_state": f"{kx.KAFKA_BIN}/kafka-consumer-groups.sh --bootstrap-server {kx.BOOTSTRAP} --describe --group <G> --state --verbose",
    "group_members": f"{kx.KAFKA_BIN}/kafka-consumer-groups.sh --bootstrap-server {kx.BOOTSTRAP} --describe --group <G> --members",
    "group_members_verbose": f"{kx.KAFKA_BIN}/kafka-consumer-groups.sh --bootstrap-server {kx.BOOTSTRAP} --describe --group <G> --members --verbose",
    "group_lag": f"{kx.KAFKA_BIN}/kafka-consumer-groups.sh --bootstrap-server {kx.BOOTSTRAP} --describe --group <G>",
    "topic_offsets_end": f"{kx.KAFKA_BIN}/kafka-run-class.sh kafka.tools.GetOffsetShell --broker-list {kx.BOOTSTRAP} --topic <T> --time -1",
    "topic_describe": f"{kx.KAFKA_BIN}/kafka-topics.sh --describe --bootstrap-server {kx.BOOTSTRAP} --topic <T>",
}


def _topics_min_from_qs(qs: dict[str, list[str]]) -> int:
    raw = (qs.get("topics_min") or ["0"])[0].strip()
    if not raw:
        return 0
    try:
        n = int(raw)
    except ValueError:
        return 0
    return max(0, min(n, 10_000_000))


def _ch_pod_index_from_qs(qs: dict[str, list[str]]) -> int:
    raw = (qs.get("ch_pod") or ["0"])[0].strip()
    try:
        n = int(raw)
    except ValueError:
        return 0
    return max(0, min(n, 31))


def _hours_from_ch_qs(qs: dict[str, list[str]]) -> int:
    raw = (qs.get("hours") or ["24"])[0].strip()
    try:
        h = int(raw)
    except ValueError:
        return 24
    return max(1, min(h, 336))


def _topics_substring_from_qs(qs: dict[str, list[str]]) -> str:
    s = (qs.get("topics_substring") or [""])[0].strip()[:200]
    if not s:
        return ""
    if not re.match(r"^[A-Za-z0-9_.*\-]+$", s):
        raise ValueError("Topic substring filter: use letters, digits, . _ - * only")
    return s


def _es_pod_index_from_qs(qs: dict[str, list[str]]) -> int:
    raw = (qs.get("es_pod") or ["0"])[0].strip()
    try:
        n = int(raw)
    except ValueError:
        return 0
    return max(0, min(n, 31))


def _idx_health_from_qs(qs: dict[str, list[str]]) -> str:
    h = (qs.get("idx_health") or ["all"])[0].strip().lower()
    return h if h in ("all", "green", "yellow", "red") else "all"


def _idx_state_from_qs(qs: dict[str, list[str]]) -> str:
    st = (qs.get("idx_state") or ["all"])[0].strip().lower()
    return st if st in ("all", "open", "close") else "all"


def _alloc_body_from_qs(qs: dict[str, list[str]]) -> str:
    raw = (qs.get("alloc_body") or ["{}"])[0]
    if len(raw) > 12_000:
        raise ValueError("alloc_body query too long (max 12000 chars)")
    return raw


def _verify_cluster_name(user: str, current_context: str, cluster_server: str) -> bool:
    u = user.strip().casefold()
    if not u:
        return False
    if u in current_context.casefold():
        return True
    if cluster_server and u in cluster_server.casefold():
        return True
    return False


def _panel_script(panel: str, group: str, topic: str) -> tuple[str | None, str | None]:
    """
    Returns (bash_body, error_message) — bash_body is run inside the kafka container after
    ``export JMX_PORT=<random high port>;`` (see ``kafka_bash_exec``).
    """
    kb = kx.KAFKA_BIN
    bs = kx.BOOTSTRAP
    if panel == "broker_versions":
        return (
            f"{kb}/kafka-broker-api-versions.sh --bootstrap-server {bs} 2>&1 | head -n 500",
            None,
        )
    if panel == "broker_default_configs":
        # --entity-default often returns nothing on Bitnami; describe per broker id from topic metadata.
        inner = f"""D=$(mktemp)
{kb}/kafka-topics.sh --describe --bootstrap-server {bs} >"$D" 2>&1 || true
IDS=$(grep 'Leader:' "$D" 2>/dev/null | sed -n 's/.*Leader: \\([0-9]*\\).*/\\1/p' | sort -u | head -n 32)
for id in $IDS; do
  echo "=== broker $id ==="
  {kb}/kafka-configs.sh --bootstrap-server {bs} --entity-type brokers --entity-name "$id" --describe 2>&1
done
rm -f "$D"
"""
        return (inner, None)
    if panel == "topics_partition_counts":
        return (
            f"{kb}/kafka-topics.sh --describe --bootstrap-server {bs} 2>&1 | "
            r"""grep Topic | awk '{print $2}' | sort | uniq -c""",
            None,
        )
    if panel == "leader_balance":
        inner = f"""D=$(mktemp)
{kb}/kafka-topics.sh --describe --bootstrap-server {bs} >"$D" 2>&1 || true
IDS=$(grep 'Leader:' "$D" 2>/dev/null | sed -n 's/.*Leader: \\([0-9]*\\).*/\\1/p' | sort -u)
for i in $IDS; do
  c=$(grep -c "Leader: $i" "$D" 2>/dev/null || echo 0)
  if [ "$c" -gt 0 ] 2>/dev/null; then echo "broker $i $c"; fi
done
rm -f "$D"
"""
        return (inner, None)
    if panel == "consumer_groups_list":
        inner = (
            f"LISTF=$(mktemp); DESCF=$(mktemp); "
            f"{kb}/kafka-consumer-groups.sh --bootstrap-server {bs} --list 2>&1 | head -n 2000 >\"$LISTF\" || true; "
            f"{kb}/kafka-consumer-groups.sh --bootstrap-server {bs} --describe --all-groups 2>&1 | head -n 500000 >\"$DESCF\" || true; "
            f"echo '---GB_STS_LIST---'; cat \"$LISTF\"; echo '---GB_STS_DESCRIBE_ALL---'; cat \"$DESCF\"; "
            f"rm -f \"$LISTF\" \"$DESCF\""
        )
        return (inner, None)
    if panel == "group_state":
        try:
            g = kx.sanitize_name(group, "Consumer group")
        except ValueError as e:
            return None, str(e)
        return (
            f"{kb}/kafka-consumer-groups.sh --bootstrap-server {bs} --describe --group {shlex.quote(g)} "
            f"--state --verbose 2>&1",
            None,
        )
    if panel == "group_members":
        try:
            g = kx.sanitize_name(group, "Consumer group")
        except ValueError as e:
            return None, str(e)
        return (
            f"{kb}/kafka-consumer-groups.sh --bootstrap-server {bs} --describe --group {shlex.quote(g)} "
            f"--members 2>&1",
            None,
        )
    if panel == "group_members_verbose":
        try:
            g = kx.sanitize_name(group, "Consumer group")
        except ValueError as e:
            return None, str(e)
        return (
            f"{kb}/kafka-consumer-groups.sh --bootstrap-server {bs} --describe --group {shlex.quote(g)} "
            f"--members --verbose 2>&1 | head -n 500",
            None,
        )
    if panel == "group_lag":
        try:
            g = kx.sanitize_name(group, "Consumer group")
        except ValueError as e:
            return None, str(e)
        return (
            f"{kb}/kafka-consumer-groups.sh --bootstrap-server {bs} --describe --group {shlex.quote(g)} 2>&1 "
            f"| head -n 8000",
            None,
        )
    if panel == "topic_offsets_end":
        try:
            t = kx.sanitize_name(topic, "Topic")
        except ValueError as e:
            return None, str(e)
        return (
            f"{kb}/kafka-run-class.sh kafka.tools.GetOffsetShell --broker-list {bs} --topic {shlex.quote(t)} "
            f"--time -1 2>&1 | head -n 8000",
            None,
        )
    if panel == "topic_describe":
        try:
            t = kx.sanitize_name(topic, "Topic")
        except ValueError as e:
            return None, str(e)
        return (
            f"{kb}/kafka-topics.sh --describe --bootstrap-server {bs} --topic {shlex.quote(t)} 2>&1 | head -n 8000",
            None,
        )
    return None, "unknown panel"


def _sse_write_chunk(wfile, obj: object) -> bool:
    try:
        payload = json.dumps(obj, ensure_ascii=False)
        wfile.write(f"data: {payload}\n\n".encode("utf-8"))
        wfile.flush()
        return True
    except (BrokenPipeError, ConnectionResetError, OSError):
        return False


def _kafka_logs_stream(handler: SimpleHTTPRequestHandler, qs: dict[str, list[str]]) -> None:
    s = _session_from(handler)
    if not s:
        handler._json({"ok": False, "error": "Session not verified."}, HTTPStatus.UNAUTHORIZED)
        return
    pod_raw = (qs.get("pod") or [""])[0].strip()
    cont_raw = (qs.get("container") or [kx.KAFKA_CONTAINER])[0].strip()
    pod_name = kx.sanitize_kafka_logs_pod(pod_raw)
    if not pod_name:
        handler._json(
            {"ok": False, "error": "Invalid pod (expected glassbox-kafka-0 … glassbox-kafka-31)."},
            HTTPStatus.BAD_REQUEST,
        )
        return
    container = kx.sanitize_kafka_logs_container(cont_raw)
    if not container:
        handler._json(
            {"ok": False, "error": f"Invalid container (only {kx.KAFKA_CONTAINER!r} is allowed)."},
            HTTPStatus.BAD_REQUEST,
        )
        return
    ns = str(s["n"])
    cl = _cloud_from_session(s)
    ap = _aws_profile_from_session(s)
    env = kx.subprocess_env(ap, cl)
    argv = [
        "kubectl",
        "logs",
        "-n",
        ns,
        pod_name,
        "-c",
        container,
        "--tail",
        "10",
        "-f",
    ]
    proc: subprocess.Popen | None = None
    try:
        proc = subprocess.Popen(
            argv,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            bufsize=0,
            env=env,
        )
    except OSError as e:
        handler._json({"ok": False, "error": str(e)}, HTTPStatus.BAD_GATEWAY)
        return
    assert proc.stdout is not None
    out = proc.stdout
    req = handler.request
    handler.send_response(HTTPStatus.OK)
    handler.send_header("Content-Type", "text/event-stream; charset=utf-8")
    handler.send_header("Cache-Control", "no-cache")
    handler.send_header("Connection", "close")
    handler.send_header("X-Accel-Buffering", "no")
    handler.end_headers()
    buf = b""

    def flush_tail() -> bool:
        nonlocal buf
        if not buf:
            return True
        ok = _sse_write_chunk(handler.wfile, {"line": buf.decode("utf-8", errors="replace")})
        buf = b""
        return ok

    try:
        while True:
            try:
                r_fds, _, _ = select.select([out.fileno(), req.fileno()], [], [], 300.0)
            except (ValueError, OSError):
                break

            if not r_fds:
                if proc.poll() is not None:
                    remainder = out.read() or b""
                    if remainder:
                        buf += remainder
                        while b"\n" in buf:
                            line, buf = buf.split(b"\n", 1)
                            if not _sse_write_chunk(handler.wfile, {"line": line.decode("utf-8", errors="replace")}):
                                return
                        if buf and not flush_tail():
                            return
                    break
                continue

            if req.fileno() in r_fds:
                try:
                    drained = req.recv(4096)
                except OSError:
                    drained = b""
                if not drained:
                    break

            if out.fileno() in r_fds:
                try:
                    chunk = os.read(out.fileno(), 65536)
                except OSError:
                    chunk = b""
                if not chunk:
                    if buf and not flush_tail():
                        return
                    if proc.poll() is not None:
                        break
                    continue
                buf += chunk
                while b"\n" in buf:
                    line, buf = buf.split(b"\n", 1)
                    if not _sse_write_chunk(handler.wfile, {"line": line.decode("utf-8", errors="replace")}):
                        return
                continue

            if proc.poll() is not None:
                remainder = out.read() or b""
                if remainder:
                    buf += remainder
                    while b"\n" in buf:
                        line, buf = buf.split(b"\n", 1)
                        if not _sse_write_chunk(handler.wfile, {"line": line.decode("utf-8", errors="replace")}):
                            return
                    if buf and not flush_tail():
                        return
                break
        rc = proc.poll()
        if rc not in (None, 0) and not _sse_write_chunk(
            handler.wfile, {"line": f"[kubectl logs exited {rc}]"}
        ):
            return
    finally:
        if proc is not None and proc.poll() is None:
            try:
                proc.kill()
            except OSError:
                pass
            try:
                proc.wait(timeout=8)
            except subprocess.TimeoutExpired:
                pass


class Handler(SimpleHTTPRequestHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory=str(ROOT), **kwargs)

    def log_message(self, fmt: str, *args) -> None:
        sys.stderr.write("%s - %s\n" % (self.address_string(), fmt % args))

    def end_headers(self) -> None:
        self.send_header("Cache-Control", "no-cache, no-store, must-revalidate")
        self.send_header("Pragma", "no-cache")
        self.send_header("Expires", "0")
        super().end_headers()

    def _json(self, obj: object, status: int = HTTPStatus.OK, extra_headers: list[tuple[str, str]] | None = None) -> None:
        data = json.dumps(obj, default=str).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        self.send_header("Cache-Control", "no-store")
        if extra_headers:
            for k, v in extra_headers:
                self.send_header(k, v)
        self.end_headers()
        try:
            self.wfile.write(data)
        except (BrokenPipeError, ConnectionResetError):
            pass

    def do_GET(self) -> None:  # noqa: N802
        parsed = urllib.parse.urlparse(self.path)
        path = parsed.path
        qs = urllib.parse.parse_qs(parsed.query)

        if path == "/api/about":
            self._json({
                "version": VERSION,
                "name": "Glassbox STS Tool",
                "description": "Read-first local dashboard: verify kubectl context, then inspect Kafka / ClickHouse / Elasticsearch in-cluster via kubectl exec.",
                "kafka_pod": kx.KAFKA_POD,
                "clickhouse_pod_prefix": kx.CLICKHOUSE_POD_PREFIX,
                "elasticsearch_pod_prefix": kx.ELASTICSEARCH_POD_PREFIX,
            })
            return

        if path == "/api/aws/profiles":
            profiles, meta = aws_profiles.list_profiles()
            self._json(
                {
                    "ok": True,
                    "profiles": profiles,
                    "default_profile": aws_profiles.AWS_PROFILE_DEFAULT,
                    "meta": meta,
                }
            )
            return

        if path == "/api/k8s/defaults":
            self._json({"ok": True, **sens.defaults_for_ui()})
            return

        if path == "/api/k8s/context":
            s = _session_from(self)
            cl = _cloud_from_session(s)
            ap = _aws_profile_from_session(s)
            cur = kx.kubectl_current_context(aws_profile=ap, cloud=cl)
            srv = _cluster_server_for_current_context(aws_profile=ap, cloud=cl) if cur.ok else ""
            self._json({
                "ok": cur.ok,
                "current_context": (cur.stdout or "").strip(),
                "cluster_server": srv,
                "kubectl_stderr": cur.stderr if not cur.ok else "",
                "aws_profile": ap,
                "cloud": cl,
            })
            return

        if path == "/api/session/status":
            s = _session_from(self)
            cl = _cloud_from_session(s)
            ap = _aws_profile_from_session(s)
            cur = kx.kubectl_current_context(aws_profile=ap, cloud=cl)
            self._json({
                "verified": s is not None,
                "session": s,
                "current_context": (cur.stdout or "").strip() if cur.ok else "",
                "cluster_server": _cluster_server_for_current_context(aws_profile=ap, cloud=cl) if cur.ok else "",
                "aws_profile": ap,
                "cloud": cl,
                "clickhouse_password_configured": bool(sens.read_clickhouse_password()),
            })
            return

        if path == "/api/k8s/stack":
            s = _session_from(self)
            if not s:
                self._json({"ok": False, "error": "Session not verified."}, HTTPStatus.UNAUTHORIZED)
                return
            ns = s["n"]
            cl = _cloud_from_session(s)
            ap = _aws_profile_from_session(s)
            self._json(stack.probe_stack_in_namespace(ns, aws_profile=ap, cloud=cl))
            return

        if path == "/api/clickhouse/panel":
            s = _session_from(self)
            if not s:
                self._json({"ok": False, "error": "Session not verified. Connect from Home first."}, HTTPStatus.UNAUTHORIZED)
                return
            pw = sens.read_clickhouse_password()
            if not pw:
                self._json(
                    {
                        "ok": False,
                        "error": "ClickHouse password file missing.",
                        "hint": "Create data/sensitive/.ch_password (single line) — path is gitignored.",
                    },
                    HTTPStatus.SERVICE_UNAVAILABLE,
                )
                return
            ns = s["n"]
            cl = _cloud_from_session(s)
            ap = _aws_profile_from_session(s)
            panel = (qs.get("panel") or [""])[0].strip()
            ch_pod = _ch_pod_index_from_qs(qs)
            hours = _hours_from_ch_qs(qs)
            from_ts = (qs.get("from_ts") or [""])[0].strip()
            to_ts = (qs.get("to_ts") or [""])[0].strip()
            event_table = (qs.get("event_table") or ["beacon_event"])[0].strip() or "beacon_event"
            tenant_id = (qs.get("tenant_id") or [""])[0].strip()
            try:
                sql = chp.sql_for_panel(
                    panel,
                    event_table=event_table,
                    hours=hours,
                    from_ts=from_ts,
                    to_ts=to_ts,
                    tenant_id=tenant_id,
                )
            except ValueError as e:
                self._json({"ok": False, "error": str(e)}, HTTPStatus.BAD_REQUEST)
                return
            pod_name = f"{kx.CLICKHOUSE_POD_PREFIX}{ch_pod}"
            r = kx.clickhouse_client_query(
                ns,
                pod_name,
                kx.CLICKHOUSE_CONTAINER,
                pw,
                sql,
                timeout=300,
                aws_profile=ap,
                cloud=cl,
            )
            self._json(
                {
                    "ok": r.ok,
                    "panel": panel,
                    "stdout": kx.tail_lines(r.stdout, 12000),
                    "stderr": kx.filter_kubectl_noise(r.stderr),
                    "returncode": r.returncode,
                    "cmd_display": r.cmd_display,
                    "cmd_hint": chp.CH_PANEL_CMD_HINT.get(panel, ""),
                    "sql_executed": sql,
                    "pod": pod_name,
                }
            )
            return

        if path == "/api/elasticsearch/panel":
            s = _session_from(self)
            if not s:
                self._json({"ok": False, "error": "Session not verified. Connect from Home first."}, HTTPStatus.UNAUTHORIZED)
                return
            ns = s["n"]
            cl = _cloud_from_session(s)
            ap = _aws_profile_from_session(s)
            panel = (qs.get("panel") or [""])[0].strip()
            es_pod = _es_pod_index_from_qs(qs)
            idx_health = _idx_health_from_qs(qs)
            idx_state = _idx_state_from_qs(qs)
            try:
                idx_sub = esp.sanitize_idx_substring((qs.get("idx_substring") or [""])[0])
            except ValueError as e:
                self._json({"ok": False, "error": str(e)}, HTTPStatus.BAD_REQUEST)
                return
            try:
                alloc_body = _alloc_body_from_qs(qs)
            except ValueError as e:
                self._json({"ok": False, "error": str(e)}, HTTPStatus.BAD_REQUEST)
                return
            index_pattern = (qs.get("index_pattern") or [""])[0].strip()
            try:
                method, path_qs, req_body = esp.es_request_for_panel(
                    panel,
                    index_pattern=index_pattern,
                    alloc_body=alloc_body,
                )
            except ValueError as e:
                self._json({"ok": False, "error": str(e)}, HTTPStatus.BAD_REQUEST)
                return
            pod_name = f"{kx.ELASTICSEARCH_POD_PREFIX}{es_pod}"
            timeout = 240 if panel in ("es_nodes_stats", "es_cluster_stats", "es_cluster_state_metadata") else 150
            r = kx.elasticsearch_curl(
                ns,
                pod_name,
                kx.ELASTICSEARCH_CONTAINER,
                path_qs,
                method=method,
                body=req_body,
                timeout=timeout,
                aws_profile=ap,
                cloud=cl,
            )
            out_stdout = r.stdout or ""
            if panel == "es_cat_indices" and r.ok:
                out_stdout = esp.postprocess_cat_indices(out_stdout, idx_health, idx_state, idx_sub)
            tail_n = 20000 if panel in ("es_nodes_stats", "es_cluster_stats") else 12000
            self._json(
                {
                    "ok": r.ok,
                    "panel": panel,
                    "stdout": kx.tail_lines(out_stdout, tail_n),
                    "stderr": kx.filter_kubectl_noise(r.stderr),
                    "returncode": r.returncode,
                    "cmd_display": r.cmd_display,
                    "cmd_hint": esp.ES_PANEL_CMD_HINT.get(panel, ""),
                    "http_method": method,
                    "path_executed": path_qs,
                    "pod": pod_name,
                }
            )
            return

        if path == "/api/kafka/logs/stream":
            _kafka_logs_stream(self, qs)
            return

        if path == "/api/kafka/panel":
            s = _session_from(self)
            if not s:
                self._json({"ok": False, "error": "Session not verified. Connect from Home first."}, HTTPStatus.UNAUTHORIZED)
                return
            ns = s["n"]
            cl = _cloud_from_session(s)
            ap = _aws_profile_from_session(s)
            panel = (qs.get("panel") or [""])[0].strip()
            group = (qs.get("group") or [""])[0].strip()
            topic = (qs.get("topic") or [""])[0].strip()
            topics_sub = ""
            if panel == "topics_partition_counts":
                try:
                    topics_sub = _topics_substring_from_qs(qs)
                except ValueError as e:
                    self._json({"ok": False, "error": str(e)}, HTTPStatus.BAD_REQUEST)
                    return
            body, err = _panel_script(panel, group, topic)
            if err:
                self._json({"ok": False, "error": err}, HTTPStatus.BAD_REQUEST)
                return
            assert body is not None
            r = kx.kafka_bash_exec(ns, body, timeout=300, aws_profile=ap, cloud=cl)
            out: dict = {
                "ok": r.ok,
                "panel": panel,
                "stdout": kx.tail_lines(r.stdout, 8000),
                "stderr": kx.filter_kubectl_noise(r.stderr),
                "returncode": r.returncode,
                "cmd_display": r.cmd_display,
                "cmd_hint": PANEL_CMD_HINT.get(panel, ""),
            }
            if panel == "topics_partition_counts":
                raw_rows = kx.parse_topics_partition_counts(r.stdout)
                tmin = _topics_min_from_qs(qs)
                filtered = kparse.filter_topic_partition_rows(
                    raw_rows, min_partitions=tmin, topic_substring=topics_sub
                )
                out["table"] = [["Partitions (×)", "Topic"]] + filtered
            elif panel == "leader_balance":
                rows = []
                for line in r.stdout.splitlines():
                    m = re.match(r"broker\s+(\d+)\s+(\d+)", line.strip())
                    if m and int(m.group(2)) > 0:
                        rows.append([m.group(1), m.group(2)])
                out["table"] = [["Broker id", "Partitions led"]] + rows
            elif panel == "consumer_groups_list":
                list_body, desc_body = kparse.consumer_groups_list_split_panel_stdout(r.stdout or "")
                names = kparse.parse_consumer_group_names(list_body)
                out["groups"] = names
                counts = kparse.parse_all_groups_describe_row_counts(desc_body)
                if counts:
                    rows_tbl: list[list[str]] = []
                    for n in names:
                        rc = int(counts.get(n, 0))
                        mb = (rc * 96.0) / (1024.0 * 1024.0)
                        rows_tbl.append([n, f"{mb:.4f}"])
                    out["table"] = [["Group", "Group size (MB)"]] + rows_tbl
                else:
                    out["table"] = [["Group"]] + [[n] for n in names]
            elif panel == "group_lag":
                lag_rows, lag_hdr = kparse.parse_group_lag_rows(r.stdout, limit=5)
                if lag_hdr and lag_rows:
                    out["table"] = [lag_hdr] + lag_rows
                    out["table_style"] = "kafka_lag_describe"
                else:
                    out["table"] = kx.parse_consumer_group_describe(r.stdout)
            elif panel == "topic_describe":
                summ, parts = kparse.parse_topic_describe(r.stdout)
                if summ:
                    out["table_summary"] = summ
                if parts:
                    out["table"] = parts
            elif panel == "topic_offsets_end":
                off_tbl = kparse.parse_topic_end_offsets(r.stdout)
                if off_tbl:
                    out["table"] = off_tbl
            self._json(out)
            return

        return super().do_GET()

    def do_POST(self) -> None:  # noqa: N802
        parsed = urllib.parse.urlparse(self.path)
        path = parsed.path
        body = _read_json_body(self)

        if path == "/api/clusters/list":
            try:
                cloud = reg.sanitize_cloud(str(body.get("cloud") or ""))
                region = reg.sanitize_region(str(body.get("region") or ""))
                aws_profile = kx.sanitize_aws_profile(str(body.get("aws_profile") or ""))
            except ValueError as e:
                self._json({"ok": False, "error": str(e)}, HTTPStatus.BAD_REQUEST)
                return
            if cloud == "aws" and aws_profile and not aws_profiles.is_allowed_profile(aws_profile):
                self._json(
                    {"ok": False, "error": "AWS profile must be one of the preset options from /api/aws/profiles."},
                    HTTPStatus.BAD_REQUEST,
                )
                return
            if cloud == "aws":
                clusters, err = cc.list_eks_clusters(region, aws_profile)
            else:
                clusters, err = cc.list_aks_clusters(region)
            if err:
                self._json({"ok": False, "error": err, "clusters": []}, HTTPStatus.BAD_GATEWAY)
                return
            self._json({"ok": True, "clusters": clusters, "cloud": cloud, "region": region})
            return

        if path == "/api/kafka/rebalance/analyze":
            s = _session_from(self)
            if not s:
                self._json({"ok": False, "error": "Session not verified."}, HTTPStatus.UNAUTHORIZED)
                return
            ns = s["n"]
            cl = _cloud_from_session(s)
            ap = _aws_profile_from_session(s)
            inner = "du -sk /bitnami/kafka/data/* 2>/dev/null | sort -rn | head -n 400"
            r = kx.kafka_bash_exec(ns, inner, timeout=180, aws_profile=ap, cloud=cl)
            tbl = kparse.aggregate_du_kafka_data_by_topic(r.stdout or "")
            stk = stack.probe_stack_in_namespace(ns, aws_profile=ap, cloud=cl)
            n_default = 3
            if stk.get("ok") and isinstance(stk.get("components"), dict):
                kc = stk["components"].get("kafka") or {}
                try:
                    n_default = int(kc.get("replicas") or 3)
                except (TypeError, ValueError):
                    n_default = 3
            n_default = max(1, min(n_default, 32))
            self._json(
                {
                    "ok": r.ok,
                    "table": tbl,
                    "default_num_brokers": n_default,
                    "stdout": "" if r.ok else kx.tail_lines(r.stdout or "", 2000),
                    "stderr": kx.filter_kubectl_noise(r.stderr),
                    "returncode": r.returncode,
                }
            )
            return

        if path == "/api/k8s/verify":
            namespace_hint = str(body.get("namespace") or "").strip()
            connect = body.get("connect")
            cloud = "aws"
            region_save: str | None = None
            aws_cluster_saved = ""
            azure_rg_saved = ""
            azure_name_saved = ""

            try:
                aws_profile = kx.sanitize_aws_profile(str(body.get("aws_profile") or ""))
            except ValueError as e:
                self._json({"ok": False, "error": str(e)}, HTTPStatus.BAD_REQUEST)
                return

            if isinstance(connect, dict) and str(connect.get("cloud") or "").strip():
                try:
                    cloud = reg.sanitize_cloud(str(connect.get("cloud") or ""))
                    region_save = reg.sanitize_region(str(connect.get("region") or ""))
                except ValueError as e:
                    self._json({"ok": False, "error": str(e)}, HTTPStatus.BAD_REQUEST)
                    return
                if cloud == "aws":
                    if aws_profile and not aws_profiles.is_allowed_profile(aws_profile):
                        self._json(
                            {"ok": False, "error": "AWS profile must be one of the preset options."},
                            HTTPStatus.BAD_REQUEST,
                        )
                        return
                    try:
                        cname = cc.sanitize_eks_cluster_name(str(connect.get("aws_cluster_name") or ""))
                    except ValueError as e:
                        self._json({"ok": False, "error": str(e)}, HTTPStatus.BAD_REQUEST)
                        return
                    up = cc.update_kubeconfig_aws(cname, region_save, aws_profile)
                    if not up.ok:
                        self._json(
                            {
                                "ok": False,
                                "error": "aws eks update-kubeconfig failed",
                                "detail": up.stderr or up.stdout,
                            },
                            HTTPStatus.BAD_GATEWAY,
                        )
                        return
                    cluster_name = cname
                    aws_cluster_saved = cname
                else:
                    aws_profile = None
                    try:
                        azure_rg_saved = cc.sanitize_azure_rg(str(connect.get("azure_resource_group") or ""))
                        azure_name_saved = cc.sanitize_azure_aks_name(str(connect.get("azure_cluster_name") or ""))
                    except ValueError as e:
                        self._json({"ok": False, "error": str(e)}, HTTPStatus.BAD_REQUEST)
                        return
                    up = cc.update_kubeconfig_azure(azure_rg_saved, azure_name_saved)
                    if not up.ok:
                        self._json(
                            {
                                "ok": False,
                                "error": "az aks get-credentials failed",
                                "detail": up.stderr or up.stdout,
                            },
                            HTTPStatus.BAD_GATEWAY,
                        )
                        return
                    cluster_name = azure_name_saved
            else:
                cluster_name = str(body.get("cluster_name") or "").strip()
                if not cluster_name:
                    self._json(
                        {
                            "ok": False,
                            "error": "Select a cluster from the list (List clusters) or type the EKS cluster name.",
                        },
                        HTTPStatus.BAD_REQUEST,
                    )
                    return
                if body.get("cloud"):
                    try:
                        cloud = reg.sanitize_cloud(str(body.get("cloud") or ""))
                    except ValueError as e:
                        self._json({"ok": False, "error": str(e)}, HTTPStatus.BAD_REQUEST)
                        return
                if body.get("region"):
                    try:
                        region_save = reg.sanitize_region(str(body.get("region") or ""))
                    except ValueError as e:
                        self._json({"ok": False, "error": str(e)}, HTTPStatus.BAD_REQUEST)
                        return
                if cloud == "aws" and aws_profile and not aws_profiles.is_allowed_profile(aws_profile):
                    self._json(
                        {"ok": False, "error": "AWS profile must be one of the preset options."},
                        HTTPStatus.BAD_REQUEST,
                    )
                    return
                try:
                    if cloud == "aws":
                        cname = cc.sanitize_eks_cluster_name(cluster_name)
                        if not region_save:
                            self._json(
                                {"ok": False, "error": "region is required to switch kubeconfig for AWS."},
                                HTTPStatus.BAD_REQUEST,
                            )
                            return
                        up = cc.update_kubeconfig_aws(cname, region_save, aws_profile)
                        if not up.ok:
                            self._json(
                                {
                                    "ok": False,
                                    "error": "aws eks update-kubeconfig failed",
                                    "detail": up.stderr or up.stdout,
                                },
                                HTTPStatus.BAD_GATEWAY,
                            )
                            return
                        cluster_name = cname
                        aws_cluster_saved = cname
                    else:
                        self._json(
                            {
                                "ok": False,
                                "error": "For Azure, pick a cluster from the list after List clusters.",
                            },
                            HTTPStatus.BAD_REQUEST,
                        )
                        return
                except ValueError as e:
                    self._json({"ok": False, "error": str(e)}, HTTPStatus.BAD_REQUEST)
                    return

            if cloud == "azure":
                aws_profile = None

            cur = kx.kubectl_current_context(aws_profile=aws_profile, cloud=cloud)
            if not cur.ok:
                self._json(
                    {"ok": False, "error": "kubectl current-context failed", "detail": cur.stderr},
                    HTTPStatus.BAD_REQUEST,
                )
                return
            current_context = (cur.stdout or "").strip()
            cluster_server = _cluster_server_for_current_context(aws_profile=aws_profile, cloud=cloud)
            if not _verify_cluster_name(cluster_name, current_context, cluster_server):
                self._json(
                    {
                        "ok": False,
                        "error": "Cluster name does not match current kubectl context or API server string.",
                        "current_context": current_context,
                        "cluster_server": cluster_server,
                        "hint": "Pick the cluster from the list after List clusters, or fix the substring to match the context ARN.",
                    },
                    HTTPStatus.BAD_REQUEST,
                )
                return
            info = kx.kubectl_cluster_info(aws_profile=aws_profile, cloud=cloud)
            if not info.ok:
                self._json(
                    {
                        "ok": False,
                        "error": "Keep-alive failed: kubectl cluster-info",
                        "detail": info.stderr or info.stdout,
                        "hint": "For EKS choose the correct AWS profile; ensure aws/az login is valid for this cloud.",
                    },
                    HTTPStatus.BAD_GATEWAY,
                )
                return
            if namespace_hint:
                try:
                    ns = kx.sanitize_name(namespace_hint, "Namespace")
                except ValueError as e:
                    self._json({"ok": False, "error": str(e)}, HTTPStatus.BAD_REQUEST)
                    return
            else:
                ns = kx.discover_namespace_for_pod(kx.KAFKA_POD, aws_profile=aws_profile, cloud=cloud)
                if not ns:
                    self._json(
                        {"ok": False, "error": f"Pod {kx.KAFKA_POD} not found in any namespace (kubectl get pod -A)."},
                        HTTPStatus.NOT_FOUND,
                    )
                    return
            pod = kx.kubectl_get_pod(ns, aws_profile=aws_profile, cloud=cloud)
            if not pod.ok:
                self._json(
                    {
                        "ok": False,
                        "error": f"Cannot reach pod {kx.KAFKA_POD} in namespace {ns!r}.",
                        "detail": kx.filter_kubectl_noise(pod.stderr),
                    },
                    HTTPStatus.BAD_GATEWAY,
                )
                return
            token = sess.pack(
                cluster_name,
                ns,
                aws_profile=aws_profile,
                cloud=cloud,
                region=region_save,
            )
            try:
                sens.write_last_connected(
                    {
                        "cloud": cloud,
                        "region": region_save or "",
                        "aws_profile": aws_profile or "",
                        "aws_cluster_name": aws_cluster_saved,
                        "azure_resource_group": azure_rg_saved,
                        "azure_cluster_name": azure_name_saved,
                        "namespace": ns,
                        "cluster_substring": cluster_name,
                    }
                )
            except OSError as e:
                sys.stderr.write(f"last_connected write failed: {e}\n")
            stack_info = stack.probe_stack_in_namespace(ns, aws_profile=aws_profile, cloud=cloud)
            if isinstance(stack_info, dict) and stack_info.get("ok"):
                stack_info = dict(stack_info)
                stack_info.pop("spec", None)
            self._json(
                {
                    "ok": True,
                    "namespace": ns,
                    "cloud": cloud,
                    "region": region_save,
                    "aws_profile": aws_profile,
                    "current_context": current_context,
                    "cluster_server": cluster_server,
                    "cluster_info": (info.stdout or "").strip()[:2000],
                    "pod_line": (pod.stdout or "").strip()[:2000],
                    "stack": stack_info,
                    "clickhouse_password_configured": bool(sens.read_clickhouse_password()),
                },
                extra_headers=[("Set-Cookie", self._session_cookie_header(token))],
            )
            return

        if path == "/api/session/clear":
            self._json({"ok": True}, extra_headers=[("Set-Cookie", self._session_clear_header())])
            return

        if path == "/api/clickhouse/exec":
            s = _session_from(self)
            if not s:
                self._json({"ok": False, "error": "Session not verified."}, HTTPStatus.UNAUTHORIZED)
                return
            pw = sens.read_clickhouse_password()
            if not pw:
                self._json(
                    {
                        "ok": False,
                        "error": "ClickHouse password file missing.",
                        "hint": "Create data/sensitive/.ch_password (single line).",
                    },
                    HTTPStatus.SERVICE_UNAVAILABLE,
                )
                return
            sql = str(body.get("sql") or "").strip()
            confirmed = bool(body.get("confirmed"))
            try:
                ch_pod = int(body.get("ch_pod") if body.get("ch_pod") is not None else 0)
            except (TypeError, ValueError):
                ch_pod = 0
            ch_pod = max(0, min(ch_pod, 31))
            ro, reason = kx.classify_sql_risk(sql)
            if not ro and not confirmed:
                self._json(
                    {
                        "ok": False,
                        "needs_confirmation": True,
                        "reason": reason or "non-read-only",
                        "message": "This SQL may write or mutate data. Submit again with confirmed: true after approval.",
                    },
                    HTTPStatus.CONFLICT,
                )
                return
            if not ro and confirmed:
                low = sql.lower()
                if any(x in low for x in ("rm -rf", "into outfile", "url('")):
                    self._json({"ok": False, "error": "SQL blocked even after confirmation."}, HTTPStatus.FORBIDDEN)
                    return
            ns = s["n"]
            cl = _cloud_from_session(s)
            ap = _aws_profile_from_session(s)
            pod_name = f"{kx.CLICKHOUSE_POD_PREFIX}{ch_pod}"
            r = kx.clickhouse_client_query(
                ns,
                pod_name,
                kx.CLICKHOUSE_CONTAINER,
                pw,
                sql,
                timeout=300,
                aws_profile=ap,
                cloud=cl,
            )
            self._json(
                {
                    "ok": r.ok,
                    "stdout": kx.tail_lines(r.stdout, 12000),
                    "stderr": kx.filter_kubectl_noise(r.stderr),
                    "returncode": r.returncode,
                    "cmd_display": r.cmd_display,
                    "pod": pod_name,
                }
            )
            return

        if path == "/api/elasticsearch/exec":
            s = _session_from(self)
            if not s:
                self._json({"ok": False, "error": "Session not verified."}, HTTPStatus.UNAUTHORIZED)
                return
            method = str(body.get("method") or "GET").strip().upper()
            if method != "GET":
                self._json({"ok": False, "error": "Custom Elasticsearch exec supports GET only."}, HTTPStatus.BAD_REQUEST)
                return
            path_q = str(body.get("path") or "").strip()
            if not path_q:
                self._json({"ok": False, "error": "path is required (e.g. /_cluster/health?pretty)"}, HTTPStatus.BAD_REQUEST)
                return
            confirmed = bool(body.get("confirmed"))
            ro, reason = kx.classify_es_custom_get(path_q)
            if not ro and not confirmed:
                self._json(
                    {
                        "ok": False,
                        "needs_confirmation": True,
                        "reason": reason or "path not on read-only allowlist",
                        "message": "Submit again with confirmed: true if you accept this request.",
                    },
                    HTTPStatus.CONFLICT,
                )
                return
            if not ro and confirmed:
                low = path_q.lower()
                if any(x in low for x in ("_bulk", "_doc/", "script", "delete_by_query")):
                    self._json({"ok": False, "error": "Path blocked even after confirmation."}, HTTPStatus.FORBIDDEN)
                    return
            try:
                es_pod = int(body.get("es_pod") if body.get("es_pod") is not None else 0)
            except (TypeError, ValueError):
                es_pod = 0
            es_pod = max(0, min(es_pod, 31))
            ns = s["n"]
            cl = _cloud_from_session(s)
            ap = _aws_profile_from_session(s)
            pod_name = f"{kx.ELASTICSEARCH_POD_PREFIX}{es_pod}"
            r = kx.elasticsearch_curl(
                ns,
                pod_name,
                kx.ELASTICSEARCH_CONTAINER,
                path_q,
                method="GET",
                body=None,
                timeout=180,
                aws_profile=ap,
                cloud=cl,
            )
            self._json(
                {
                    "ok": r.ok,
                    "stdout": kx.tail_lines(r.stdout, 12000),
                    "stderr": kx.filter_kubectl_noise(r.stderr),
                    "returncode": r.returncode,
                    "cmd_display": r.cmd_display,
                    "pod": pod_name,
                    "path_executed": path_q,
                }
            )
            return

        if path == "/api/kafka/exec":
            s = _session_from(self)
            if not s:
                self._json({"ok": False, "error": "Session not verified."}, HTTPStatus.UNAUTHORIZED)
                return
            cmd = str(body.get("command") or "").strip()
            confirmed = bool(body.get("confirmed"))
            ro, reason = kx.classify_command_risk(cmd)
            if not ro and not confirmed:
                self._json(
                    {
                        "ok": False,
                        "needs_confirmation": True,
                        "reason": reason or "non-read-only",
                        "message": "This command appears to perform a write or destructive action. "
                        "Submit again with confirmed: true after operator approval.",
                    },
                    HTTPStatus.CONFLICT,
                )
                return
            if not ro and confirmed:
                # Double-check: still block absolute worst patterns
                low = cmd.lower()
                if any(x in low for x in ("rm -rf", "mkfs", ":(){", "dd if=")):
                    self._json({"ok": False, "error": "Command blocked even after confirmation."}, HTTPStatus.FORBIDDEN)
                    return
            ns = s["n"]
            cl = _cloud_from_session(s)
            r = kx.kafka_bash_exec(
                ns, cmd, timeout=300, aws_profile=_aws_profile_from_session(s), cloud=cl
            )
            self._json(
                {
                    "ok": r.ok,
                    "stdout": kx.tail_lines(r.stdout, 12000),
                    "stderr": kx.filter_kubectl_noise(r.stderr),
                    "returncode": r.returncode,
                    "cmd_display": r.cmd_display,
                    "ran_with_jmx_dynamic": True,
                }
            )
            return

        self.send_error(HTTPStatus.NOT_FOUND)

    def _session_cookie_header(self, token: str) -> str:
        v = urllib.parse.quote(token, safe="")
        return f"{sess.COOKIE_NAME}={v}; HttpOnly; SameSite=Lax; Path=/; Max-Age={12 * 3600}"

    def _session_clear_header(self) -> str:
        return f"{sess.COOKIE_NAME}=; HttpOnly; SameSite=Lax; Path=/; Max-Age=0"


def main() -> None:
    global _httpd
    sens.ensure_sensitive_dir()
    host = _bind_host()
    port = _port()
    # ThreadingHTTPServer picks AF based on host; bracket host for IPv6 literals.
    if ":" in host and not host.startswith("["):
        try:
            socket.inet_pton(socket.AF_INET6, host)
            host_display = f"[{host}]"
        except OSError:
            host_display = host
    else:
        host_display = host
    _httpd = ThreadingHTTPServer((host, port), Handler)
    sys.stderr.write(
        f"Glassbox STS Tool {VERSION} — http://{host_display}:{port}/\n"
        f"Kubectl + read-only Kafka panels (session required). PID {os.getpid()}\n"
    )
    try:
        _httpd.serve_forever()
    except KeyboardInterrupt:
        sys.stderr.write("\nStopped.\n")


if __name__ == "__main__":
    main()
