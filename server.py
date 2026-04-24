#!/usr/bin/env python3
"""
Glassbox STS tool — local web UI for read-only Kafka / ClickHouse / Elasticsearch / PostgreSQL / Clingine / Cassandra diagnostics via kubectl exec.

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

VERSION = "2.0.2"

from backend import aws_profiles  # noqa: E402
from backend import ch_panels as chp  # noqa: E402
from backend import es_panels as esp  # noqa: E402
from backend import kc_panels as kcp  # noqa: E402
from backend import cloud_clusters as cc  # noqa: E402
from backend import kafka_parse as kparse  # noqa: E402
from backend import k8s_exec as kx  # noqa: E402
from backend import regions as reg  # noqa: E402
from backend import sensitive_store as sens  # noqa: E402
from backend import session as sess  # noqa: E402
from backend import pg_cassandra as pgc  # noqa: E402
from backend import stack_components as stack  # noqa: E402
from backend import workload_pod_status as podstat  # noqa: E402

_httpd: ThreadingHTTPServer | None = None

_LOG_DEFAULT_CONT: dict[str, str] = {
    "kafka": kx.KAFKA_CONTAINER,
    "kafkaconnect": kx.KAFKA_CONNECT_CONTAINER,
    "clickhouse": kx.CLICKHOUSE_CONTAINER,
    "elasticsearch": kx.ELASTICSEARCH_CONTAINER,
    "opensearch": kx.OPENSEARCH_CONTAINER,
    "postgresql": kx.POSTGRES_CONTAINER,
    "cassandra": kx.CASSANDRA_CONTAINER,
    "clingine": kx.CLINGINE_CONTAINER,
}


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
    "consumer_groups_list": f"{kx.KAFKA_BIN}/kafka-consumer-groups.sh --bootstrap-server {kx.BOOTSTRAP} --list; --describe --all-groups; plus du on /bitnami/kafka/data/* to sum on-broker log sizes (MB) for topics assigned to each group",
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


def _os_pod_index_from_qs(qs: dict[str, list[str]]) -> int:
    raw = (qs.get("os_pod") or ["0"])[0].strip()
    try:
        n = int(raw)
    except ValueError:
        return 0
    return max(0, min(n, 31))


def _kc_pod_index_from_qs(qs: dict[str, list[str]]) -> int:
    raw = (qs.get("kc_pod") or ["0"])[0].strip()
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


def _shards_substring_from_qs(qs: dict[str, list[str]]) -> str:
    try:
        return esp.sanitize_shards_substring((qs.get("shards_substring") or [""])[0])
    except ValueError as e:
        raise ValueError(str(e)) from e


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
echo '---GB_STS_LEADER_DESCRIBE---'
cat "$D"
echo '---GB_STS_LEADER_COUNTS---'
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


def _workload_logs_stream(
    handler: SimpleHTTPRequestHandler, qs: dict[str, list[str]], *, default_workload: str | None
) -> None:
    s = _session_from(handler)
    if not s:
        handler._json({"ok": False, "error": "Session not verified."}, HTTPStatus.UNAUTHORIZED)
        return
    workload_raw = (qs.get("workload") or ([default_workload] if default_workload else [""]))[0].strip().lower()
    if workload_raw not in _LOG_DEFAULT_CONT:
        handler._json(
            {
                "ok": False,
                "error": "Missing or invalid workload (use kafka, kafkaconnect, clickhouse, elasticsearch, opensearch, postgresql, cassandra, clingine).",
            },
            HTTPStatus.BAD_REQUEST,
        )
        return
    workload = workload_raw
    pod_raw = (qs.get("pod") or [""])[0].strip()
    def_cont = _LOG_DEFAULT_CONT[workload]
    cont_raw = (qs.get("container") or [def_cont])[0].strip()
    pod_name = kx.sanitize_pod_for_workload(workload, pod_raw)
    if not pod_name:
        handler._json(
            {"ok": False, "error": f"Invalid pod for workload {workload!r}."},
            HTTPStatus.BAD_REQUEST,
        )
        return
    container = kx.sanitize_workload_logs_container(workload, cont_raw)
    if not container:
        handler._json(
            {"ok": False, "error": f"Invalid container for workload {workload!r} (expected {def_cont!r})."},
            HTTPStatus.BAD_REQUEST,
        )
        return
    ns = str(s["n"])
    cl = _cloud_from_session(s)
    ap = _aws_profile_from_session(s)
    env = kx.subprocess_env(ap, cl)
    if workload == "clingine":
        logq = shlex.quote(kx.CLINGINE_LOG_FILE)
        inner = (
            f"if [ -f {logq} ]; then tail -n 200 -f {logq}; "
            f"else echo '[gb_sts] missing {kx.CLINGINE_LOG_FILE}' >&2; tail -f /dev/null; fi"
        )
        argv = [
            "kubectl",
            "exec",
            "-n",
            ns,
            pod_name,
            "-c",
            container,
            "--",
            "bash",
            "-lc",
            inner,
        ]
    else:
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

    def _search_engine_panel_get(self, qs: dict[str, list[str]], *, engine: str) -> None:
        """Elasticsearch or OpenSearch HTTP panels (shared paths on localhost:9200 in pod)."""
        s = _session_from(self)
        if not s:
            self._json({"ok": False, "error": "Session not verified. Connect from Home first."}, HTTPStatus.UNAUTHORIZED)
            return
        ns = s["n"]
        cl = _cloud_from_session(s)
        ap = _aws_profile_from_session(s)
        panel = (qs.get("panel") or [""])[0].strip()
        if engine == "elasticsearch":
            if panel not in esp.ES_PANEL_IDS:
                self._json({"ok": False, "error": "unknown panel"}, HTTPStatus.BAD_REQUEST)
                return
            es_panel = panel
            es_pod = _es_pod_index_from_qs(qs)
            pod_name = f"{kx.ELASTICSEARCH_POD_PREFIX}{es_pod}"
            container = kx.ELASTICSEARCH_CONTAINER
            cmd_hint_map = esp.ES_PANEL_CMD_HINT
        elif engine == "opensearch":
            if panel not in esp.OS_PANEL_IDS:
                self._json({"ok": False, "error": "unknown OpenSearch panel"}, HTTPStatus.BAD_REQUEST)
                return
            es_panel = esp.os_panel_to_es_panel(panel)
            es_pod = _os_pod_index_from_qs(qs)
            pod_name = f"{kx.OPENSEARCH_POD_PREFIX}{es_pod}"
            container = kx.OPENSEARCH_CONTAINER
            cmd_hint_map = esp.OS_PANEL_CMD_HINT
        else:
            self._json({"ok": False, "error": "internal: bad engine"}, HTTPStatus.INTERNAL_SERVER_ERROR)
            return

        idx_health = _idx_health_from_qs(qs)
        idx_state = _idx_state_from_qs(qs)
        try:
            idx_sub = esp.sanitize_idx_substring((qs.get("idx_substring") or [""])[0])
        except ValueError as e:
            self._json({"ok": False, "error": str(e)}, HTTPStatus.BAD_REQUEST)
            return
        try:
            shards_sub = _shards_substring_from_qs(qs)
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
                es_panel,
                index_pattern=index_pattern,
                alloc_body=alloc_body,
            )
        except ValueError as e:
            self._json({"ok": False, "error": str(e)}, HTTPStatus.BAD_REQUEST)
            return
        timeout = (
            240
            if es_panel
            in ("es_nodes_stats", "es_cluster_stats", "es_cluster_state_metadata", "es_cluster_health_shards")
            else 150
        )
        r = kx.elasticsearch_curl(
            ns,
            pod_name,
            container,
            path_qs,
            method=method,
            body=req_body,
            timeout=timeout,
            aws_profile=ap,
            cloud=cl,
        )
        out_stdout = r.stdout or ""
        if es_panel == "es_cat_indices" and r.ok:
            # Health/state only on the server; index-name substring is applied in the UI on the loaded table.
            out_stdout = esp.postprocess_cat_indices(out_stdout, idx_health, idx_state, "")
        table: list[list[str]] | None = None
        if r.ok:
            if es_panel == "es_cat_indices":
                table = esp.table_from_cat_indices_stdout(out_stdout)
            elif es_panel == "es_cat_indices_named":
                table = esp.table_from_cat_indices_stdout(out_stdout)
            elif es_panel == "es_cat_shards":
                table = esp.table_from_cat_shards_stdout(out_stdout, "")
            elif es_panel == "es_cat_shards_named":
                table = esp.table_from_cat_shards_stdout(out_stdout, "")
            elif es_panel == "es_cat_recovery":
                table = esp.table_from_cat_recovery_stdout(out_stdout, "")
            elif es_panel == "es_cat_shard_stores":
                table = esp.table_from_cat_shard_stores_stdout(out_stdout, "")
        tail_n = 20000 if es_panel in ("es_nodes_stats", "es_cluster_stats") else 12000
        out_payload: dict = {
            "ok": r.ok,
            "panel": panel,
            "stdout": kx.tail_lines(out_stdout, tail_n),
            "stderr": kx.filter_kubectl_noise(r.stderr),
            "returncode": r.returncode,
            "cmd_display": r.cmd_display,
            "cmd_hint": cmd_hint_map.get(panel, ""),
            "http_method": method,
            "path_executed": path_qs,
            "pod": pod_name,
        }
        if table is not None:
            out_payload["table"] = table
        self._json(out_payload)

    def _search_engine_exec_post(self, body: dict, *, engine: str) -> None:
        s = _session_from(self)
        if not s:
            self._json({"ok": False, "error": "Session not verified."}, HTTPStatus.UNAUTHORIZED)
            return
        method = str(body.get("method") or "GET").strip().upper()
        if method != "GET":
            self._json(
                {"ok": False, "error": f"Custom {engine} exec supports GET only."},
                HTTPStatus.BAD_REQUEST,
            )
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
        if engine == "elasticsearch":
            try:
                pod_i = int(body.get("es_pod") if body.get("es_pod") is not None else 0)
            except (TypeError, ValueError):
                pod_i = 0
            pod_i = max(0, min(pod_i, 31))
            pod_name = f"{kx.ELASTICSEARCH_POD_PREFIX}{pod_i}"
            container = kx.ELASTICSEARCH_CONTAINER
        else:
            try:
                pod_i = int(body.get("os_pod") if body.get("os_pod") is not None else 0)
            except (TypeError, ValueError):
                pod_i = 0
            pod_i = max(0, min(pod_i, 31))
            pod_name = f"{kx.OPENSEARCH_POD_PREFIX}{pod_i}"
            container = kx.OPENSEARCH_CONTAINER
        ns = s["n"]
        cl = _cloud_from_session(s)
        ap = _aws_profile_from_session(s)
        r = kx.elasticsearch_curl(
            ns,
            pod_name,
            container,
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

    def _kafka_connect_panel_get(self, qs: dict[str, list[str]]) -> None:
        s = _session_from(self)
        if not s:
            self._json({"ok": False, "error": "Session not verified. Connect from Home first."}, HTTPStatus.UNAUTHORIZED)
            return
        panel = (qs.get("panel") or [""])[0].strip()
        connector = (qs.get("connector") or [""])[0].strip()
        panel_filter = (qs.get("panel_filter") or [""])[0].strip()[:200]
        try:
            method, path_qs = kcp.request_for_panel(panel, connector=connector)
        except ValueError as e:
            self._json({"ok": False, "error": str(e)}, HTTPStatus.BAD_REQUEST)
            return
        kc_pod = _kc_pod_index_from_qs(qs)
        pod_name = f"{kx.KAFKA_CONNECT_POD_PREFIX}{kc_pod}"
        ns = s["n"]
        cl = _cloud_from_session(s)
        ap = _aws_profile_from_session(s)
        r = kx.kafka_connect_curl(
            ns,
            pod_name,
            kx.KAFKA_CONNECT_CONTAINER,
            path_qs,
            method=method,
            body=None,
            timeout=160,
            aws_profile=ap,
            cloud=cl,
        )
        out_stdout = r.stdout or ""
        table = kcp.table_for_panel(panel, out_stdout, panel_filter=panel_filter) if r.ok else None
        out: dict = {
            "ok": r.ok,
            "panel": panel,
            "stdout": kx.tail_lines(out_stdout, 15000),
            "stderr": kx.filter_kubectl_noise(r.stderr),
            "returncode": r.returncode,
            "cmd_display": r.cmd_display,
            "cmd_hint": kcp.KC_PANEL_CMD_HINT.get(panel, ""),
            "http_method": method,
            "path_executed": path_qs,
            "pod": pod_name,
            "connector": connector,
            "panel_filter": panel_filter,
        }
        if table is not None:
            out["table"] = table
        self._json(out)

    def do_GET(self) -> None:  # noqa: N802
        parsed = urllib.parse.urlparse(self.path)
        path = parsed.path
        qs = urllib.parse.parse_qs(parsed.query)

        if path == "/api/about":
            self._json({
                "version": VERSION,
                "name": "Glassbox STS Tool",
                "description": "Read-first local dashboard: verify kubectl context, then inspect Kafka / Kafka Connect / ClickHouse / Elasticsearch / OpenSearch in-cluster via kubectl exec.",
                "kafka_pod": kx.KAFKA_POD,
                "kafkaconnect_pod_prefix": kx.KAFKA_CONNECT_POD_PREFIX,
                "clickhouse_pod_prefix": kx.CLICKHOUSE_POD_PREFIX,
                "elasticsearch_pod_prefix": kx.ELASTICSEARCH_POD_PREFIX,
                "opensearch_pod_prefix": kx.OPENSEARCH_POD_PREFIX,
            })
            return

        if path == "/api/aws/profiles":
            self._json(aws_profiles.profiles_api_payload())
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
                "postgres_password_configured": bool(sens.read_postgres_password()),
                "workload_log_containers": {
                    "kafka": kx.KAFKA_CONTAINER,
                    "kafkaconnect": kx.KAFKA_CONNECT_CONTAINER,
                    "clickhouse": kx.CLICKHOUSE_CONTAINER,
                    "elasticsearch": kx.ELASTICSEARCH_CONTAINER,
                    "opensearch": kx.OPENSEARCH_CONTAINER,
                    "postgresql": kx.POSTGRES_CONTAINER,
                    "cassandra": kx.CASSANDRA_CONTAINER,
                    "clingine": kx.CLINGINE_CONTAINER,
                },
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

        if path == "/api/k8s/workload-pod-status":
            s = _session_from(self)
            if not s:
                self._json({"ok": False, "error": "Session not verified."}, HTTPStatus.UNAUTHORIZED)
                return
            wl = (qs.get("workload") or [""])[0].strip().lower()
            ns = s["n"]
            cl = _cloud_from_session(s)
            ap = _aws_profile_from_session(s)
            self._json(podstat.fetch_workload_pod_status(ns, wl, aws_profile=ap, cloud=cl))
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
            ch_database = (qs.get("database") or ["glassbox"])[0].strip() or "glassbox"
            try:
                sql = chp.sql_for_panel(
                    panel,
                    event_table=event_table,
                    hours=hours,
                    from_ts=from_ts,
                    to_ts=to_ts,
                    tenant_id=tenant_id,
                    database=ch_database,
                )
            except ValueError as e:
                self._json({"ok": False, "error": str(e)}, HTTPStatus.BAD_REQUEST)
                return
            pod_name = f"{kx.CLICKHOUSE_POD_PREFIX}{ch_pod}"
            explore = panel in ("ch_explore_databases", "ch_explore_tables")
            out_fmt = "TabSeparatedWithNames" if explore else "PrettyCompact"
            r = kx.clickhouse_client_query(
                ns,
                pod_name,
                kx.CLICKHOUSE_CONTAINER,
                pw,
                sql,
                timeout=300,
                aws_profile=ap,
                cloud=cl,
                output_format=out_fmt,
            )
            table_rows: list[list[str]] = []
            if explore and r.ok:
                table_rows = kx.parse_clickhouse_tsv_with_header(r.stdout or "")
            payload: dict = {
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
            if explore:
                payload["table"] = table_rows
            self._json(payload)
            return

        if path == "/api/elasticsearch/panel":
            self._search_engine_panel_get(qs, engine="elasticsearch")
            return

        if path == "/api/opensearch/panel":
            self._search_engine_panel_get(qs, engine="opensearch")
            return

        if path == "/api/kafkaconnect/panel":
            self._kafka_connect_panel_get(qs)
            return

        if path == "/api/kafka/logs/stream":
            _workload_logs_stream(self, qs, default_workload="kafka")
            return

        if path == "/api/k8s/logs/stream":
            _workload_logs_stream(self, qs, default_workload=None)
            return

        if path == "/api/postgres/panel":
            s = _session_from(self)
            if not s:
                self._json({"ok": False, "error": "Session not verified. Connect from Home first."}, HTTPStatus.UNAUTHORIZED)
                return
            pw = sens.read_postgres_password()
            if not pw:
                self._json(
                    {
                        "ok": False,
                        "error": "PostgreSQL password file missing.",
                        "hint": "Create data/sensitive/.pg_password (single line, PGPASSWORD for user clarisite).",
                    },
                    HTTPStatus.SERVICE_UNAVAILABLE,
                )
                return
            panel = (qs.get("panel") or [""])[0].strip()
            pg_pod = (qs.get("pg_pod") or [""])[0].strip()
            pod_name = kx.sanitize_pod_for_workload("postgresql", pg_pod)
            if not pod_name:
                self._json({"ok": False, "error": "Invalid pg_pod."}, HTTPStatus.BAD_REQUEST)
                return
            ns = s["n"]
            cl = _cloud_from_session(s)
            ap = _aws_profile_from_session(s)
            if panel == "databases":
                sql = pgc.postgres_databases_sql()
                r = kx.postgres_psql_query(
                    ns, pod_name, kx.POSTGRES_CONTAINER, pw, sql, timeout=120, aws_profile=ap, cloud=cl
                )
                self._json(
                    {
                        "ok": r.ok,
                        "panel": panel,
                        "stdout": kx.tail_lines(r.stdout, 8000),
                        "stderr": kx.filter_kubectl_noise(r.stderr),
                        "returncode": r.returncode,
                        "cmd_display": r.cmd_display,
                        "pod": pod_name,
                    }
                )
                return
            if panel == "schemas":
                sql = pgc.postgres_schemas_sql()
                r = kx.postgres_psql_query(
                    ns, pod_name, kx.POSTGRES_CONTAINER, pw, sql, timeout=120, aws_profile=ap, cloud=cl
                )
                self._json(
                    {
                        "ok": r.ok,
                        "panel": panel,
                        "stdout": kx.tail_lines(r.stdout, 8000),
                        "stderr": kx.filter_kubectl_noise(r.stderr),
                        "returncode": r.returncode,
                        "cmd_display": r.cmd_display,
                        "pod": pod_name,
                    }
                )
                return
            if panel == "tables":
                try:
                    schema = pgc.sanitize_postgres_schema((qs.get("schema") or ["public"])[0])
                except ValueError as e:
                    self._json({"ok": False, "error": str(e)}, HTTPStatus.BAD_REQUEST)
                    return
                sql = pgc.postgres_tables_sql(schema)
                r = kx.postgres_psql_query(
                    ns, pod_name, kx.POSTGRES_CONTAINER, pw, sql, timeout=120, aws_profile=ap, cloud=cl
                )
                out_tbl: dict = {
                    "ok": r.ok,
                    "panel": panel,
                    "schema": schema,
                    "stdout": kx.tail_lines(r.stdout, 8000),
                    "stderr": kx.filter_kubectl_noise(r.stderr),
                    "returncode": r.returncode,
                    "cmd_display": r.cmd_display,
                    "pod": pod_name,
                }
                if r.ok:
                    names = [ln.strip() for ln in (r.stdout or "").splitlines() if ln.strip()]
                    out_tbl["table"] = [["Table"]] + [[n] for n in names[:500]]
                self._json(out_tbl)
                return
            self._json({"ok": False, "error": "unknown panel"}, HTTPStatus.BAD_REQUEST)
            return

        if path == "/api/cassandra/panel":
            s = _session_from(self)
            if not s:
                self._json({"ok": False, "error": "Session not verified. Connect from Home first."}, HTTPStatus.UNAUTHORIZED)
                return
            panel = (qs.get("panel") or [""])[0].strip()
            cass_pod = (qs.get("cass_pod") or [""])[0].strip()
            pod_name = kx.sanitize_pod_for_workload("cassandra", cass_pod)
            if not pod_name:
                self._json({"ok": False, "error": "Invalid cass_pod."}, HTTPStatus.BAD_REQUEST)
                return
            ns = s["n"]
            cl = _cloud_from_session(s)
            ap = _aws_profile_from_session(s)
            if panel == "keyspaces":
                cql = pgc.cassandra_keyspaces_cql()
            elif panel == "desc_keyspace":
                try:
                    cql = pgc.cassandra_desc_keyspace_cql((qs.get("keyspace") or [""])[0])
                except ValueError as e:
                    self._json({"ok": False, "error": str(e)}, HTTPStatus.BAD_REQUEST)
                    return
            elif panel == "tables":
                try:
                    cql = pgc.cassandra_tables_cql((qs.get("keyspace") or [""])[0])
                except ValueError as e:
                    self._json({"ok": False, "error": str(e)}, HTTPStatus.BAD_REQUEST)
                    return
            else:
                self._json({"ok": False, "error": "unknown panel"}, HTTPStatus.BAD_REQUEST)
                return
            r = kx.cassandra_cqlsh_query(ns, pod_name, kx.CASSANDRA_CONTAINER, cql, timeout=120, aws_profile=ap, cloud=cl)
            out_payload: dict = {
                "ok": r.ok,
                "panel": panel,
                "stdout": kx.tail_lines(r.stdout, 8000),
                "stderr": kx.filter_kubectl_noise(r.stderr),
                "returncode": r.returncode,
                "cmd_display": r.cmd_display,
                "pod": pod_name,
                "cql_executed": cql,
            }
            if panel == "keyspaces" and r.ok:
                ks_names = pgc.parse_cassandra_keyspaces_cqlsh_stdout(r.stdout)
                out_payload["table"] = [["Keyspace name"]] + [[n] for n in ks_names]
            self._json(out_payload)
            return

        if path == "/api/cassandra/nodetool":
            s = _session_from(self)
            if not s:
                self._json({"ok": False, "error": "Session not verified. Connect from Home first."}, HTTPStatus.UNAUTHORIZED)
                return
            op = (qs.get("op") or [""])[0].strip().lower()
            cass_pod = (qs.get("cass_pod") or [""])[0].strip()
            pod_name = kx.sanitize_pod_for_workload("cassandra", cass_pod)
            if not pod_name:
                self._json({"ok": False, "error": "Invalid cass_pod."}, HTTPStatus.BAD_REQUEST)
                return
            ns = s["n"]
            cl = _cloud_from_session(s)
            ap = _aws_profile_from_session(s)
            r = kx.cassandra_nodetool_readonly(
                ns, pod_name, kx.CASSANDRA_CONTAINER, op, timeout=240, aws_profile=ap, cloud=cl
            )
            self._json(
                {
                    "ok": r.ok,
                    "op": op,
                    "stdout": kx.tail_lines(r.stdout, 25000),
                    "stderr": kx.filter_kubectl_noise(r.stderr),
                    "returncode": r.returncode,
                    "cmd_display": r.cmd_display,
                    "pod": pod_name,
                }
            )
            return

        if path == "/api/clingine/panel":
            s = _session_from(self)
            if not s:
                self._json({"ok": False, "error": "Session not verified. Connect from Home first."}, HTTPStatus.UNAUTHORIZED)
                return
            panel = (qs.get("panel") or [""])[0].strip()
            clg_pod = (qs.get("clingine_pod") or [""])[0].strip()
            pod_name = kx.sanitize_pod_for_workload("clingine", clg_pod)
            if not pod_name:
                self._json({"ok": False, "error": "Invalid clingine_pod."}, HTTPStatus.BAD_REQUEST)
                return
            ns = s["n"]
            cl = _cloud_from_session(s)
            ap = _aws_profile_from_session(s)
            if panel == "keyspaces":
                cql = pgc.cassandra_keyspaces_cql()
            elif panel == "desc_keyspace":
                try:
                    cql = pgc.cassandra_desc_keyspace_cql((qs.get("keyspace") or [""])[0])
                except ValueError as e:
                    self._json({"ok": False, "error": str(e)}, HTTPStatus.BAD_REQUEST)
                    return
            elif panel == "tables":
                try:
                    cql = pgc.cassandra_tables_cql((qs.get("keyspace") or [""])[0])
                except ValueError as e:
                    self._json({"ok": False, "error": str(e)}, HTTPStatus.BAD_REQUEST)
                    return
            else:
                self._json({"ok": False, "error": "unknown panel"}, HTTPStatus.BAD_REQUEST)
                return
            r = kx.clingine_cqlsh_via_cassandra_proxy(
                ns, pod_name, cql, timeout=120, aws_profile=ap, cloud=cl
            )
            out_payload: dict = {
                "ok": r.ok,
                "panel": panel,
                "stdout": kx.tail_lines(r.stdout, 8000),
                "stderr": kx.filter_kubectl_noise(r.stderr),
                "returncode": r.returncode,
                "cmd_display": r.cmd_display,
                "pod": pod_name,
                "cql_executed": cql,
                "cql_via": "cqlsh on Cassandra pod (see GB_STS_CLINGINE_CQLSH_PROXY_POD) with CQLSH_HOST=clingine-external-<n> CQLSH_PORT=8186",
            }
            if panel == "keyspaces" and r.ok:
                ks_names = pgc.parse_cassandra_keyspaces_cqlsh_stdout(r.stdout)
                out_payload["table"] = [["Keyspace name"]] + [[n] for n in ks_names]
            if panel == "tables" and r.ok:
                tnames = pgc.parse_cassandra_tables_cqlsh_stdout(r.stdout)
                out_payload["table"] = [["Table name"]] + [[n] for n in tnames]
            self._json(out_payload)
            return

        if path == "/api/clingine/nodetool":
            s = _session_from(self)
            if not s:
                self._json({"ok": False, "error": "Session not verified. Connect from Home first."}, HTTPStatus.UNAUTHORIZED)
                return
            op = (qs.get("op") or [""])[0].strip().lower()
            clg_pod = (qs.get("clingine_pod") or [""])[0].strip()
            pod_name = kx.sanitize_pod_for_workload("clingine", clg_pod)
            if not pod_name:
                self._json({"ok": False, "error": "Invalid clingine_pod."}, HTTPStatus.BAD_REQUEST)
                return
            ns = s["n"]
            cl = _cloud_from_session(s)
            ap = _aws_profile_from_session(s)
            nt_bin = (os.environ.get("GB_STS_CLINGINE_NODETOOL") or kx.CLINGINE_NODETOOL_DEFAULT).strip() or kx.CLINGINE_NODETOOL_DEFAULT
            nt_ks = (qs.get("nt_keyspace") or [""])[0].strip()
            nt_tb = (qs.get("nt_table") or [""])[0].strip()
            if op == "tablestats":
                if not nt_ks or not nt_tb:
                    self._json(
                        {
                            "ok": False,
                            "error": "Clingine tablestats requires nt_keyspace and nt_table (single keyspace.table).",
                        },
                        HTTPStatus.BAD_REQUEST,
                    )
                    return
                try:
                    pgc.sanitize_cassandra_keyspace(nt_ks)
                    pgc.sanitize_cassandra_table(nt_tb)
                except ValueError as e:
                    self._json({"ok": False, "error": str(e)}, HTTPStatus.BAD_REQUEST)
                    return
            r = kx.clingine_nodetool_readonly(
                ns,
                pod_name,
                kx.CLINGINE_CONTAINER,
                op,
                timeout=240,
                aws_profile=ap,
                cloud=cl,
                nodetool_bin=nt_bin,
                tablestats_keyspace=nt_ks if op == "tablestats" else None,
                tablestats_table=nt_tb if op == "tablestats" else None,
            )
            self._json(
                {
                    "ok": r.ok,
                    "op": op,
                    "stdout": kx.tail_lines(r.stdout, 25000),
                    "stderr": kx.filter_kubectl_noise(r.stderr),
                    "returncode": r.returncode,
                    "cmd_display": r.cmd_display,
                    "pod": pod_name,
                }
            )
            return

        if path == "/api/postgres/tenant_hint":
            s = _session_from(self)
            if not s:
                self._json({"ok": False, "error": "Session not verified."}, HTTPStatus.UNAUTHORIZED)
                return
            pw = sens.read_postgres_password()
            if not pw:
                self._json({"ok": True, "tenant_id": None, "note": "postgres password not configured"})
                return
            ns = s["n"]
            cl = _cloud_from_session(s)
            ap = _aws_profile_from_session(s)
            pg_pod_raw = (qs.get("pg_pod") or [""])[0].strip()
            pod_name = kx.sanitize_pod_for_workload("postgresql", pg_pod_raw) if pg_pod_raw else None
            if not pod_name:
                stk = stack.probe_stack_in_namespace(ns, aws_profile=ap, cloud=cl)
                pods = ((stk.get("components") or {}).get("postgresql") or {}).get("pods") or []
                if pods:
                    pod_name = kx.sanitize_pod_for_workload("postgresql", pods[0])
                if not pod_name:
                    pod_name = kx.sanitize_pod_for_workload("postgresql", "glassbox-postgresql-0")
            if not pod_name:
                self._json({"ok": True, "tenant_id": None, "note": "no postgres pod name resolved"})
                return
            sql = pgc.postgres_clarisite_tenant_id_sql()
            r = kx.postgres_psql_query(
                ns, pod_name, kx.POSTGRES_CONTAINER, pw, sql, timeout=60, aws_profile=ap, cloud=cl
            )
            tid = (r.stdout or "").strip() if r.ok else ""
            self._json(
                {
                    "ok": r.ok,
                    "tenant_id": tid or None,
                    "pod": pod_name,
                    "stderr": kx.filter_kubectl_noise(r.stderr) if r.stderr else "",
                    "returncode": r.returncode,
                }
            )
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
                raw = r.stdout or ""
                describe_part = raw
                counts_part = raw
                mark_desc = "---GB_STS_LEADER_DESCRIBE---"
                mark_counts = "---GB_STS_LEADER_COUNTS---"
                if mark_desc in raw and mark_counts in raw:
                    _pre, rest = raw.split(mark_desc, 1)
                    describe_part, counts_part = rest.split(mark_counts, 1)
                rows = []
                for line in counts_part.splitlines():
                    m = re.match(r"broker\s+(\d+)\s+(\d+)", line.strip())
                    if m and int(m.group(2)) > 0:
                        rows.append([m.group(1), m.group(2)])
                out["table"] = [["Broker id", "Partitions led"]] + rows
                size_mb: dict[str, str] = {}
                r_du = kx.kafka_bash_exec(
                    ns,
                    "du -sk /bitnami/kafka/data/* 2>/dev/null | sort -rn | head -n 600",
                    timeout=220,
                    aws_profile=ap,
                    cloud=cl,
                )
                if r_du.ok:
                    by_topic = kparse.aggregate_du_kafka_data_by_topic(r_du.stdout or "")
                    if len(by_topic) > 1:
                        for rr in by_topic[1:]:
                            if len(rr) >= 2:
                                size_mb[str(rr[0])] = str(rr[1])
                else:
                    out.setdefault("errors", {})
                    out["errors"]["leader_balance_sizes"] = kx.filter_kubectl_noise(r_du.stderr) or "du scan failed"
                led_map = {str(rr[0]): int(rr[1]) for rr in rows}
                bundle = kparse.build_leader_balance_insights(
                    describe_part or "",
                    broker_led=led_map,
                    topic_sizes_mb=size_mb,
                )
                if bundle:
                    out.update(bundle)
            elif panel == "consumer_groups_list":
                list_body, desc_body = kparse.consumer_groups_list_split_panel_stdout(r.stdout or "")
                names = kparse.parse_consumer_group_names(list_body)
                out["groups"] = names
                topic_kb = {}
                r_du = kx.kafka_bash_exec(
                    ns,
                    "du -sk /bitnami/kafka/data/* 2>/dev/null | sort -rn | head -n 600",
                    timeout=220,
                    aws_profile=ap,
                    cloud=cl,
                )
                if r_du.ok:
                    topic_kb = kparse.topic_total_kb_from_du_stdout(r_du.stdout or "")
                else:
                    out.setdefault("errors", {})
                    out["errors"]["consumer_groups_sizes"] = kx.filter_kubectl_noise(r_du.stderr) or "du scan failed"
                gt = kparse.parse_consumer_group_assigned_topics(desc_body)
                rows_tbl: list[list[str]] = []
                for n in names:
                    topics = gt.get(n) or set()
                    kb_sum = sum(topic_kb.get(t, 0) for t in topics)
                    if kb_sum > 0:
                        mb = kb_sum / 1024.0
                        rows_tbl.append([n, f"{mb:.2f}"])
                    elif topic_kb:
                        rows_tbl.append([n, "0"])
                    else:
                        rows_tbl.append([n, "—"])

                def _consumer_group_mb_sort_key(row: list[str]) -> float:
                    if len(row) < 2:
                        return float("-inf")
                    s = (row[1] or "").strip()
                    if s in ("—", "-", ""):
                        return float("-inf")
                    try:
                        return float(s)
                    except ValueError:
                        return float("-inf")

                rows_tbl.sort(key=_consumer_group_mb_sort_key, reverse=True)
                out["table"] = [["Group", "Topics size (MB)"]] + rows_tbl
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
                # Default namespace even when Kafka (or other workloads) is absent.
                ns = "default"
            ns_probe = kx.kubectl_list_pod_names(ns, aws_profile=aws_profile, cloud=cloud)
            if not ns_probe.ok:
                self._json(
                    {
                        "ok": False,
                        "error": f"Cannot access namespace {ns!r}.",
                        "detail": kx.filter_kubectl_noise(ns_probe.stderr),
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
                    "pod_line": "",
                    "stack": stack_info,
                    "clickhouse_password_configured": bool(sens.read_clickhouse_password()),
                    "postgres_password_configured": bool(sens.read_postgres_password()),
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

        if path == "/api/postgres/exec":
            s = _session_from(self)
            if not s:
                self._json({"ok": False, "error": "Session not verified."}, HTTPStatus.UNAUTHORIZED)
                return
            pw = sens.read_postgres_password()
            if not pw:
                self._json(
                    {
                        "ok": False,
                        "error": "PostgreSQL password file missing.",
                        "hint": "Create data/sensitive/.pg_password (single line).",
                    },
                    HTTPStatus.SERVICE_UNAVAILABLE,
                )
                return
            sql = str(body.get("sql") or "").strip()
            confirmed = bool(body.get("confirmed"))
            ro, reason = pgc.classify_postgres_sql(sql)
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
                if any(x in low for x in ("rm -rf", ";--", "into outfile", "copy (program", "lo_import", "lo_export")):
                    self._json({"ok": False, "error": "SQL blocked even after confirmation."}, HTTPStatus.FORBIDDEN)
                    return
            pg_pod = str(body.get("pg_pod") or "").strip()
            pod_name = kx.sanitize_pod_for_workload("postgresql", pg_pod)
            if not pod_name:
                self._json({"ok": False, "error": "Invalid pg_pod."}, HTTPStatus.BAD_REQUEST)
                return
            ns = s["n"]
            cl = _cloud_from_session(s)
            ap = _aws_profile_from_session(s)
            r = kx.postgres_psql_query(
                ns, pod_name, kx.POSTGRES_CONTAINER, pw, sql, timeout=300, aws_profile=ap, cloud=cl
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

        if path == "/api/cassandra/exec":
            s = _session_from(self)
            if not s:
                self._json({"ok": False, "error": "Session not verified."}, HTTPStatus.UNAUTHORIZED)
                return
            cql = str(body.get("cql") or "").strip()
            confirmed = bool(body.get("confirmed"))
            ro, reason = pgc.classify_cassandra_cql(cql)
            if not ro and not confirmed:
                self._json(
                    {
                        "ok": False,
                        "needs_confirmation": True,
                        "reason": reason or "non-read-only",
                        "message": "This CQL may write or mutate data. Submit again with confirmed: true after approval.",
                    },
                    HTTPStatus.CONFLICT,
                )
                return
            if not ro and confirmed:
                low = cql.lower()
                if any(x in low for x in ("rm -rf", ";--", "bash", "curl ", "wget ", "| sh")):
                    self._json({"ok": False, "error": "CQL blocked even after confirmation."}, HTTPStatus.FORBIDDEN)
                    return
            cass_pod = str(body.get("cass_pod") or "").strip()
            pod_name = kx.sanitize_pod_for_workload("cassandra", cass_pod)
            if not pod_name:
                self._json({"ok": False, "error": "Invalid cass_pod."}, HTTPStatus.BAD_REQUEST)
                return
            ns = s["n"]
            cl = _cloud_from_session(s)
            ap = _aws_profile_from_session(s)
            r = kx.cassandra_cqlsh_query(
                ns, pod_name, kx.CASSANDRA_CONTAINER, cql, timeout=300, aws_profile=ap, cloud=cl
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

        if path == "/api/clingine/exec":
            s = _session_from(self)
            if not s:
                self._json({"ok": False, "error": "Session not verified."}, HTTPStatus.UNAUTHORIZED)
                return
            cql = str(body.get("cql") or "").strip()
            confirmed = bool(body.get("confirmed"))
            ro, reason = pgc.classify_cassandra_cql(cql)
            if not ro and not confirmed:
                self._json(
                    {
                        "ok": False,
                        "needs_confirmation": True,
                        "reason": reason or "non-read-only",
                        "message": "This CQL may write or mutate data. Submit again with confirmed: true after approval.",
                    },
                    HTTPStatus.CONFLICT,
                )
                return
            if not ro and confirmed:
                low = cql.lower()
                if any(x in low for x in ("rm -rf", ";--", "bash", "curl ", "wget ", "| sh")):
                    self._json({"ok": False, "error": "CQL blocked even after confirmation."}, HTTPStatus.FORBIDDEN)
                    return
            clg_pod = str(body.get("clingine_pod") or "").strip()
            pod_name = kx.sanitize_pod_for_workload("clingine", clg_pod)
            if not pod_name:
                self._json({"ok": False, "error": "Invalid clingine_pod."}, HTTPStatus.BAD_REQUEST)
                return
            ns = s["n"]
            cl = _cloud_from_session(s)
            ap = _aws_profile_from_session(s)
            r = kx.clingine_cqlsh_via_cassandra_proxy(
                ns, pod_name, cql, timeout=300, aws_profile=ap, cloud=cl
            )
            self._json(
                {
                    "ok": r.ok,
                    "stdout": kx.tail_lines(r.stdout, 12000),
                    "stderr": kx.filter_kubectl_noise(r.stderr),
                    "returncode": r.returncode,
                    "cmd_display": r.cmd_display,
                    "pod": pod_name,
                    "cql_via": "cqlsh on Cassandra pod with CQLSH_HOST/CQLSH_PORT to Clingine",
                }
            )
            return

        if path == "/api/elasticsearch/exec":
            self._search_engine_exec_post(body, engine="elasticsearch")
            return

        if path == "/api/opensearch/exec":
            self._search_engine_exec_post(body, engine="opensearch")
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
