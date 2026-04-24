"""Run kubectl locally and exec read-only Kafka diagnostics inside the broker pod."""

from __future__ import annotations

import base64
import json
import os
import random
import re
import shlex
import subprocess
from dataclasses import dataclass
from pathlib import Path

from backend import pg_cassandra as pgcass

KAFKA_POD = "glassbox-kafka-0"
KAFKA_CONTAINER = "kafka"
KAFKA_BIN = "/opt/bitnami/kafka/bin"
BOOTSTRAP = "127.0.0.1:9092"

CLICKHOUSE_POD_PREFIX = "glassbox-clickhouse-"
CLICKHOUSE_CONTAINER = "glassbox-clickhouse"
# Default database for ``clickhouse-client -d`` so unqualified names (e.g. ``beacon_event``) resolve like ``glassbox.beacon_event``.
CLICKHOUSE_DEFAULT_DATABASE = (os.environ.get("GB_STS_CLICKHOUSE_DATABASE") or "glassbox").strip() or "glassbox"
if not re.match(r"^[a-zA-Z_][a-zA-Z0-9_]*$", CLICKHOUSE_DEFAULT_DATABASE):
    CLICKHOUSE_DEFAULT_DATABASE = "glassbox"

ELASTICSEARCH_POD_PREFIX = "glassbox-elasticsearch-master-"
ELASTICSEARCH_CONTAINER = os.environ.get("GB_STS_ES_CONTAINER", "elasticsearch").strip() or "elasticsearch"
ELASTICSEARCH_LOCAL = "http://127.0.0.1:9200"

OPENSEARCH_POD_PREFIX = "glassbox-opensearch-master-"
OPENSEARCH_CONTAINER = os.environ.get("GB_STS_OS_CONTAINER", "opensearch").strip() or "opensearch"

KAFKA_CONNECT_POD_PREFIX = "glassbox-kafkaconnect-"
# Bitnami chart uses the same container name as the broker pod (``kafka``), not ``kafkaconnect``.
KAFKA_CONNECT_CONTAINER = os.environ.get("GB_STS_KAFKA_CONNECT_CONTAINER", KAFKA_CONTAINER).strip() or KAFKA_CONTAINER
KAFKA_CONNECT_LOCAL = "http://127.0.0.1:8083"

# Glassbox chart uses this container name (Bitnami chart default is often ``postgresql``).
POSTGRES_CONTAINER = os.environ.get("GB_STS_PG_CONTAINER", "glassbox-postgresql").strip() or "glassbox-postgresql"
CASSANDRA_CONTAINER = os.environ.get("GB_STS_CASS_CONTAINER", "cassandra").strip() or "cassandra"
CLINGINE_CONTAINER = os.environ.get("GB_STS_CLINGINE_CONTAINER", "clingine").strip() or "clingine"
# Clingine: embedded Cassandra + app logs (not container stdout).
CLINGINE_LOG_FILE = "/root/clingine/log/servers.root.log"
CLINGINE_NODETOOL_DEFAULT = "/root/clingine/bin/nodetool.sh"

_CLINGINE_POD_INDEX = re.compile(r"^clingine-(\d+)$")

# Commands that must never run without an explicit user confirmation flag.
_WRITE_MARKERS = (
    "--delete",
    "--alter",
    "--execute",
    "kafka-delete-records",
    "--reset-offsets",
    "RemoveGroupMembers",
    "reassign-partitions",
    "--add-config",
    "--delete-config",
    "javac ",
    "java -cp",
    "> /tmp/",
    "bash /tmp/",
    "cat <<",
)

_SAFE_TOPIC_GROUP = re.compile(r"^[A-Za-z0-9_.-]+$")

_KAFKA_LOGS_POD = re.compile(r"^glassbox-kafka-(\d+)$")


def sanitize_kafka_logs_pod(pod: str) -> str | None:
    """Allow only ``glassbox-kafka-<n>`` with n in 0..31."""
    m = _KAFKA_LOGS_POD.match((pod or "").strip())
    if not m:
        return None
    i = int(m.group(1))
    if i < 0 or i > 31:
        return None
    return f"glassbox-kafka-{i}"


def sanitize_kafka_logs_container(container: str) -> str | None:
    c = (container or "").strip()
    if c == KAFKA_CONTAINER:
        return c
    return None


_LOG_WORKLOADS = frozenset(
    {"kafka", "clickhouse", "elasticsearch", "opensearch", "kafkaconnect", "postgresql", "cassandra", "clingine"}
)
_CH_LOG_POD = re.compile(r"^glassbox-clickhouse-(\d+)$")
_ES_LOG_POD = re.compile(r"^glassbox-elasticsearch-master-(\d+)$")
_OS_LOG_POD = re.compile(r"^glassbox-opensearch-master-(\d+)$")
_KC_LOG_POD = re.compile(r"^glassbox-kafkaconnect-(\d+)$")
_PG_LOG_SINGLE = re.compile(r"^glassbox-postgresql-(\d+)$")
_PG_LOG_HA = re.compile(r"^glassbox-postgresql-ha-postgresql-(\d+)$")
_CASS_LOG_POD = re.compile(r"^glassbox-cassandra-(\d+)$")
_CLINGINE_LOG_POD = re.compile(r"^clingine-(\d+)$")


def sanitize_pod_for_workload(workload: str, pod: str) -> str | None:
    """Strict pod names for kubectl logs/exec (must match stack chart naming)."""
    w = (workload or "").strip().lower()
    p = (pod or "").strip()
    if w == "kafka":
        return sanitize_kafka_logs_pod(p)
    if w == "clickhouse":
        m = _CH_LOG_POD.match(p)
        if not m:
            return None
        i = int(m.group(1))
        return p if 0 <= i <= 31 else None
    if w == "elasticsearch":
        m = _ES_LOG_POD.match(p)
        if not m:
            return None
        i = int(m.group(1))
        return p if 0 <= i <= 31 else None
    if w == "opensearch":
        m = _OS_LOG_POD.match(p)
        if not m:
            return None
        i = int(m.group(1))
        return p if 0 <= i <= 31 else None
    if w == "kafkaconnect":
        m = _KC_LOG_POD.match(p)
        if not m:
            return None
        i = int(m.group(1))
        return p if 0 <= i <= 31 else None
    if w == "postgresql":
        for rx in (_PG_LOG_SINGLE, _PG_LOG_HA):
            m = rx.match(p)
            if m:
                i = int(m.group(1))
                return p if 0 <= i <= 31 else None
        return None
    if w == "cassandra":
        m = _CASS_LOG_POD.match(p)
        if not m:
            return None
        i = int(m.group(1))
        return p if 0 <= i <= 31 else None
    if w == "clingine":
        m = _CLINGINE_LOG_POD.match(p)
        if not m:
            return None
        i = int(m.group(1))
        return p if 0 <= i <= 31 else None
    return None


def sanitize_workload_logs_container(workload: str, container: str) -> str | None:
    w = (workload or "").strip().lower()
    c = (container or "").strip()
    if w == "kafka" and c == KAFKA_CONTAINER:
        return c
    if w == "clickhouse" and c == CLICKHOUSE_CONTAINER:
        return c
    if w == "elasticsearch" and c == ELASTICSEARCH_CONTAINER:
        return c
    if w == "opensearch" and c == OPENSEARCH_CONTAINER:
        return c
    if w == "kafkaconnect" and c == KAFKA_CONNECT_CONTAINER:
        return c
    if w == "postgresql" and c == POSTGRES_CONTAINER:
        return c
    if w == "cassandra" and c == CASSANDRA_CONTAINER:
        return c
    if w == "clingine" and c == CLINGINE_CONTAINER:
        return c
    return None


@dataclass
class CmdResult:
    ok: bool
    stdout: str
    stderr: str
    returncode: int
    cmd_display: str


def subprocess_env(aws_profile: str | None, cloud: str | None = None) -> dict[str, str]:
    """
    Copy of process env.
    For Azure kubectl, clear AWS_PROFILE so the wrong AWS account is not used.
    For AWS with a named profile, merge keys from ``~/.aws/credentials`` (same idea as exporting
    AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY / AWS_SESSION_TOKEN before aws/kubectl).
    """
    from backend import aws_credentials as ac

    env = os.environ.copy()
    c = (cloud or "aws").strip().lower()
    if c == "azure":
        env.pop("AWS_PROFILE", None)
    elif aws_profile and aws_profile.strip():
        prof = aws_profile.strip()
        env["AWS_PROFILE"] = prof
        cred_path = Path.home() / ".aws" / "credentials"
        extra = ac.load_named_profile_credentials(cred_path, prof)
        env.update(extra)
    return env


def _run(
    argv: list[str],
    timeout: int,
    aws_profile: str | None = None,
    cloud: str | None = None,
) -> CmdResult:
    display = " ".join(argv)
    env = subprocess_env(aws_profile, cloud)
    try:
        p = subprocess.run(
            argv,
            capture_output=True,
            text=True,
            timeout=timeout,
            check=False,
            env=env,
        )
        return CmdResult(
            ok=p.returncode == 0,
            stdout=p.stdout or "",
            stderr=p.stderr or "",
            returncode=p.returncode,
            cmd_display=display,
        )
    except subprocess.TimeoutExpired:
        return CmdResult(
            ok=False,
            stdout="",
            stderr=f"Timed out after {timeout}s",
            returncode=-1,
            cmd_display=display,
        )
    except FileNotFoundError:
        return CmdResult(
            ok=False,
            stdout="",
            stderr="kubectl not found on PATH",
            returncode=-1,
            cmd_display=display,
        )
    except Exception as exc:  # noqa: BLE001 — surface to operator UI
        return CmdResult(
            ok=False,
            stdout="",
            stderr=str(exc),
            returncode=-1,
            cmd_display=display,
        )


def kubectl_current_context(
    timeout: int = 15, aws_profile: str | None = None, cloud: str | None = None
) -> CmdResult:
    return _run(["kubectl", "config", "current-context"], timeout=timeout, aws_profile=aws_profile, cloud=cloud)


def kubectl_cluster_info(
    timeout: int = 20, aws_profile: str | None = None, cloud: str | None = None
) -> CmdResult:
    return _run(
        ["kubectl", "cluster-info", "--request-timeout=15s"],
        timeout=timeout,
        aws_profile=aws_profile,
        cloud=cloud,
    )


def discover_kafka_namespace(
    pod_name: str = KAFKA_POD,
    timeout: int = 30,
    aws_profile: str | None = None,
    cloud: str | None = None,
) -> CmdResult:
    """Find namespace for the given StatefulSet pod name."""
    tmpl = (
        '{{range .items}}{{if eq .metadata.name "'
        + pod_name
        + '"}}{{.metadata.namespace}}{{"\\n"}}{{end}}{{end}}'
    )
    return _run(
        [
            "kubectl",
            "get",
            "pod",
            "-A",
            "-o",
            f"go-template={tmpl}",
        ],
        timeout=timeout,
        aws_profile=aws_profile,
        cloud=cloud,
    )


def kubectl_list_pod_names(
    namespace: str, timeout: int = 45, aws_profile: str | None = None, cloud: str | None = None
) -> CmdResult:
    """List pod names in namespace (one per line) via ``kubectl get pods -o json``."""
    r = _run(
        ["kubectl", "get", "pods", "-n", namespace, "-o", "json", "--request-timeout=30s"],
        timeout=timeout,
        aws_profile=aws_profile,
        cloud=cloud,
    )
    if not r.ok:
        return r
    try:
        data = json.loads(r.stdout or "{}")
        names = [str(item.get("metadata", {}).get("name") or "") for item in data.get("items") or []]
        names = [n for n in names if n]
        names.sort()
    except (json.JSONDecodeError, TypeError, ValueError):
        return CmdResult(
            ok=False,
            stdout="",
            stderr="invalid JSON from kubectl get pods",
            returncode=-1,
            cmd_display=r.cmd_display,
        )
    out = "\n".join(names) + ("\n" if names else "")
    return CmdResult(ok=True, stdout=out, stderr=r.stderr, returncode=0, cmd_display=r.cmd_display)


def kubectl_get_pod(
    namespace: str, timeout: int = 20, aws_profile: str | None = None, cloud: str | None = None
) -> CmdResult:
    return _run(
        [
            "kubectl",
            "get",
            "pod",
            "-n",
            namespace,
            KAFKA_POD,
            "-o",
            "wide",
            "--request-timeout=15s",
        ],
        timeout=timeout,
        aws_profile=aws_profile,
        cloud=cloud,
    )


def sanitize_name(name: str, label: str) -> str:
    n = (name or "").strip()
    if not n:
        raise ValueError(f"{label} is required")
    if not _SAFE_TOPIC_GROUP.match(n):
        raise ValueError(f"{label} may only contain letters, digits, dot, underscore, hyphen")
    return n


def sanitize_aws_profile(name: str | None) -> str | None:
    """Optional AWS named profile for AWS_PROFILE; same charset as sanitize_name."""
    n = (name or "").strip()
    if not n:
        return None
    if not _SAFE_TOPIC_GROUP.match(n):
        raise ValueError("AWS profile name may only contain letters, digits, dot, underscore, hyphen")
    return n


def classify_command_risk(command: str) -> tuple[bool, str]:
    """
    Returns (is_read_only, reason_if_not).
    Anything not clearly read-only is treated as requiring confirmation.
    """
    c = (command or "").strip().lower()
    if not c:
        return False, "empty command"
    for m in _WRITE_MARKERS:
        if m.lower() in c:
            return False, f"blocked pattern: {m!r}"
    # kafka-console-producer, etc.
    if "producer" in c and "console" in c:
        return False, "console producer"
    return True, ""


def classify_sql_risk(sql: str) -> tuple[bool, str]:
    """Returns (is_read_only, reason). Conservative for multi-statement or DDL."""
    s = (sql or "").strip()
    if not s:
        return False, "empty SQL"
    low = s.lower()
    if ";" in s.rstrip().rstrip(";"):
        return False, "multiple statements (;)"
    dangerous = (
        "insert ",
        "alter ",
        "drop ",
        "delete ",
        "truncate ",
        "create ",
        "attach ",
        "detach ",
        "optimize ",
        "rename ",
        "revoke ",
        "grant ",
        "system ",
    )
    for d in dangerous:
        if d in low:
            return False, f"write-like SQL ({d.strip()})"
    if re.match(r"^\s*(with\s+)?select\b", low):
        return True, ""
    if re.match(r"^\s*show\b", low):
        return True, ""
    if re.match(r"^\s*describe\b", low):
        return True, ""
    if re.match(r"^\s*exists\b", low):
        return True, ""
    return False, "only plain SELECT / SHOW / DESCRIBE / EXISTS are auto-approved"


def elasticsearch_curl(
    namespace: str,
    pod_name: str,
    container: str,
    path_and_query: str,
    method: str = "GET",
    body: str | None = None,
    timeout: int = 120,
    aws_profile: str | None = None,
    cloud: str | None = None,
) -> CmdResult:
    """
    Run curl against Elasticsearch HTTP inside the pod (no port-forward).
    ``path_and_query`` must start with ``/`` (e.g. ``/_cluster/health?pretty``).
    """
    pq = (path_and_query or "").strip()
    if not pq.startswith("/"):
        pq = "/" + pq
    url = ELASTICSEARCH_LOCAL.rstrip("/") + pq
    url_q = shlex.quote(url)
    m = (method or "GET").upper()
    inner_max = max(10, timeout - 8)
    if m == "GET":
        inner = f"curl -sS --connect-timeout 15 --max-time {inner_max} -X GET {url_q}"
    elif m == "POST":
        raw_body = body if body is not None else "{}"
        b64 = base64.b64encode(raw_body.encode("utf-8")).decode("ascii")
        b64q = shlex.quote(b64)
        inner = (
            f"body=$(echo {b64q} | base64 -d) && "
            f'curl -sS --connect-timeout 15 --max-time {inner_max} -X POST {url_q} '
            f"-H {shlex.quote('Content-Type: application/json')} -d \"$body\""
        )
    else:
        return CmdResult(
            ok=False,
            stdout="",
            stderr="elasticsearch_curl: only GET or POST supported",
            returncode=-1,
            cmd_display="elasticsearch_curl",
        )
    argv = [
        "kubectl",
        "exec",
        "-n",
        namespace,
        "-c",
        container,
        pod_name,
        "--",
        "bash",
        "-lc",
        inner,
    ]
    return _run(argv, timeout=timeout, aws_profile=aws_profile, cloud=cloud)


def kafka_connect_curl(
    namespace: str,
    pod_name: str,
    container: str,
    path_and_query: str,
    method: str = "GET",
    body: str | None = None,
    timeout: int = 120,
    aws_profile: str | None = None,
    cloud: str | None = None,
) -> CmdResult:
    """
    Run curl against Kafka Connect REST API inside the pod (no port-forward).
    ``path_and_query`` must start with ``/`` (e.g. ``/connectors``).
    """
    pq = (path_and_query or "").strip()
    if not pq.startswith("/"):
        pq = "/" + pq
    url = KAFKA_CONNECT_LOCAL.rstrip("/") + pq
    url_q = shlex.quote(url)
    m = (method or "GET").upper()
    inner_max = max(10, timeout - 8)
    if m == "GET":
        inner = f"curl -sS --connect-timeout 15 --max-time {inner_max} -X GET {url_q}"
    elif m == "POST":
        raw_body = body if body is not None else "{}"
        b64 = base64.b64encode(raw_body.encode("utf-8")).decode("ascii")
        b64q = shlex.quote(b64)
        inner = (
            f"body=$(echo {b64q} | base64 -d) && "
            f'curl -sS --connect-timeout 15 --max-time {inner_max} -X POST {url_q} '
            f"-H {shlex.quote('Content-Type: application/json')} -d \"$body\""
        )
    else:
        return CmdResult(
            ok=False,
            stdout="",
            stderr="kafka_connect_curl: only GET or POST supported",
            returncode=-1,
            cmd_display="kafka_connect_curl",
        )
    argv = [
        "kubectl",
        "exec",
        "-n",
        namespace,
        "-c",
        container,
        pod_name,
        "--",
        "bash",
        "-lc",
        inner,
    ]
    return _run(argv, timeout=timeout, aws_profile=aws_profile, cloud=cloud)


def classify_es_custom_get(path: str) -> tuple[bool, str]:
    """Custom operator path: GET only, conservative allowlist."""
    p = (path or "").strip()
    if not p.startswith("/"):
        return False, "path must start with /"
    if "\n" in p or "\r" in p or ".." in p:
        return False, "path contains illegal sequences"
    if len(p) > 900:
        return False, "path too long"
    if not re.match(r"^/[A-Za-z0-9_./?=&,*+%:@~\[\]-]*$", p):
        return False, "path contains disallowed characters"
    low = p.lower()
    for bad in (
        "_bulk",
        "_delete_by_query",
        "_update_by_query",
        "_mtermvectors",
        "_msearch",
        "_doc",
        "_create",
        "_rollover",
        "_close",
        "_open",
        "_forcemerge",
        "_shrink",
        "_split",
        "_clone",
        "_cache/clear",
        "_unfreeze",
        "_freeze",
        "/delete",
        "update_by_query",
        "delete_by_query",
    ):
        if bad in low:
            return False, f"blocked segment in path ({bad})"
    return True, ""


def clickhouse_client_query(
    namespace: str,
    pod_name: str,
    container: str,
    password: str,
    sql: str,
    timeout: int = 180,
    aws_profile: str | None = None,
    cloud: str | None = None,
    *,
    output_format: str = "PrettyCompact",
) -> CmdResult:
    """Run ``clickhouse-client -q`` inside the ClickHouse pod (password + SQL via base64-safe shell)."""
    pw = shlex.quote(password)
    db = shlex.quote(CLICKHOUSE_DEFAULT_DATABASE)
    fmt = (output_format or "PrettyCompact").strip() or "PrettyCompact"
    if not re.match(r"^[a-zA-Z][a-zA-Z0-9_]*$", fmt):
        fmt = "PrettyCompact"
    fmt_q = shlex.quote(fmt)
    b64 = base64.b64encode((sql or "").encode("utf-8")).decode("ascii")
    b64q = shlex.quote(b64)
    inner = (
        f"decoded=$(echo {b64q} | base64 -d) && "
        "export TERM=dumb; "
        f"clickhouse-client --password {pw} -d {db} --format {fmt_q} -q \"$decoded\""
    )
    argv = [
        "kubectl",
        "exec",
        "-n",
        namespace,
        "-c",
        container,
        pod_name,
        "--",
        "bash",
        "-lc",
        inner,
    ]
    return _run(argv, timeout=timeout, aws_profile=aws_profile, cloud=cloud)


def pod_bash_lc_exec(
    namespace: str,
    pod_name: str,
    container: str,
    inner_bash_lc: str,
    timeout: int = 180,
    aws_profile: str | None = None,
    cloud: str | None = None,
) -> CmdResult:
    """Run ``bash -lc <inner>`` inside an arbitrary pod container (operator-controlled commands only)."""
    argv = [
        "kubectl",
        "exec",
        "-n",
        namespace,
        "-c",
        container,
        pod_name,
        "--",
        "bash",
        "-lc",
        inner_bash_lc,
    ]
    return _run(argv, timeout=timeout, aws_profile=aws_profile, cloud=cloud)


def postgres_psql_query(
    namespace: str,
    pod_name: str,
    container: str,
    password: str,
    sql: str,
    timeout: int = 180,
    aws_profile: str | None = None,
    cloud: str | None = None,
) -> CmdResult:
    """Run psql -c with password from env (SQL via base64, same pattern as ClickHouse).

    ``kubectl exec`` often yields a minimal PATH; Bitnami images ship ``psql`` under
    ``/opt/bitnami/postgresql/bin/``. Override with env ``GB_STS_PSQL`` (full path on the pod).
    """
    pw = shlex.quote(password)
    b64 = base64.b64encode((sql or "").encode("utf-8")).decode("ascii")
    b64q = shlex.quote(b64)
    explicit = os.environ.get("GB_STS_PSQL", "").strip()
    if explicit:
        psql_q = shlex.quote(explicit)
        inner = (
            f"decoded=$(echo {b64q} | base64 -d) && export PGPASSWORD={pw} && "
            'FS=$(printf \'\\t\') && '
            f'{psql_q} -U clarisite -d glassbox -t -A -F "$FS" -v ON_ERROR_STOP=1 -c "$decoded"'
        )
    else:
        inner = (
            f"decoded=$(echo {b64q} | base64 -d) && export PGPASSWORD={pw} && "
            'PSQL=""; '
            "for p in /opt/bitnami/postgresql/bin/psql /opt/bitnami/common/bin/psql "
            "/usr/bin/psql /usr/local/bin/psql; do "
            '  if [ -x "$p" ]; then PSQL="$p"; break; fi; '
            "done; "
            'if [ -z "$PSQL" ] && command -v psql >/dev/null 2>&1; then PSQL=$(command -v psql); fi; '
            'if [ -z "$PSQL" ]; then echo "psql not found (set GB_STS_PSQL to the in-pod path, then restart server.py)" >&2; exit 127; fi; '
            'FS=$(printf \'\\t\') && '
            '"$PSQL" -U clarisite -d glassbox -t -A -F "$FS" -v ON_ERROR_STOP=1 -c "$decoded"'
        )
    return pod_bash_lc_exec(namespace, pod_name, container, inner, timeout=timeout, aws_profile=aws_profile, cloud=cloud)


def clingine_cql_host_for_pod(clingine_pod_name: str) -> str | None:
    """``clingine-<n>`` → ``{GB_STS_CLINGINE_EXTERNAL_PREFIX|clingine-external}-<n>`` (DNS in-cluster)."""
    m = _CLINGINE_POD_INDEX.match((clingine_pod_name or "").strip())
    if not m:
        return None
    prefix = (os.environ.get("GB_STS_CLINGINE_EXTERNAL_PREFIX") or "clingine-external").strip() or "clingine-external"
    if not re.match(r"^[a-zA-Z][a-zA-Z0-9-]{0,62}$", prefix):
        prefix = "clingine-external"
    return f"{prefix}-{int(m.group(1))}"


def clingine_cqlsh_via_cassandra_proxy(
    namespace: str,
    clingine_pod_name: str,
    cql: str,
    *,
    timeout: int = 180,
    aws_profile: str | None = None,
    cloud: str | None = None,
) -> CmdResult:
    """
    Clingine pods have no ``cqlsh``. Run ``cqlsh`` inside a Cassandra chart pod (default
    ``glassbox-cassandra-0``) with ``CQLSH_HOST`` / ``CQLSH_PORT`` set to the Clingine external
    service (``clingine-external-<n>`` : ``8186`` by default).
    """
    host = clingine_cql_host_for_pod(clingine_pod_name)
    if not host:
        return CmdResult(
            ok=False,
            stdout="",
            stderr="invalid clingine pod for CQL (expected clingine-<n>)",
            returncode=2,
            cmd_display="(rejected)",
        )
    port_raw = (os.environ.get("GB_STS_CLINGINE_CQL_PORT") or "8186").strip() or "8186"
    if not port_raw.isdigit() or not (1 <= len(port_raw) <= 5):
        return CmdResult(
            ok=False,
            stdout="",
            stderr="invalid GB_STS_CLINGINE_CQL_PORT",
            returncode=2,
            cmd_display="(rejected)",
        )
    proxy_raw = (os.environ.get("GB_STS_CLINGINE_CQLSH_PROXY_POD") or "glassbox-cassandra-0").strip() or "glassbox-cassandra-0"
    proxy_pod = sanitize_pod_for_workload("cassandra", proxy_raw)
    if not proxy_pod:
        return CmdResult(
            ok=False,
            stdout="",
            stderr="invalid GB_STS_CLINGINE_CQLSH_PROXY_POD (use glassbox-cassandra-<n>)",
            returncode=2,
            cmd_display="(rejected)",
        )
    b64 = base64.b64encode((cql or "").encode("utf-8")).decode("ascii")
    b64q = shlex.quote(b64)
    host_q = shlex.quote(host)
    port_q = shlex.quote(port_raw)
    explicit = os.environ.get("GB_STS_CQLSH", "").strip()
    if explicit:
        qbin = shlex.quote(explicit)
        inner = (
            f"decoded=$(echo {b64q} | base64 -d) && "
            f"export CQLSH_HOST={host_q} CQLSH_PORT={port_q} && "
            f"{qbin} -e \"$decoded\""
        )
    else:
        inner = (
            f"decoded=$(echo {b64q} | base64 -d) && "
            f"export CQLSH_HOST={host_q} CQLSH_PORT={port_q} && "
            "CQLSH=\"\"; "
            "for p in /opt/bitnami/cassandra/bin/cqlsh /opt/apache-cassandra/bin/cqlsh "
            "/usr/bin/cqlsh /usr/local/bin/cqlsh; do "
            "  if [ -x \"$p\" ]; then CQLSH=\"$p\"; break; fi; "
            "done; "
            "if [ -z \"$CQLSH\" ] && command -v cqlsh >/dev/null 2>&1; then CQLSH=$(command -v cqlsh); fi; "
            "if [ -z \"$CQLSH\" ]; then echo \"cqlsh not found (set GB_STS_CQLSH to the in-pod path on the Cassandra pod)\" >&2; exit 127; fi; "
            '"$CQLSH" -e "$decoded"'
        )
    return pod_bash_lc_exec(
        namespace,
        proxy_pod,
        CASSANDRA_CONTAINER,
        inner,
        timeout=timeout,
        aws_profile=aws_profile,
        cloud=cloud,
    )


def cassandra_nodetool_readonly(
    namespace: str,
    pod_name: str,
    container: str,
    op: str,
    *,
    timeout: int = 220,
    aws_profile: str | None = None,
    cloud: str | None = None,
    nodetool_bin: str | None = None,
    rpc_host: str | None = None,
    jmx_port: str | None = None,
    tablestats_keyspace: str | None = None,
    tablestats_table: str | None = None,
    retries: int = 1,
    retry_sleep_secs: int = 1,
) -> CmdResult:
    """
    Run a fixed read-only ``nodetool`` subcommand inside the Cassandra pod (no JMX mutations).

    Override binary with env ``GB_STS_NODETOOL`` (absolute path on the pod), or pass ``nodetool_bin``
    for Clingine (e.g. ``/root/clingine/bin/nodetool.sh``).

    ``jmx_port`` adds ``-p <port>`` (JMX) when set — needed for some Clingine images where JMX is not
    on nodetool's default. ``tablestats_keyspace`` + ``tablestats_table`` narrow ``tablestats`` to one CF.

    ``retries`` (>=1) wraps the nodetool invocation in a bash retry loop with ``retry_sleep_secs``
    between attempts; the loop exits on first success and otherwise returns the last attempt's output
    and exit status. Use this for flaky JMX (e.g. Clingine's embedded Cassandra).
    """
    allowed: dict[str, tuple[str, int]] = {
        "status": ("status", 800),
        "tablestats": ("tablestats", 15000),
        "tpstats": ("tpstats", 5000),
        "gcstats": ("gcstats", 400),
        "compactionstats": ("compactionstats", 1200),
        "proxyhistograms": ("proxyhistograms", 5000),
    }
    key = (op or "").strip().lower()
    if key not in allowed:
        return CmdResult(
            ok=False,
            stdout="",
            stderr=f"unknown nodetool op (allowed: {', '.join(sorted(allowed))})",
            returncode=2,
            cmd_display="(rejected)",
        )
    sub, cap = allowed[key]
    if key == "tablestats" and tablestats_keyspace and tablestats_table:
        cap = min(cap, 8000)
    if nodetool_bin and str(nodetool_bin).strip():
        nt_path = str(nodetool_bin).strip()
    else:
        nt_default = "/opt/bitnami/cassandra/bin/nodetool"
        nt_path = os.environ.get("GB_STS_NODETOOL", nt_default).strip() or nt_default
    ntq = shlex.quote(nt_path)
    rpc = (rpc_host or "localhost").strip() or "localhost"
    if not re.match(r"^[a-zA-Z0-9_.-]+$", rpc):
        return CmdResult(
            ok=False,
            stdout="",
            stderr="invalid rpc_host for nodetool",
            returncode=2,
            cmd_display="(rejected)",
        )
    rpc_q = shlex.quote(rpc)
    jmx_suffix = ""
    if jmx_port is not None and str(jmx_port).strip():
        jp = str(jmx_port).strip()
        if jp.isdigit() and 1 <= len(jp) <= 5:
            jmx_suffix = f" -p {shlex.quote(jp)}"
    sub_cmd = sub
    if key == "tablestats" and tablestats_keyspace and tablestats_table:
        try:
            ks = pgcass.sanitize_cassandra_keyspace(tablestats_keyspace)
            tb = pgcass.sanitize_cassandra_table(tablestats_table)
        except ValueError as e:
            return CmdResult(
                ok=False,
                stdout="",
                stderr=str(e),
                returncode=2,
                cmd_display="(rejected)",
            )
        sub_cmd = f"tablestats {shlex.quote(ks)}.{shlex.quote(tb)}"
    inner = f"if [ ! -f {ntq} ] && [ ! -x {ntq} ]; then echo 'nodetool not found (set GB_STS_NODETOOL or pass nodetool_bin)' >&2; exit 127; fi; "
    nt_invocation = f"{ntq} -h {rpc_q}{jmx_suffix} {sub_cmd}"
    n = max(1, int(retries or 1))
    sleep_s = max(0, int(retry_sleep_secs or 0))
    if n <= 1:
        inner += f"{nt_invocation} 2>&1 | head -n {cap}"
    else:
        # Retry wrapper: capture combined stdout+stderr, break on first success, otherwise
        # emit the last attempt's output and exit with its return code.
        iters = " ".join(str(i) for i in range(1, n + 1))
        inner += (
            f"_NT_OUT=''; _NT_EC=1; "
            f"for _NT_I in {iters}; do "
            f"_NT_OUT=\"$({nt_invocation} 2>&1)\"; _NT_EC=$?; "
            f"[ $_NT_EC -eq 0 ] && break; "
            f"sleep {sleep_s}; "
            f"done; "
            f"printf '%s\\n' \"$_NT_OUT\" | head -n {cap}; "
            f"exit $_NT_EC"
        )
    return pod_bash_lc_exec(namespace, pod_name, container, inner, timeout=timeout, aws_profile=aws_profile, cloud=cloud)


def _clingine_nodetool_jmx_stderr_flaky(stderr: str | None, stdout: str | None = None) -> bool:
    """True when the nodetool result smells like a JMX hiccup we should retry with a different port.

    Inspects both stderr and stdout because the retry wrapper folds ``2>&1`` into stdout.
    """
    s = ((stderr or "") + "\n" + (stdout or "")).lower()
    if not s.strip():
        return False
    return (
        "nosuchobjectexception" in s
        or "no such object" in s
        or "failed to connect" in s
        or "broken pipe" in s
        or "connection refused" in s
    )


CLINGINE_NODETOOL_DEFAULT_JMX_PORT = "30081"


def clingine_nodetool_readonly(
    namespace: str,
    pod_name: str,
    container: str,
    op: str,
    *,
    timeout: int = 240,
    aws_profile: str | None = None,
    cloud: str | None = None,
    nodetool_bin: str | None = None,
    tablestats_keyspace: str | None = None,
    tablestats_table: str | None = None,
    retries: int = 5,
    retry_sleep_secs: int = 1,
) -> CmdResult:
    """
    Nodetool against Clingine's embedded Cassandra (best-effort).

    Clingine's JMX is on port ``30081`` (not 7199) and the connection is intermittently flaky
    (``Failed to connect to 'localhost:30081' - NoSuchObjectException``). To match the operator's
    workaround we wrap each invocation in a 5-attempt loop with a 1-second sleep between attempts:

        for i in 1 2 3 4 5; do nodetool.sh -h localhost -p 30081 <op> && break || sleep 1; done

    JMX port selection (``GB_STS_CLINGINE_JMX_PORT``):
      - **unset / empty** → use ``-p 30081`` (the listening port observed on the pod).
      - **digits** (e.g. ``9081``) → pin to that port.
      - **off / none / - / 0** → omit ``-p`` entirely (let nodetool pick its default).

    On flaky JMX stderr we retry with ``-p`` omitted as a last-ditch fallback (only when the env
    is not pinning a specific port).
    """
    raw = (os.environ.get("GB_STS_CLINGINE_JMX_PORT") or "").strip()

    def one(jmx: str | None) -> CmdResult:
        return cassandra_nodetool_readonly(
            namespace,
            pod_name,
            container,
            op,
            timeout=timeout,
            aws_profile=aws_profile,
            cloud=cloud,
            nodetool_bin=nodetool_bin,
            jmx_port=jmx,
            tablestats_keyspace=tablestats_keyspace,
            tablestats_table=tablestats_table,
            retries=retries,
            retry_sleep_secs=retry_sleep_secs,
        )

    if raw.isdigit() and 1 <= len(raw) <= 5:
        return one(raw)
    if raw.lower() in ("off", "none", "-", "0"):
        r0 = one(None)
        if r0.ok or not _clingine_nodetool_jmx_stderr_flaky(r0.stderr, r0.stdout):
            return r0
        return one(CLINGINE_NODETOOL_DEFAULT_JMX_PORT)

    r1 = one(CLINGINE_NODETOOL_DEFAULT_JMX_PORT)
    if r1.ok:
        return r1
    if _clingine_nodetool_jmx_stderr_flaky(r1.stderr, r1.stdout):
        r2 = one(None)
        if r2.ok:
            return r2
    return r1


def cassandra_cqlsh_query(
    namespace: str,
    pod_name: str,
    container: str,
    cql: str,
    timeout: int = 180,
    aws_profile: str | None = None,
    cloud: str | None = None,
) -> CmdResult:
    """
    Run ``cqlsh`` with CQL via base64 inside the pod.

    ``kubectl exec`` often uses a minimal PATH; Bitnami images ship cqlsh under
    ``/opt/bitnami/cassandra/bin/``. Override with env ``GB_STS_CQLSH`` (full path on the pod).

    Host resolution (first non-empty): ``GB_STS_CASSANDRA_CQL_HOST``, chart env ``CASSANDRA_HOST``,
    ``POD_IP``, ``CASSANDRA_BROADCAST_ADDRESS``, then ``127.0.0.1``. Port: ``GB_STS_CASSANDRA_CQL_PORT``,
    ``CASSANDRA_CQL_PORT_NUMBER``, else ``9042``.
    """
    b64 = base64.b64encode((cql or "").encode("utf-8")).decode("ascii")
    b64q = shlex.quote(b64)
    host_snip = (
        'CQL_HOST="${GB_STS_CASSANDRA_CQL_HOST:-}"; '
        '[ -z "$CQL_HOST" ] && CQL_HOST="${CASSANDRA_HOST:-}"; '
        '[ -z "$CQL_HOST" ] && CQL_HOST="${POD_IP:-}"; '
        '[ -z "$CQL_HOST" ] && CQL_HOST="${CASSANDRA_BROADCAST_ADDRESS:-}"; '
        '[ -z "$CQL_HOST" ] && CQL_HOST="127.0.0.1"; '
        'CQL_PORT="${GB_STS_CASSANDRA_CQL_PORT:-${CASSANDRA_CQL_PORT_NUMBER:-9042}}"; '
    )
    explicit = os.environ.get("GB_STS_CQLSH", "").strip()
    if explicit:
        qbin = shlex.quote(explicit)
        inner = f"decoded=$(echo {b64q} | base64 -d) && {host_snip}{qbin} \"$CQL_HOST\" \"$CQL_PORT\" -e \"$decoded\""
    else:
        inner = (
            f"decoded=$(echo {b64q} | base64 -d) && "
            "CQLSH=\"\"; "
            "for p in /opt/bitnami/cassandra/bin/cqlsh /opt/apache-cassandra/bin/cqlsh "
            "/usr/bin/cqlsh /usr/local/bin/cqlsh; do "
            "  if [ -x \"$p\" ]; then CQLSH=\"$p\"; break; fi; "
            "done; "
            "if [ -z \"$CQLSH\" ] && command -v cqlsh >/dev/null 2>&1; then CQLSH=$(command -v cqlsh); fi; "
            "if [ -z \"$CQLSH\" ]; then echo \"cqlsh not found (set GB_STS_CQLSH to the in-pod path, then restart server.py)\" >&2; exit 127; fi; "
            f"{host_snip}"
            '"$CQLSH" "$CQL_HOST" "$CQL_PORT" -e "$decoded"'
        )
    return pod_bash_lc_exec(namespace, pod_name, container, inner, timeout=timeout, aws_profile=aws_profile, cloud=cloud)


def parse_clickhouse_tsv_with_header(stdout: str) -> list[list[str]]:
    """Parse ``TabSeparatedWithNames`` (or any uniform tab-separated rows) into a table of strings."""
    rows: list[list[str]] = []
    for line in (stdout or "").splitlines():
        if not line.strip():
            continue
        rows.append(line.split("\t"))
    if not rows:
        return []
    n = len(rows[0])
    if n == 0:
        return []
    return rows


def kafka_bash_exec(
    namespace: str,
    remote_bash_body: str,
    timeout: int = 180,
    aws_profile: str | None = None,
    cloud: str | None = None,
) -> CmdResult:
    """
    Run a bash -lc script body inside the **kafka** container (not jmx sidecar).
    Uses a random high JMX_PORT per invocation to avoid bind collisions on 3333 when JVM tools start agents.
    """
    jmx = random.randint(40000, 49151)
    inner = f"export JMX_PORT={jmx}; {remote_bash_body}"
    argv = [
        "kubectl",
        "exec",
        "-n",
        namespace,
        "-c",
        KAFKA_CONTAINER,
        KAFKA_POD,
        "--",
        "bash",
        "-lc",
        inner,
    ]
    return _run(argv, timeout=timeout, aws_profile=aws_profile, cloud=cloud)


def parse_topics_partition_counts(text: str) -> list[list[str]]:
    """Parse `uniq -c` style lines: '    257 topic_name'."""
    rows: list[list[str]] = []
    for line in text.splitlines():
        line = line.strip()
        if not line:
            continue
        parts = line.split(None, 1)
        if len(parts) == 2 and parts[0].isdigit():
            rows.append([parts[0], parts[1]])
    return rows


def parse_leader_balance(text: str) -> list[list[str]]:
    """Count lines containing 'Leader: N' per broker id from topic --describe."""
    counts: dict[str, int] = {}
    for line in text.splitlines():
        if "Leader:" not in line:
            continue
        m = re.search(r"Leader:\s*(\d+)", line)
        if m:
            bid = m.group(1)
            counts[bid] = counts.get(bid, 0) + 1
    return [[b, str(counts[b])] for b in sorted(counts, key=lambda x: int(x))]


def parse_consumer_group_describe(text: str) -> list[list[str]]:
    """Whitespace-split kafka-consumer-groups --describe table (header + rows)."""
    lines = [ln for ln in text.splitlines() if ln.strip()]
    if not lines:
        return []
    rows = []
    for ln in lines:
        rows.append(ln.split())
    return rows


def tail_lines(text: str, max_lines: int = 500) -> str:
    ls = text.splitlines()
    if len(ls) <= max_lines:
        return text
    return "\n".join(ls[-max_lines:]) + f"\n\n… truncated, showing last {max_lines} lines …"


def discover_namespace_for_pod(
    pod_name: str,
    timeout: int = 30,
    aws_profile: str | None = None,
    cloud: str | None = None,
) -> str | None:
    r = discover_kafka_namespace(pod_name=pod_name, timeout=timeout, aws_profile=aws_profile, cloud=cloud)
    if not r.ok:
        return None
    for line in r.stdout.splitlines():
        ns = line.strip()
        if ns:
            return ns
    return None


def filter_kubectl_noise(stderr: str) -> str:
    """Drop benign multi-container defaulting hints."""
    if not stderr:
        return ""
    lines = [ln for ln in stderr.splitlines() if "Defaulted container" not in ln]
    return "\n".join(lines).strip()
