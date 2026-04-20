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

KAFKA_POD = "glassbox-kafka-0"
KAFKA_CONTAINER = "kafka"
KAFKA_BIN = "/opt/bitnami/kafka/bin"
BOOTSTRAP = "127.0.0.1:9092"

CLICKHOUSE_POD_PREFIX = "glassbox-clickhouse-"
CLICKHOUSE_CONTAINER = "glassbox-clickhouse"

ELASTICSEARCH_POD_PREFIX = "glassbox-elasticsearch-master-"
ELASTICSEARCH_CONTAINER = os.environ.get("GB_STS_ES_CONTAINER", "elasticsearch").strip() or "elasticsearch"
ELASTICSEARCH_LOCAL = "http://127.0.0.1:9200"

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
) -> CmdResult:
    """Run ``clickhouse-client -q`` inside the ClickHouse pod (password + SQL via base64-safe shell)."""
    pw = shlex.quote(password)
    b64 = base64.b64encode((sql or "").encode("utf-8")).decode("ascii")
    b64q = shlex.quote(b64)
    inner = f"decoded=$(echo {b64q} | base64 -d) && clickhouse-client --password {pw} -q \"$decoded\""
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
