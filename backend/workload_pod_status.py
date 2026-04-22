"""Batched kubectl read-only status for workload pods (one page = few subprocess calls)."""

from __future__ import annotations

import json
import re
import shlex
from datetime import datetime, timezone
from typing import Any

from backend import k8s_exec as kx
from backend import stack_components as stack

_ALLOWED_WORKLOADS = frozenset(
    {"kafka", "kafkaconnect", "clickhouse", "elasticsearch", "opensearch", "postgresql", "cassandra"}
)

_WORKLOAD_DISK_PATH: dict[str, str | None] = {
    "kafka": "/bitnami/kafka",
    "kafkaconnect": None,
    "elasticsearch": "/bitnami/elasticsearch/data",
    "opensearch": "/bitnami/opensearch/data",
    "clickhouse": "/var/lib/clickhouse",
    "postgresql": "/bitnami/postgresql",
    "cassandra": "/bitnami/cassandra",
}

_WORKLOAD_CONTAINER: dict[str, str | None] = {
    "kafka": kx.KAFKA_CONTAINER,
    "kafkaconnect": None,
    "elasticsearch": kx.ELASTICSEARCH_CONTAINER,
    "opensearch": kx.OPENSEARCH_CONTAINER,
    "clickhouse": kx.CLICKHOUSE_CONTAINER,
    "postgresql": kx.POSTGRES_CONTAINER,
    "cassandra": kx.CASSANDRA_CONTAINER,
}


def _prefixes_for_workload(workload: str) -> list[str]:
    w = (workload or "").strip().lower()
    for spec in stack.STACK_COMPONENTS:
        if spec["id"] == w:
            return list(spec["pod_prefixes"])
    raise ValueError(f"unknown workload: {workload!r}")


def _pod_matches_prefixes(name: str, prefixes: list[str]) -> bool:
    return any(name.startswith(p) for p in prefixes)


# Kafka pod status: only broker StatefulSet pods (glassbox-kafka-<n>), not Deployments
# such as glassbox-kafka-exporter-* that share the same name prefix.
_KAFKA_STS_BROKER_POD = re.compile(r"^glassbox-kafka-\d+$")


def _kafka_broker_status_pod(name: str) -> bool:
    return bool(_KAFKA_STS_BROKER_POD.match(name))


def _parse_rfc3339(ts: str | None) -> datetime | None:
    if not ts or not isinstance(ts, str):
        return None
    t = ts.strip()
    if not t:
        return None
    try:
        if t.endswith("Z"):
            t = t[:-1] + "+00:00"
        # Strip sub-second noise for fromisoformat in older Python
        if re.search(r"\.\d+", t):
            t = re.sub(r"\.\d+", "", t, count=1)
        dt = datetime.fromisoformat(t)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except (ValueError, TypeError, OSError):
        return None


def _age_short(ts: str | None) -> str:
    dt = _parse_rfc3339(ts)
    if not dt:
        return "N/A"
    now = datetime.now(timezone.utc)
    secs = int((now - dt).total_seconds())
    if secs < 0:
        return "N/A"
    d, rem = divmod(secs, 86400)
    h, rem = divmod(rem, 3600)
    m, s = divmod(rem, 60)
    if d:
        return f"{d}d{h}h" if h else f"{d}d"
    if h:
        return f"{h}h{m}m" if m else f"{h}h"
    if m:
        return f"{m}m"
    return f"{s}s"


def _pod_ready_and_restarts(item: dict[str, Any]) -> tuple[str, int]:
    statuses = (item.get("status") or {}).get("containerStatuses") or []
    if not statuses:
        return "0/0", 0
    total = len(statuses)
    ready = sum(1 for s in statuses if s.get("ready"))
    restarts = sum(int(s.get("restartCount") or 0) for s in statuses)
    return f"{ready}/{total}", restarts


def _parse_top_pod(stdout: str, wanted: set[str]) -> dict[str, dict[str, str]]:
    out: dict[str, dict[str, str]] = {}
    for line in (stdout or "").splitlines():
        line = line.strip()
        if not line or line.upper().startswith("NAME "):
            continue
        parts = line.split()
        if len(parts) < 3:
            continue
        name = parts[0]
        if name not in wanted:
            continue
        out[name] = {"cpu": parts[1], "memory": parts[2]}
    return out


def _parse_top_node(stdout: str) -> dict[str, dict[str, str]]:
    """First column = node name; remaining columns vary by kubectl version."""
    rows: dict[str, dict[str, str]] = {}
    lines = [ln.strip() for ln in (stdout or "").splitlines() if ln.strip()]
    if not lines:
        return rows
    start = 1 if lines[0].upper().startswith("NAME") else 0
    for line in lines[start:]:
        parts = line.split()
        if len(parts) < 2:
            continue
        name = parts[0]
        info: dict[str, str] = {"raw": " ".join(parts[1:])}
        # Typical: NAME CPU(cores) CPU% MEMORY(bytes) MEMORY%
        if len(parts) >= 5:
            info["cpu"] = parts[1]
            info["cpu_pct"] = parts[2]
            info["memory"] = parts[3]
            info["mem_pct"] = parts[4]
        elif len(parts) >= 3:
            info["cpu"] = parts[1]
            info["memory"] = parts[2]
        rows[name] = info
    return rows


def _collect_events_for_pods(
    events_json: dict[str, Any], pod_names: set[str], per_pod: int = 5, max_total: int = 40
) -> dict[str, list[dict[str, str]]]:
    by_pod: dict[str, list[dict[str, str]]] = {n: [] for n in pod_names}
    items = events_json.get("items") or []
    scored: list[tuple[str, float, dict[str, str]]] = []
    for ev in items:
        ref = ev.get("involvedObject") or {}
        if ref.get("kind") != "Pod":
            continue
        pname = str(ref.get("name") or "")
        if pname not in pod_names:
            continue
        ts = str(ev.get("lastTimestamp") or ev.get("firstTimestamp") or "")
        dt = _parse_rfc3339(ts)
        score = dt.timestamp() if dt else 0.0
        scored.append(
            (
                pname,
                score,
                {
                    "type": str(ev.get("type") or "Normal"),
                    "reason": str(ev.get("reason") or "N/A"),
                    "message": (str(ev.get("message") or "") or "N/A")[:240],
                    "age": _age_short(ts) if ts else "N/A",
                },
            )
        )
    scored.sort(key=lambda x: x[1], reverse=True)
    total = 0
    for pname, _s, row in scored:
        if total >= max_total:
            break
        if len(by_pod[pname]) >= per_pod:
            continue
        by_pod[pname].append(row)
        total += 1
    return by_pod


def _parse_df_batch(stdout: str, pod_names: set[str]) -> dict[str, dict[str, str]]:
    """
    Parse helper output from one batched subprocess that loops kubectl exec + ``df -h`` over pods.
    Expected marker lines: ``__GB_POD__ <name>`` and data lines: ``SIZE|USED|FREE``.
    """
    out: dict[str, dict[str, str]] = {p: {"size": "N/A", "used": "N/A", "free": "N/A"} for p in pod_names}
    current = ""
    for raw in (stdout or "").splitlines():
        ln = raw.strip()
        if not ln:
            continue
        if ln.startswith("__GB_POD__ "):
            current = ln.split("__GB_POD__ ", 1)[1].strip()
            if current not in out:
                out[current] = {"size": "N/A", "used": "N/A", "free": "N/A"}
            continue
        if not current:
            continue
        if "|" not in ln:
            continue
        parts = [p.strip() for p in ln.split("|")]
        if len(parts) < 3:
            continue
        out[current] = {"size": parts[0] or "N/A", "used": parts[1] or "N/A", "free": parts[2] or "N/A"}
        current = ""
    return out


def fetch_workload_pod_status(
    namespace: str,
    workload: str,
    *,
    aws_profile: str | None = None,
    cloud: str | None = None,
) -> dict[str, Any]:
    """
    Read-only: ``kubectl get pods -o json`` (filtered), ``kubectl top pod``, ``kubectl top node``,
    ``kubectl get events -o json`` (filtered in-process). Four subprocess calls total.
    """
    w = (workload or "").strip().lower()
    if w not in _ALLOWED_WORKLOADS:
        return {"ok": False, "error": f"unsupported workload: {workload!r}"}
    try:
        prefixes = _prefixes_for_workload(w)
    except ValueError as e:
        return {"ok": False, "error": str(e)}

    errors: dict[str, str] = {}

    r_pods = kx._run(
        ["kubectl", "get", "pods", "-n", namespace, "-o", "json", "--request-timeout=45s"],
        timeout=55,
        aws_profile=aws_profile,
        cloud=cloud,
    )
    if not r_pods.ok:
        return {
            "ok": False,
            "error": kx.filter_kubectl_noise(r_pods.stderr) or "kubectl get pods failed",
            "workload": w,
            "namespace": namespace,
        }

    try:
        pod_doc = json.loads(r_pods.stdout or "{}")
    except json.JSONDecodeError:
        return {"ok": False, "error": "invalid JSON from kubectl get pods", "workload": w, "namespace": namespace}

    items = pod_doc.get("items") or []
    matched: list[dict[str, Any]] = []
    for item in items:
        meta = item.get("metadata") or {}
        name = str(meta.get("name") or "")
        if not name or not _pod_matches_prefixes(name, prefixes):
            continue
        if w == "kafka" and not _kafka_broker_status_pod(name):
            continue
        matched.append(item)
    matched.sort(key=lambda it: str((it.get("metadata") or {}).get("name") or ""))

    pod_names = {str((it.get("metadata") or {}).get("name") or "") for it in matched}
    pod_names.discard("")

    top_pod_map: dict[str, dict[str, str]] = {}
    r_top = kx._run(
        ["kubectl", "top", "pod", "-n", namespace, "--no-headers", "--request-timeout=20s"],
        timeout=30,
        aws_profile=aws_profile,
        cloud=cloud,
    )
    if r_top.ok:
        top_pod_map = _parse_top_pod(r_top.stdout or "", pod_names)
    else:
        errors["top_pod"] = kx.filter_kubectl_noise(r_top.stderr) or "kubectl top pods failed"

    top_node_map = {}
    r_node = kx._run(
        ["kubectl", "top", "node", "--no-headers", "--request-timeout=20s"],
        timeout=30,
        aws_profile=aws_profile,
        cloud=cloud,
    )
    if r_node.ok:
        top_node_map = _parse_top_node(r_node.stdout or "")
    else:
        errors["top_node"] = kx.filter_kubectl_noise(r_node.stderr) or "kubectl top nodes failed"

    events_by_pod: dict[str, list[dict[str, str]]] = {n: [] for n in pod_names}
    r_ev = kx._run(
        ["kubectl", "get", "events", "-n", namespace, "-o", "json", "--request-timeout=30s"],
        timeout=40,
        aws_profile=aws_profile,
        cloud=cloud,
    )
    if r_ev.ok:
        try:
            ev_doc = json.loads(r_ev.stdout or "{}")
            events_by_pod = _collect_events_for_pods(ev_doc, pod_names)
        except json.JSONDecodeError:
            errors["events"] = "invalid JSON from kubectl get events"
    else:
        errors["events"] = kx.filter_kubectl_noise(r_ev.stderr) or "kubectl get events failed"

    disk_by_pod: dict[str, dict[str, str]] = {n: {"size": "N/A", "used": "N/A", "free": "N/A"} for n in pod_names}
    disk_path = _WORKLOAD_DISK_PATH.get(w)
    disk_container = _WORKLOAD_CONTAINER.get(w)
    if disk_path and pod_names:
        pods_list = sorted(pod_names)
        pod_tokens = " ".join(shlex.quote(p) for p in pods_list)
        nsq = shlex.quote(namespace)
        pathq = shlex.quote(disk_path)
        cq = shlex.quote(disk_container) if disk_container else ""
        c_arg = f"-c {cq} " if cq else ""
        script = (
            "for p in "
            + pod_tokens
            + "; do "
            + 'printf "__GB_POD__ %s\\n" "$p"; '
            + f'kubectl exec -n {nsq} {c_arg}"$p" -- df -h {pathq} 2>/dev/null | awk \'NR==2{{print $2\"|\"$3\"|\"$4}}\' || true; '
            + "done"
        )
        r_df = kx._run(["bash", "-lc", script], timeout=80, aws_profile=aws_profile, cloud=cloud)
        if r_df.ok:
            disk_by_pod = _parse_df_batch(r_df.stdout or "", pod_names)
            if pod_names and not any(v.get("size") != "N/A" for v in disk_by_pod.values()):
                errors["storage"] = "df probe returned no rows (mount path missing or container mismatch)"
        else:
            errors["storage"] = kx.filter_kubectl_noise(r_df.stderr) or "kubectl exec df scan failed"

    pods_out: list[dict[str, Any]] = []
    for item in matched:
        meta = item.get("metadata") or {}
        status = item.get("status") or {}
        spec = item.get("spec") or {}
        name = str(meta.get("name") or "N/A")
        phase = str(status.get("phase") or "N/A")
        ready, restarts = _pod_ready_and_restarts(item)
        created = meta.get("creationTimestamp")
        age = _age_short(str(created) if created else None)
        node = str(spec.get("nodeName") or "") or "N/A"
        tp = top_pod_map.get(name, {})
        cpu = tp.get("cpu") or "N/A"
        mem = tp.get("memory") or "N/A"
        nn = top_node_map.get(node, {}) if node != "N/A" else {}
        if isinstance(nn, dict) and nn:
            node_cpu = nn.get("cpu_pct") or nn.get("cpu") or "N/A"
            node_mem = nn.get("mem_pct") or nn.get("memory") or "N/A"
        else:
            node_cpu = "N/A"
            node_mem = "N/A"

        pods_out.append(
            {
                "name": name,
                "phase": phase,
                "ready": ready,
                "restarts": restarts,
                "age": age,
                "node": node,
                "cpu": cpu,
                "memory": mem,
                "node_cpu": node_cpu,
                "node_mem": node_mem,
                "storage_size": disk_by_pod.get(name, {}).get("size", "N/A"),
                "storage_used": disk_by_pod.get(name, {}).get("used", "N/A"),
                "storage_free": disk_by_pod.get(name, {}).get("free", "N/A"),
                "events": events_by_pod.get(name, []),
            }
        )

    return {
        "ok": True,
        "workload": w,
        "namespace": namespace,
        "pods": pods_out,
        "errors": errors,
        "fetched_at": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC"),
    }
