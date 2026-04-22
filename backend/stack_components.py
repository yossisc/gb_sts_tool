"""
Known Glassbox stack components: pod name prefixes used to count replicas in the session namespace.
"""

from __future__ import annotations

import re
from typing import Any, TypedDict

from backend import k8s_exec as kx


class ComponentSpec(TypedDict, total=False):
    id: str
    label: str
    pod_prefixes: list[str]
    notes: str
    # Optional: precise regex applied AFTER prefix match to count only true workload pods.
    # Used e.g. for kafka where ``glassbox-kafka-`` also matches the exporter Deployment.
    pod_pattern: str


# Reaching glassbox-kafka-0 (or any replica) is enough for cluster-wide Kafka tools; same idea for ES/OS.
STACK_COMPONENTS: list[ComponentSpec] = [
    {
        "id": "kafka",
        "label": "Kafka",
        "pod_prefixes": ["glassbox-kafka-"],
        "pod_pattern": r"^glassbox-kafka-\d+$",
        "notes": "StatefulSet prefix; use pod 0 for cluster-wide kafka tools.",
    },
    {
        "id": "elasticsearch",
        "label": "Elasticsearch",
        "pod_prefixes": ["glassbox-elasticsearch-master-"],
        "notes": "Master pods; multi-node cluster.",
    },
    {
        "id": "opensearch",
        "label": "OpenSearch",
        "pod_prefixes": ["glassbox-opensearch-master-"],
        "notes": "Master pods; multi-node cluster.",
    },
    {
        "id": "kafkaconnect",
        "label": "Kafka Connect",
        "pod_prefixes": ["glassbox-kafkaconnect-"],
        "notes": "StatefulSet prefix; REST API on localhost:8083 inside pod.",
    },
    {
        "id": "clingine",
        "label": "Clingine",
        "pod_prefixes": ["clingine-"],
        "notes": "Scale-out; pick pod index in UI when supported.",
    },
    {
        "id": "clickhouse",
        "label": "ClickHouse",
        "pod_prefixes": ["glassbox-clickhouse-"],
        "notes": "Use glassbox-clickhouse-0..n-1; password from data/sensitive/.ch_password",
    },
    {
        "id": "postgresql",
        "label": "PostgreSQL",
        "pod_prefixes": ["glassbox-postgresql-", "glassbox-postgresql-ha-postgresql-"],
        "notes": "Non-HA vs HA chart pod names.",
    },
    {
        "id": "cassandra",
        "label": "Cassandra",
        "pod_prefixes": ["glassbox-cassandra-"],
        "notes": "Scale-out; pick pod index when supported.",
    },
    {
        "id": "prometheus",
        "label": "Prometheus",
        "pod_prefixes": ["glassbox-prometheus-server-"],
        "notes": "Server pod prefix.",
    },
]


def probe_stack_in_namespace(
    namespace: str,
    *,
    aws_profile: str | None = None,
    cloud: str | None = None,
) -> dict[str, Any]:
    """
    List pod names in namespace and count matches per component.
    Returns ``{ "components": { id: { "label", "replicas", "pods": [...] } }, "spec": STACK_COMPONENTS }``.
    """
    r = kx.kubectl_list_pod_names(namespace, aws_profile=aws_profile, cloud=cloud)
    if not r.ok:
        return {
            "ok": False,
            "error": r.stderr or r.stdout or "kubectl get pods failed",
            "components": {},
        }
    names = [ln.strip() for ln in (r.stdout or "").splitlines() if ln.strip()]
    components: dict[str, Any] = {}
    for spec in STACK_COMPONENTS:
        pattern = spec.get("pod_pattern")
        rx = re.compile(pattern) if pattern else None
        matched: list[str] = []
        for name in names:
            if not any(name.startswith(prefix) for prefix in spec["pod_prefixes"]):
                continue
            if rx is not None and not rx.match(name):
                continue
            matched.append(name)
        matched.sort()
        components[spec["id"]] = {
            "label": spec["label"],
            "replicas": len(matched),
            "pods": matched,
            "notes": spec["notes"],
        }
    return {"ok": True, "namespace": namespace, "components": components, "spec": STACK_COMPONENTS}
