"""List EKS / AKS clusters and merge kubeconfig for kubectl."""

from __future__ import annotations

import json
import re
from typing import Any

from backend import k8s_exec as kx
from backend.regions import AWS_TO_AZURE_LOCATION

_EKS_CLUSTER = re.compile(r"^[a-zA-Z0-9][a-zA-Z0-9_-]{0,99}$")
_AZ_RG = re.compile(r"^[A-Za-z0-9_.()\-]{1,90}$")
_AZ_NAME = re.compile(r"^[a-zA-Z0-9][a-zA-Z0-9_-]{0,99}$")


def sanitize_eks_cluster_name(name: str) -> str:
    n = (name or "").strip()
    if not n:
        raise ValueError("Cluster name is required")
    if not _EKS_CLUSTER.match(n):
        raise ValueError("Invalid EKS / cluster name characters")
    return n


def sanitize_azure_rg(name: str) -> str:
    n = (name or "").strip()
    if not n:
        raise ValueError("Resource group is required")
    if not _AZ_RG.match(n):
        raise ValueError("Invalid Azure resource group name")
    return n


def sanitize_azure_aks_name(name: str) -> str:
    n = (name or "").strip()
    if not n:
        raise ValueError("AKS cluster name is required")
    if not _AZ_NAME.match(n):
        raise ValueError("Invalid AKS cluster name")
    return n


def list_eks_clusters(region: str, aws_profile: str | None, timeout: int = 60) -> tuple[list[dict[str, Any]], str | None]:
    r = kx._run(
        ["aws", "eks", "list-clusters", "--region", region, "--output", "json"],
        timeout,
        aws_profile=aws_profile,
        cloud="aws",
    )
    if not r.ok:
        return [], r.stderr or r.stdout or "aws eks list-clusters failed"
    try:
        data = json.loads(r.stdout)
    except json.JSONDecodeError:
        return [], "Invalid JSON from aws eks list-clusters"
    names = data.get("clusters") or []
    clusters = [{"name": n, "resourceGroup": "", "location": region, "cloud": "aws"} for n in names]
    return clusters, None


def list_aks_clusters(region: str, timeout: int = 90) -> tuple[list[dict[str, Any]], str | None]:
    arm_loc = AWS_TO_AZURE_LOCATION.get(region)
    if not arm_loc:
        return [], f"No Azure location mapping for region {region!r}"
    r = kx._run(
        ["az", "aks", "list", "--query", f"[?location=='{arm_loc}']", "-o", "json"],
        timeout,
        aws_profile=None,
        cloud="azure",
    )
    if not r.ok:
        return [], r.stderr or r.stdout or "az aks list failed"
    try:
        rows = json.loads(r.stdout)
    except json.JSONDecodeError:
        return [], "Invalid JSON from az aks list"
    clusters: list[dict[str, Any]] = []
    for row in rows:
        name = row.get("name")
        rg = row.get("resourceGroup")
        loc = row.get("location", "")
        if name and rg:
            clusters.append(
                {
                    "name": name,
                    "resourceGroup": rg,
                    "location": loc,
                    "cloud": "azure",
                    "powerState": (row.get("powerState") or {}).get("code", ""),
                }
            )
    return clusters, None


def update_kubeconfig_aws(cluster_name: str, region: str, aws_profile: str | None, timeout: int = 60) -> kx.CmdResult:
    argv = [
        "aws",
        "eks",
        "update-kubeconfig",
        "--name",
        cluster_name,
        "--region",
        region,
    ]
    return kx._run(argv, timeout=timeout, aws_profile=aws_profile, cloud="aws")


def update_kubeconfig_azure(resource_group: str, cluster_name: str, timeout: int = 120) -> kx.CmdResult:
    argv = [
        "az",
        "aks",
        "get-credentials",
        "--resource-group",
        resource_group,
        "--name",
        cluster_name,
        "--overwrite-existing",
    ]
    return kx._run(argv, timeout=timeout, aws_profile=None, cloud="azure")
