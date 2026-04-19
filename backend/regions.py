"""Allowed region codes for cluster listing (AWS-style codes; Azure listing maps to ARM locations)."""

from __future__ import annotations

REGION_CODES: tuple[str, ...] = (
    "eu-west-1",
    "us-east-1",
    "us-east-2",
    "ap-southeast-1",
    "ap-southeast-2",
    "ap-east-1",
    "il-central-1",
    "ca-central-1",
)

# Approximate map: AWS public region code → Azure ARM `location` filter for `az aks list`.
AWS_TO_AZURE_LOCATION: dict[str, str] = {
    "eu-west-1": "northeurope",
    "us-east-1": "eastus",
    "us-east-2": "eastus2",
    "ap-southeast-1": "southeastasia",
    "ap-southeast-2": "australiaeast",
    "ap-east-1": "eastasia",
    "il-central-1": "israelcentral",
    "ca-central-1": "canadacentral",
}


def sanitize_region(code: str) -> str:
    c = (code or "").strip()
    if c not in REGION_CODES:
        raise ValueError(f"Region must be one of: {', '.join(REGION_CODES)}")
    return c


def sanitize_cloud(cloud: str) -> str:
    v = (cloud or "").strip().lower()
    if v not in ("aws", "azure"):
        raise ValueError('cloud must be "aws" or "azure"')
    return v
