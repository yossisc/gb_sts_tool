"""Built-in AWS named profiles for this tool (not read from ~/.aws/credentials)."""

from __future__ import annotations

# Preset list only — do not scan local AWS config files.
AWS_PROFILE_DEFAULT = "glassbox_AWS_MT"

AWS_PROFILE_OPTIONS: tuple[str, ...] = (
    "glassbox_AWS_MT",
    "glassbox_prod",
    "glassbox_PROD_MT",
    "glassbox_PROD_ST",
    "glassbox_PROD_ST2",
    "glassbox_WF",
    "glassbox_STG",
)


def list_profiles() -> tuple[list[str], dict[str, str]]:
    """Returns (profiles in display order, meta)."""
    meta = {
        "source": "builtin",
        "default_profile": AWS_PROFILE_DEFAULT,
    }
    return list(AWS_PROFILE_OPTIONS), meta


def is_allowed_profile(name: str) -> bool:
    return name in AWS_PROFILE_OPTIONS
