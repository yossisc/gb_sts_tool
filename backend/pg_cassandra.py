"""PostgreSQL / Cassandra read-only helpers for kubectl-sidecar exec."""

from __future__ import annotations

import re

_PG_IDENT = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]{0,62}$")


def sanitize_postgres_schema(schema: str) -> str:
    """Allow typical PostgreSQL identifier-style schema names (read-only catalog queries only)."""
    s = (schema or "").strip()
    if not _PG_IDENT.match(s):
        raise ValueError("invalid schema name (letters, digits, underscore; max 63 chars)")
    return s


def classify_postgres_sql(sql: str) -> tuple[bool, str]:
    """Conservative read-only check for ad-hoc psql (Glassbox operator)."""
    s = (sql or "").strip()
    if not s:
        return False, "empty SQL"
    low = s.lower()
    if ";" in s.rstrip().rstrip(";"):
        return False, "multiple statements (;)"
    dangerous = (
        "insert ",
        "update ",
        "delete ",
        "alter ",
        "drop ",
        "truncate ",
        "create ",
        "grant ",
        "revoke ",
        "copy ",
        "call ",
        "execute ",
        "vacuum ",
        "reindex ",
        "cluster ",
        "refresh ",
    )
    for d in dangerous:
        if d in low:
            return False, f"write-like SQL ({d.strip()})"
    if re.match(r"^\s*(with\s+)?select\b", low):
        return True, ""
    if re.match(r"^\s*show\b", low):
        return True, ""
    if re.match(r"^\s*explain\b", low):
        return True, ""
    if re.match(r"^\s*table\b", low):
        return True, ""
    return False, "only SELECT / SHOW / EXPLAIN / TABLE are auto-approved"


def postgres_tables_sql(schema: str) -> str:
    """List tables in a fixed schema (read-only)."""
    s = sanitize_postgres_schema(schema)
    return (
        "SELECT table_name FROM information_schema.tables "
        f"WHERE table_schema = '{s}' AND table_type = 'BASE TABLE' "
        "ORDER BY table_name LIMIT 500"
    )


def postgres_databases_sql() -> str:
    """List non-template databases (read-only)."""
    return "SELECT datname FROM pg_database WHERE datistemplate IS FALSE ORDER BY datname"


def postgres_schemas_sql() -> str:
    """List schemas in the current database (read-only)."""
    return (
        "SELECT schema_name FROM information_schema.schemata "
        "WHERE catalog_name = current_database() ORDER BY schema_name"
    )


_CASS_KEYSPACE = re.compile(r"^[A-Za-z][A-Za-z0-9_]{0,62}$")


def sanitize_cassandra_keyspace(name: str) -> str:
    n = (name or "").strip()
    if not n:
        raise ValueError("keyspace is required")
    if not _CASS_KEYSPACE.match(n):
        raise ValueError("invalid keyspace name")
    return n


def classify_cassandra_cql(cql: str) -> tuple[bool, str]:
    s = (cql or "").strip()
    if not s:
        return False, "empty CQL"
    low = s.lower()
    if ";" in s.rstrip().rstrip(";"):
        return False, "multiple statements (;)"
    dangerous = (
        "insert ",
        "update ",
        "delete ",
        "drop ",
        "alter ",
        "create ",
        "truncate ",
        "grant ",
        "revoke ",
        "batch ",
        "apply ",
    )
    for d in dangerous:
        if d in low:
            return False, f"write-like CQL ({d.strip()})"
    if re.match(r"^\s*select\b", low):
        return True, ""
    if re.match(r"^\s*describe\b", low):
        return True, ""
    if re.match(r"^\s*desc\b", low):
        return True, ""
    if re.match(r"^\s*list\b", low):
        return True, ""
    return False, "only SELECT / DESCRIBE / LIST are auto-approved"


def cassandra_keyspaces_cql() -> str:
    return "DESCRIBE KEYSPACES;"


def cassandra_desc_keyspace_cql(keyspace: str) -> str:
    ks = sanitize_cassandra_keyspace(keyspace)
    return f"DESCRIBE KEYSPACE {ks};"


def cassandra_tables_cql(keyspace: str) -> str:
    ks = sanitize_cassandra_keyspace(keyspace)
    return "SELECT table_name FROM system_schema.tables " f"WHERE keyspace_name = '{ks}';"
