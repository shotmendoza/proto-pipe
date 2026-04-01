"""Data layer — raw DuckDB operations.

This module owns all direct DuckDB interactions that are not tied to
specific business logic. Business logic modules (ingest, migration) call
these functions rather than executing SQL directly.
"""
from __future__ import annotations

import hashlib
import json
import uuid
from datetime import datetime, timezone

import duckdb


# ---------------------------------------------------------------------------
# Generic table inspection
# ---------------------------------------------------------------------------

def table_exists(conn: duckdb.DuckDBPyConnection, table: str) -> bool:
    """Return True if the table exists in the connected DB."""
    result = conn.execute(
        "SELECT count(*) FROM information_schema.tables WHERE table_name = ?",
        [table],
    ).fetchone()
    return result is not None and result[0] > 0


def get_column_types(conn: duckdb.DuckDBPyConnection, table: str) -> dict[str, str]:
    """Return {column_name: data_type} for all columns in a table.

    Returns an empty dict if the table does not exist.
    """
    rows = conn.execute(
        "SELECT column_name, data_type FROM information_schema.columns WHERE table_name = ?",
        [table],
    ).fetchall()
    return {row[0]: row[1].upper() for row in rows}


def get_columns(conn: duckdb.DuckDBPyConnection, table: str) -> set[str]:
    """Return column names for a table."""
    return set(get_column_types(conn, table).keys())


def get_all_tables(conn: duckdb.DuckDBPyConnection) -> list[str]:
    """Return all table names in the connected DB, sorted alphabetically."""
    return conn.execute("""
        SELECT table_name FROM information_schema.tables
        WHERE table_schema = 'main'
        ORDER BY table_name
    """).df()["table_name"].tolist()


# ---------------------------------------------------------------------------
# Table bootstrap
# ---------------------------------------------------------------------------

def init_ingest_log(conn: duckdb.DuckDBPyConnection) -> None:
    """Create the ingest_log table if it doesn't exist. Safe to call multiple times."""
    conn.execute("""
        CREATE TABLE IF NOT EXISTS ingest_log (
            id          VARCHAR PRIMARY KEY,
            filename    VARCHAR NOT NULL,
            table_name  VARCHAR,
            status      VARCHAR NOT NULL,
            rows        INTEGER,
            new_cols    VARCHAR,
            message     VARCHAR,
            ingested_at TIMESTAMPTZ NOT NULL
        )
    """)


def init_flagged_rows(conn: duckdb.DuckDBPyConnection) -> None:
    """Create the flagged_rows table if it doesn't exist. Safe to call multiple times."""
    conn.execute("""
        CREATE TABLE IF NOT EXISTS flagged_rows (
            id          VARCHAR PRIMARY KEY,
            table_name  VARCHAR NOT NULL,
            check_name  VARCHAR NOT NULL,
            reason      VARCHAR,
            flagged_at  TIMESTAMPTZ NOT NULL
        )
    """)


def init_check_registry_metadata(conn: duckdb.DuckDBPyConnection) -> None:
    """Create check_registry_metadata table if it doesn't exist. Safe to call multiple times."""
    conn.execute("""
        CREATE TABLE IF NOT EXISTS check_registry_metadata (
            id                      VARCHAR PRIMARY KEY,
            check_name              VARCHAR NOT NULL UNIQUE,
            check_key               VARCHAR NOT NULL,
            is_multiselect_eligible BOOLEAN NOT NULL,
            column_params           VARCHAR,
            scalar_params           VARCHAR,
            recorded_at             TIMESTAMPTZ NOT NULL
        )
    """)


def init_column_type_registry(conn: duckdb.DuckDBPyConnection) -> None:
    """Create the column_type_registry table if it doesn't exist. Safe to call multiple times."""
    conn.execute("""
        CREATE TABLE IF NOT EXISTS column_type_registry (
            column_name   VARCHAR NOT NULL,
            source_name   VARCHAR NOT NULL,
            declared_type VARCHAR NOT NULL,
            recorded_at   TIMESTAMPTZ NOT NULL,
            PRIMARY KEY (column_name, source_name)
        )
    """)


def init_all_pipeline_tables(conn: duckdb.DuckDBPyConnection) -> None:
    """Bootstrap all pipeline-managed tables. Called by vp db-init.

    Safe to call multiple times — all init functions use CREATE IF NOT EXISTS.
    """
    init_ingest_log(conn)
    init_check_registry_metadata(conn)
    init_column_type_registry(conn)
    init_flagged_rows(conn)


# ---------------------------------------------------------------------------
# column_type_registry operations
# ---------------------------------------------------------------------------

def get_registry_types(
    conn: duckdb.DuckDBPyConnection,
    columns: list[str] | None = None,
) -> dict[str, str]:
    """Return {column_name: declared_type} from column_type_registry.

    When a column has entries from multiple sources, the most recently
    confirmed type wins. Falls back to {} if the table doesn't exist yet.
    """
    try:
        rows = conn.execute("""
            SELECT DISTINCT ON (column_name) column_name, declared_type
            FROM column_type_registry
            ORDER BY column_name, recorded_at DESC
        """).fetchall()
    except Exception:
        return {}

    result = {row[0]: row[1] for row in rows}
    if columns is not None:
        result = {k: v for k, v in result.items() if k in columns}
    return result


def get_registry_hints(
    conn: duckdb.DuckDBPyConnection,
    columns: list[str] | None = None,
) -> dict[str, dict[str, str]]:
    """Return {column_name: {source_name: declared_type}} from column_type_registry.

    Used for display — surfaces conflicts when sources disagree.
    Falls back to {} if the table doesn't exist yet.
    """
    try:
        query = "SELECT column_name, source_name, declared_type FROM column_type_registry"
        params: list = []
        if columns:
            placeholders = ", ".join(["?"] * len(columns))
            query += f" WHERE column_name IN ({placeholders})"
            params = list(columns)
        query += " ORDER BY column_name, recorded_at DESC"
        rows = conn.execute(query, params).fetchall()
    except Exception:
        return {}

    result: dict[str, dict[str, str]] = {}
    for col, source, dtype in rows:
        result.setdefault(col, {})[source] = dtype
    return result


def write_registry_types(
    conn: duckdb.DuckDBPyConnection,
    source_name: str,
    column_types: dict[str, str],
) -> None:
    """Upsert confirmed column types into column_type_registry."""
    if not column_types:
        return
    now = datetime.now(timezone.utc)
    for col, dtype in column_types.items():
        conn.execute("""
            INSERT INTO column_type_registry
                (column_name, source_name, declared_type, recorded_at)
            VALUES (?, ?, ?, ?)
            ON CONFLICT (column_name, source_name)
            DO UPDATE SET declared_type = excluded.declared_type,
                          recorded_at   = excluded.recorded_at
        """, [col, source_name, dtype, now])


def delete_registry_types_for_source(
    conn: duckdb.DuckDBPyConnection,
    source_name: str,
) -> None:
    """Remove all column_type_registry entries for a given source."""
    try:
        conn.execute(
            "DELETE FROM column_type_registry WHERE source_name = ?",
            [source_name],
        )
    except Exception:
        pass


# ---------------------------------------------------------------------------
# ingest_log operations
# ---------------------------------------------------------------------------

def log_ingest(
    conn: duckdb.DuckDBPyConnection,
    filename: str,
    table_name: str | None,
    status: str,
    rows: int | None = None,
    new_cols: list[str] | None = None,
    message: str | None = None,
) -> None:
    """Insert one ingest attempt record into ingest_log."""
    conn.execute("""
        INSERT INTO ingest_log
            (id, filename, table_name, status, rows, new_cols, message, ingested_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """, [
        str(uuid.uuid4()),
        filename,
        table_name,
        status,
        rows,
        json.dumps(new_cols) if new_cols else None,
        message,
        datetime.now(timezone.utc),
    ])


def already_ingested(conn: duckdb.DuckDBPyConnection, filename: str) -> bool:
    """Return True if filename has a successful ingest_log entry."""
    result = conn.execute("""
        SELECT count(*) FROM ingest_log
        WHERE filename = ? AND status = 'ok'
    """, [filename]).fetchone()
    return result is not None and result[0] > 0


def get_ingested_filenames(conn: duckdb.DuckDBPyConnection) -> set[str]:
    """Return all filenames with status='ok' in ingest_log."""
    try:
        rows = conn.execute(
            "SELECT filename FROM ingest_log WHERE status = 'ok'"
        ).fetchall()
        return {row[0] for row in rows}
    except Exception:
        return set()


def flag_id_for(
        pk_value: str | int | float | None,
) -> str:
    """Return the deterministic flag id for a given primary key value.

    id = md5(str(pk_value))

    Using md5 means the same expression is computable in DuckDB SQL:
        md5(CAST(source.pk_col AS VARCHAR))

    If pk_value is null, then will return a UUID4 with a string wrap.
    """
    if pk_value is None:
        return str(uuid.uuid4())
    return hashlib.md5(str(pk_value).encode()).hexdigest()
