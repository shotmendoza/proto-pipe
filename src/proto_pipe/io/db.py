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

from proto_pipe.constants import NULLABLE_EXTENSION_DTYPES


################################
# PIPELINE EVENT FUNCTIONALITY
################################
def init_pipeline_events(conn: duckdb.DuckDBPyConnection) -> None:
    """Create the pipeline_events table if it doesn't exist. Safe to call multiple times.

    Structured event log written by CLI commands (vp ingest, vp validate,
    vp run-all). Used by vp export log to review and archive run history.

    event_type values:
      ingest_ok: file ingested successfully
      ingest_failed: file failed to ingest (exception or bad file)
      validation_passed: report ran, all checks passed
      validation_failed: report ran, check failures written to validation_block
      report_error: report run itself crashed (source missing, exception)
      deliverable_produced: deliverable written to disk successfully

    severity values: info | warn | error
    """
    conn.execute("""
                 CREATE TABLE IF NOT EXISTS pipeline_events (
                                                                event_type  VARCHAR     NOT NULL,
                                                                source_name VARCHAR,
                                                                severity    VARCHAR     NOT NULL,
                                                                detail      VARCHAR,
                                                                occurred_at TIMESTAMPTZ NOT NULL
                 )
                 """)


def write_pipeline_events(
    pipeline_db: str,
    events: list[dict],
) -> None:
    """Write one or more events to pipeline_events. Fire-and-forget.

    Opens its own short-lived connection, so callers need no connection
    management. Exceptions are silently swallowed — event writing failures
    must never crash the pipeline.

    Each event dict must have: event_type, severity.
    Optional keys: source_name, detail.

    :param pipeline_db: Path to pipeline.db.
    :param events: List of event dicts to write.
    """
    if not events:
        return
    try:
        import duckdb as _duckdb

        conn = _duckdb.connect(pipeline_db)
        try:
            now = datetime.now(timezone.utc)
            for e in events:
                conn.execute(
                    """
                    INSERT INTO pipeline_events
                        (event_type, source_name, severity, detail, occurred_at)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    [
                        e["event_type"],
                        e.get("source_name"),
                        e["severity"],
                        (e.get("detail") or "")[:1000],
                        now,
                    ],
                )
        finally:
            conn.close()
    except Exception:
        pass  # event failures are never surfaced to the user


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

def init_ingest_state(conn: duckdb.DuckDBPyConnection) -> None:
    """Create the ingest_state table if it doesn't exist. Safe to call multiple times.

    Tracks per-file ingest history. Was: ingest_log.
    status values: ok | failed | skipped | correction
    """
    conn.execute("""
        CREATE TABLE IF NOT EXISTS ingest_state (
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


def init_source_block(conn: duckdb.DuckDBPyConnection) -> None:
    """Create the source_block table if it doesn't exist. Safe to call multiple times.

    Stores ingest-time row conflicts (type mismatch, duplicate with changed
    values). Hard-blocks deliverables until resolved. Was: flagged_rows.

    check_name values: type_conflict | duplicate_conflict
    bad_columns: pipe-delimited column names e.g. "amount|region"
    """
    conn.execute("""
        CREATE TABLE IF NOT EXISTS source_block (
            id                  VARCHAR PRIMARY KEY,
            table_name          VARCHAR NOT NULL,
            check_name          VARCHAR NOT NULL,
            pk_value            VARCHAR,
            source_file         VARCHAR,
            source_file_missing BOOLEAN DEFAULT FALSE,
            bad_columns         VARCHAR,
            reason              VARCHAR,
            flagged_at          TIMESTAMPTZ NOT NULL
        )
    """)


def init_source_pass(conn: duckdb.DuckDBPyConnection) -> None:
    """Create the source_pass table if it doesn't exist. Safe to call multiple times.

    Tracks per-record accepted ingest state. _handle_duplicates compares
    incoming row hashes against this table — not the source table directly.
    Updated only on successful ingest or correction. Never updated for
    blocked rows.
    """
    conn.execute("""
        CREATE TABLE IF NOT EXISTS source_pass (
            pk_value    VARCHAR NOT NULL,
            table_name  VARCHAR NOT NULL,
            row_hash    VARCHAR NOT NULL,
            source_file VARCHAR NOT NULL,
            ingested_at TIMESTAMPTZ NOT NULL,
            PRIMARY KEY (pk_value, table_name)
        )
    """)


def init_validation_pass(conn: duckdb.DuckDBPyConnection) -> None:
    """Create the validation_pass table if it doesn't exist. Safe to call multiple times.

    Tracks per-record accepted validation state per report. Enables
    incremental validation — only new/changed records or records affected
    by check set changes are re-validated.

    status values: passed | failed | skipped | corrected
    check_set_hash: md5 of all check names + function source hashes
    """
    conn.execute("""
        CREATE TABLE IF NOT EXISTS validation_pass (
            pk_value       VARCHAR NOT NULL,
            table_name     VARCHAR NOT NULL,
            report_name    VARCHAR NOT NULL,
            row_hash       VARCHAR NOT NULL,
            check_set_hash VARCHAR NOT NULL,
            status         VARCHAR NOT NULL,
            validated_at   TIMESTAMPTZ NOT NULL,
            PRIMARY KEY (pk_value, table_name, report_name)
        )
    """)


def init_validation_block(conn: duckdb.DuckDBPyConnection) -> None:
    """Create the validation_block table if it doesn't exist. Safe to call multiple times.

    Stores check/transform failures from vp validate. Warns but does not
    block deliverables. Mirrors source_block structure but joins to the
    report table (not source files) for the correction view.

    check_name: the registered check/transform name that produced the failure
    bad_columns: pipe-delimited column names e.g. "amount|region"
    """
    conn.execute("""
        CREATE TABLE IF NOT EXISTS validation_block (
            id          VARCHAR PRIMARY KEY,
            table_name  VARCHAR NOT NULL,
            report_name VARCHAR NOT NULL,
            check_name  VARCHAR NOT NULL,
            pk_value    VARCHAR,
            bad_columns VARCHAR,
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
# ingest_state operations (was: ingest_log)
# ---------------------------------------------------------------------------

def log_ingest_state(
    conn: duckdb.DuckDBPyConnection,
    filename: str,
    table_name: str | None,
    status: str,
    rows: int | None = None,
    new_cols: list[str] | None = None,
    message: str | None = None,
) -> None:
    """Insert one ingest attempt record into ingest_state.

    status values: ok | failed | skipped | correction
    """
    conn.execute("""
        INSERT INTO ingest_state
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
    """Return True if filename has a successful ingest_state entry."""
    result = conn.execute("""
        SELECT count(*) FROM ingest_state
        WHERE filename = ? AND status = 'ok'
    """, [filename]).fetchone()
    return result is not None and result[0] > 0


def get_ingested_filenames(conn: duckdb.DuckDBPyConnection) -> set[str]:
    """Return all filenames with status='ok' in ingest_state."""
    try:
        rows = conn.execute(
            "SELECT filename FROM ingest_state WHERE status = 'ok'"
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


# ---------------------------------------------------------------------------
# source_pass operations
# ---------------------------------------------------------------------------

def get_source_pass_hashes(
    conn: duckdb.DuckDBPyConnection,
    table_name: str,
    pk_values: list[str],
) -> dict[str, str]:
    """Return {pk_value: row_hash} for the given pk_values from source_pass.

    Used by _handle_duplicates to compare incoming row hashes without
    querying the source table directly. Missing PKs are new records.
    """
    if not pk_values:
        return {}
    placeholders = ", ".join(["?"] * len(pk_values))
    rows = conn.execute(f"""
        SELECT pk_value, row_hash
        FROM source_pass
        WHERE table_name = ?
        AND pk_value IN ({placeholders})
    """, [table_name] + pk_values).fetchall()
    return {row[0]: row[1] for row in rows}


def bulk_upsert_source_pass(
    conn: duckdb.DuckDBPyConnection,
    table_name: str,
    records: list[dict],
) -> None:
    """Bulk upsert multiple records into source_pass.

    Each record: {pk_value, row_hash, source_file}
    Called after a successful chunk insert in _handle_duplicates.
    Only called for clean accepted rows — never for blocked rows.
    """
    if not records:
        return
    import pandas as pd
    now = datetime.now(timezone.utc)
    df = pd.DataFrame([
        {
            "pk_value": r["pk_value"],
            "table_name": table_name,
            "row_hash": r["row_hash"],
            "source_file": r["source_file"],
            "ingested_at": now,
        }
        for r in records
    ])
    conn.execute("""
        INSERT INTO source_pass (pk_value, table_name, row_hash, source_file, ingested_at)
        SELECT pk_value, table_name, row_hash, source_file, ingested_at FROM df
        ON CONFLICT (pk_value, table_name)
        DO UPDATE SET
            row_hash    = excluded.row_hash,
            source_file = excluded.source_file,
            ingested_at = excluded.ingested_at
    """)


def clear_source_pass_for_table(
    conn: duckdb.DuckDBPyConnection,
    table_name: str,
) -> int:
    """Delete all source_pass entries for a table. Returns count deleted.

    Called by vp delete source to clean up all record-level state.
    """
    result = conn.execute(
        "DELETE FROM source_pass WHERE table_name = ? RETURNING pk_value",
        [table_name],
    ).fetchall()
    return len(result)


def clear_source_block_for_pks(
    conn: duckdb.DuckDBPyConnection,
    table_name: str,
    pk_values: list[str],
) -> int:
    """Delete source_block entries for rows that have been accepted into source_pass.

    Called by ingest.py immediately after bulk_upsert_source_pass so that
    a row cannot simultaneously appear in source_pass (accepted) and
    source_block (flagged). Returns count deleted.

    Behavioural guarantee: a pk_value present in source_pass for a given
    table must not appear in source_block for that same table.
    """
    if not pk_values:
        return 0
    placeholders = ", ".join(["?"] * len(pk_values))
    result = conn.execute(
        f"DELETE FROM source_block WHERE table_name = ? "
        f"AND pk_value IN ({placeholders}) RETURNING id",
        [table_name] + pk_values,
    ).fetchall()
    return len(result)


# ---------------------------------------------------------------------------
# validation_pass operations
# ---------------------------------------------------------------------------


def upsert_validation_pass(
    conn: duckdb.DuckDBPyConnection,
    table_name: str,
    report_name: str,
    pk_value: str,
    row_hash: str,
    check_set_hash: str,
    status: str,
) -> None:
    """Upsert one record into validation_pass after vp validate runs.

    status values: passed | failed | skipped | corrected
    On conflict, updates all fields.
    """
    conn.execute("""
        INSERT INTO validation_pass
            (pk_value, table_name, report_name, row_hash, check_set_hash, status, validated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT (pk_value, table_name, report_name)
        DO UPDATE SET
            row_hash       = excluded.row_hash,
            check_set_hash = excluded.check_set_hash,
            status         = excluded.status,
            validated_at   = excluded.validated_at
    """, [pk_value, table_name, report_name, row_hash, check_set_hash, status,
          datetime.now(timezone.utc)])


def get_validation_pass_hashes(
    conn: duckdb.DuckDBPyConnection,
    table_name: str,
    report_name: str,
    check_set_hash: str,
) -> dict[str, str]:
    """Return {pk_value: row_hash} for records validated with the current check set.

    Used by vp validate to identify pending records:
    - Not in result → never validated → pending
    - In result but source row_hash differs → source changed → pending
    - In result, hashes match → skip
    """
    rows = conn.execute("""
        SELECT pk_value, row_hash
        FROM validation_pass
        WHERE table_name = ?
        AND report_name = ?
        AND check_set_hash = ?
    """, [table_name, report_name, check_set_hash]).fetchall()
    return {row[0]: row[1] for row in rows}


def get_current_check_set_hash(
    conn: duckdb.DuckDBPyConnection,
    report_name: str,
) -> str | None:
    """Return the most recent check_set_hash used for a report.

    Returns None if the report has never been validated.
    Used to detect when the check set has changed between runs.
    """
    result = conn.execute("""
        SELECT check_set_hash
        FROM validation_pass
        WHERE report_name = ?
        ORDER BY validated_at DESC
        LIMIT 1
    """, [report_name]).fetchone()
    return result[0] if result else None


def clear_validation_pass_for_report(
    conn: duckdb.DuckDBPyConnection,
    report_name: str,
) -> int:
    """Delete all validation_pass entries for a report. Returns count deleted.

    Called by vp delete report to clean up all record-level state.
    """
    result = conn.execute(
        "DELETE FROM validation_pass WHERE report_name = ? RETURNING pk_value",
        [report_name],
    ).fetchall()
    return len(result)


def coerce_for_display(df: "pd.DataFrame") -> "pd.DataFrame":
    """Convert pandas nullable extension types to object dtype.

    DuckDB returns integer and boolean columns as pandas nullable extension
    types (Int8, Int16, Int32, Int64, UInt*, boolean). These raise
    TypeError when fillna() is called with a non-integer value, which
    breaks any display or edit layer that calls fillna("") or similar.

    Converting to object dtype preserves values and NA semantics while
    making the DataFrame safe for any downstream pandas operation.

    Only call this for display/edit contexts — not before writing back
    to DuckDB, where apply_declared_types + registry types should be
    used instead.

    :param df: DataFrame returned from a DuckDB .df() call.
    :return:   Copy of df with nullable extension columns cast to object.
    """
    result = df.copy()
    for col in result.columns:
        if isinstance(result[col].dtype, NULLABLE_EXTENSION_DTYPES):
            result[col] = result[col].astype(object)
    return result


################
# KEY FUNCTIONS
################

def ensure_pipeline_tables(conn: duckdb.DuckDBPyConnection) -> None:
    """Ensure all pipeline-managed tables exist on this connection.

    Lightweight guard -- checks for ingest_state as a proxy for full
    initialisation and calls init_all_pipeline_tables only when missing.
    Safe to call at the top of any function that writes to pipeline tables.

    Covers two failure modes:
    1. Fresh DB opened without vp db-init being run first.
    2. vp db-init --migrate on an old DB where tables are partially missing.
    """
    if not table_exists(conn, "ingest_state"):
        init_all_pipeline_tables(conn)


def init_all_pipeline_tables(conn: duckdb.DuckDBPyConnection) -> None:
    """Bootstrap all pipeline-managed tables. Called by vp db-init.

    Safe to call multiple times — all init functions use CREATE IF NOT EXISTS.
    """
    init_ingest_state(conn)
    init_source_pass(conn)
    init_source_block(conn)
    init_validation_pass(conn)
    init_validation_block(conn)
    init_check_registry_metadata(conn)
    init_column_type_registry(conn)
    init_pipeline_events(conn)
