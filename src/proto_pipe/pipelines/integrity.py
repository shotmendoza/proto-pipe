"""Integrity module — row-level type checking and flagging.

Shared between ingest.py (Scenario A/B pre-scans) and migration.py
(pre-scan before column type changes). Lives here to avoid circular
imports between those two modules.

Dependency chain:
  db.py ← no proto_pipe imports
  integrity.py ← db.py only
  migration.py ← db.py + integrity.py
  ingest.py ← db.py + integrity.py + migration.py
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Callable

import duckdb
import pandas as pd

from proto_pipe.constants import NUMERIC_DUCKDB_TYPES
from proto_pipe.io.db import get_column_types, init_flagged_rows


# ---------------------------------------------------------------------------
# IntegrityResult
# ---------------------------------------------------------------------------

@dataclass
class IntegrityResult:
    """Result of a pre-scan integrity check on a single row and column.

    Produced by validators in _ROW_VALIDATORS before any INSERT is attempted.
    Rows that produce IntegrityResult entries are written to flagged_rows and
    excluded from the INSERT — clean rows proceed normally.

    Attributes:
        pk_value:   Primary key value identifying the row. Falls back to
                    None (uuid4 flag identity) if no primary key is defined.
        column:     Column where the incompatibility was detected.
        reason:     Plain-English description of what went wrong.
        suggestion: Optional hint for how the user can resolve the issue.
    """

    pk_value: str | None
    column: str
    reason: str
    suggestion: str | None = None


# ---------------------------------------------------------------------------
# Pre-scan
# ---------------------------------------------------------------------------

def check_type_compatibility(
    df: pd.DataFrame,
    reference_types: dict[str, str],
    conn: duckdb.DuckDBPyConnection,
    pk_col: str | None,
) -> list[IntegrityResult]:
    """Check DataFrame values against declared or existing column types using TRY_CAST.

    Used in Scenario A (first ingest, reference_types from registry),
    Scenario B (subsequent ingest, reference_types from existing table),
    and migration pre-scan (reference_types are the new declared types).

    Pipeline columns (prefixed with _) are always skipped.

    :param df:              DataFrame to scan.
    :param reference_types: {column: declared_type} to check against.
    :param conn:            Open DuckDB connection.
    :param pk_col:          Primary key column for row identification, or None.
    :return: List of IntegrityResult — one per failing row per column.
    """
    results: list[IntegrityResult] = []

    for col, declared_type in reference_types.items():
        if col not in df.columns:
            continue
        if col.startswith("_"):
            continue

        pk_select = f'"{pk_col}"' if pk_col and pk_col in df.columns else "NULL"

        try:
            failing = conn.execute(f"""
                SELECT {pk_select} AS pk_value,
                       CAST("{col}" AS VARCHAR) AS raw_value
                FROM df
                WHERE TRY_CAST("{col}" AS {declared_type}) IS NULL
                AND "{col}" IS NOT NULL
            """).df()
        except Exception as exc:
            results.append(IntegrityResult(
                pk_value=None,
                column=col,
                reason=f"Invalid declared type '{declared_type}': {exc}",
                suggestion=(
                    f"Fix the column_types entry for '{col}'. "
                    f"Valid types: VARCHAR, DOUBLE, BIGINT, BOOLEAN, DATE, TIMESTAMPTZ."
                ),
            ))
            continue

        for _, row in failing.iterrows():
            pk_raw = row["pk_value"]
            pk_value = None if (pk_raw is None or pd.isna(pk_raw)) else str(pk_raw)
            results.append(IntegrityResult(
                pk_value=pk_value,
                column=col,
                reason=f"Value '{row['raw_value']}' cannot be cast to {declared_type}.",
                suggestion=(
                    f"Fix the value in the source file, or run 'vp source edit' "
                    f"to update the declared type for '{col}'."
                ),
            ))

    return results


def _check_numeric_type_conflicts(
    conn: duckdb.DuckDBPyConnection,
    table: str,
    df: pd.DataFrame,
    pk_col: str | None,
) -> list[IntegrityResult]:
    """Scenario B validator: find rows with values incompatible with existing numeric columns.

    Only checks columns typed as numeric in DuckDB. Non-numeric columns
    are skipped — DuckDB handles those implicitly.
    """
    existing_types = get_column_types(conn, table)
    numeric_reference = {
        col: db_type
        for col, db_type in existing_types.items()
        if col in df.columns
        and not col.startswith("_")
        and db_type in NUMERIC_DUCKDB_TYPES
    }
    return check_type_compatibility(df, numeric_reference, conn, pk_col)


_ROW_VALIDATORS: list[Callable] = [
    _check_numeric_type_conflicts,
]
"""Ordered list of pre-scan validators run before INSERT on subsequent ingests.

Each validator: (conn, table, df, pk_col) -> list[IntegrityResult].
Add new validators here — the ingest loop never needs to change.
"""


# ---------------------------------------------------------------------------
# Writing flags
# ---------------------------------------------------------------------------

def write_integrity_flags(
    conn: duckdb.DuckDBPyConnection,
    table: str,
    issues: list[IntegrityResult],
) -> int:
    """Write IntegrityResult entries to flagged_rows. Returns count written.

    Uses ON CONFLICT DO NOTHING — idempotent on re-run.
    check_name is 'type_conflict' to distinguish from 'duplicate_conflict'.
    """
    if not issues:
        return 0

    import hashlib

    def _flag_id(pk_value: str | None) -> str:
        if pk_value is None:
            import uuid
            return str(uuid.uuid4())
        return hashlib.md5(str(pk_value).encode()).hexdigest()

    init_flagged_rows(conn)

    flags_df = pd.DataFrame({
        "id": [_flag_id(i.pk_value) for i in issues],
        "table_name": table,
        "check_name": "type_conflict",
        "reason": [f"[{i.column}] {i.reason}"[:500] for i in issues],
        "flagged_at": datetime.now(timezone.utc),
    })

    conn.execute("""
        INSERT INTO flagged_rows (id, table_name, check_name, reason, flagged_at)
        SELECT id, table_name, check_name, reason, flagged_at
        FROM flags_df
        ON CONFLICT (id) DO NOTHING
    """)
    return len(issues)


# ---------------------------------------------------------------------------
# Applying declared types
# ---------------------------------------------------------------------------

def apply_declared_types(
    df: pd.DataFrame,
    column_types: dict[str, str],
) -> pd.DataFrame:
    """Cast DataFrame columns to their declared types after a successful pre-scan.

    Called in Scenario A and migration after _check_type_compatibility confirms
    all values are castable. Ensures the table is created with the correct
    DuckDB types rather than whatever pandas inferred.
    """
    df = df.copy()
    for col, declared_type in column_types.items():
        if col not in df.columns:
            continue
        # Strip date format suffix if present (DATE|%m-%d-%y)
        dt = declared_type.split("|")[0].upper()
        try:
            if dt in ("DOUBLE", "FLOAT", "REAL"):
                df[col] = pd.to_numeric(df[col], errors="coerce")
            elif dt in ("BIGINT", "INTEGER", "INT", "SMALLINT", "TINYINT", "HUGEINT"):
                df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
            elif dt == "BOOLEAN":
                df[col] = df[col].astype(bool)
            # VARCHAR, DATE, TIMESTAMPTZ — leave as-is; DuckDB handles them
        except Exception:
            pass
    return df
