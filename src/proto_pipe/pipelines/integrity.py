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

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Callable

import duckdb
import pandas as pd

from proto_pipe.constants import NUMERIC_DUCKDB_TYPES
from proto_pipe.io.db import get_column_types, flag_id_for


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
        pk_value:   Primary key value identifying the row.
        column:     Column where the incompatibility was detected.
        reason:     Plain-English description of what went wrong.
        suggestion: Optional hint for how the user can resolve the issue.
        filename:   Source filename that produced this issue. Set by the
                    ingest loop after validators run so the flag reason
                    tells the user exactly which file to investigate.
    """

    pk_value: str | None
    column: str
    reason: str
    suggestion: str | None = None
    filename: str | None = None


# ---------------------------------------------------------------------------
# Pre-scan
# ---------------------------------------------------------------------------
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

    Uses ON CONFLICT DO NOTHING — running the same file twice won't duplicate flags.
    check_name is set to 'type_conflict' to distinguish from duplicate_conflict entries.
    Filename is appended to the reason when present so the user knows which file to fix.
    """
    if not issues:
        return 0

    def _reason(i: IntegrityResult) -> str:
        base = f"[{i.column}] {i.reason}"
        if i.filename:
            base += f" (file: {i.filename})"
        return base[:500]

    flags_df = pd.DataFrame(
        {
            "id": [flag_id_for(i.pk_value) for i in issues],
            "table_name": table,
            "check_name": "type_conflict",
            "reason": [_reason(i) for i in issues],
            "flagged_at": datetime.now(timezone.utc),
        }
    )

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

    For DATE/TIMESTAMPTZ columns with a format suffix (e.g. "DATE|%m-%d-%y"),
    uses pd.to_datetime with the declared format so DuckDB receives proper
    date values rather than unparseable strings.

    :param df:           The incoming DataFrame.
    :param column_types: {column_name: declared_type} — may include format suffix.
    :return: DataFrame with columns cast to declared types.
    """

    df = df.copy()
    for col, declared_type in column_types.items():
        if col not in df.columns:
            continue

        # Parse optional format suffix
        if "|" in declared_type:
            dt, fmt = declared_type.split("|", 1)
            dt = dt.upper()
        else:
            dt = declared_type.upper()
            fmt = None

        try:
            if dt in ("DOUBLE", "FLOAT", "REAL"):
                df[col] = pd.to_numeric(df[col], errors="coerce")
            elif dt in ("BIGINT", "INTEGER", "INT", "SMALLINT", "TINYINT", "HUGEINT"):
                df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
            elif dt == "BOOLEAN":
                df[col] = df[col].astype(bool)
            elif dt in ("DATE", "TIMESTAMPTZ") and fmt:
                # Parse using declared format so DuckDB gets proper date objects
                parsed = pd.to_datetime(df[col], format=fmt, errors="coerce")
                if dt == "DATE":
                    # Strip time component — DuckDB DATE column doesn't want it
                    df[col] = parsed.dt.date
                else:
                    # TIMESTAMPTZ — keep as datetime, DuckDB handles timezone
                    df[col] = parsed
            # VARCHAR, DATE/TIMESTAMPTZ without format — leave as-is
        except Exception:
            pass

    return df


def check_type_compatibility(
    df: pd.DataFrame,
    reference_types: dict[str, str],
    conn: duckdb.DuckDBPyConnection,
    pk_col: str | None,
) -> list[IntegrityResult]:
    """Check DataFrame values against declared or existing column types.

    Handles TYPE|format entries for DATE and TIMESTAMPTZ columns by using
    TRY_STRPTIME instead of TRY_CAST for those columns.

    :param df:              DataFrame to scan.
    :param reference_types: {column: declared_type} — may include format suffix
                            e.g. "DATE|%m-%d-%y" or plain "VARCHAR".
    :param conn:            Open the DuckDB connection.
    :param pk_col:          Primary key column for row identification, or None.
    :return: List of IntegrityResult — one per failing row per column.
    """

    results: list[IntegrityResult] = []

    for col, declared_type in reference_types.items():
        if col not in df.columns:
            continue
        if col.startswith("_"):
            continue

        # Parse optional format suffix: "DATE|%m-%d-%y" → ("DATE", "%m-%d-%y")
        if "|" in declared_type:
            base_type, fmt = declared_type.split("|", 1)
            base_type = base_type.upper()
        else:
            base_type, fmt = declared_type.upper(), None

        pk_select = f'"{pk_col}"' if pk_col and pk_col in df.columns else "NULL"

        try:
            if fmt and base_type in ("DATE", "TIMESTAMPTZ"):
                # Use TRY_STRPTIME for date columns with a declared format string.
                # TRY_CAST cannot handle non-ISO date strings; TRY_STRPTIME applies
                # the exact format the user declared.
                failing = conn.execute(f"""
                    SELECT {pk_select} AS pk_value,
                           CAST("{col}" AS VARCHAR) AS raw_value
                    FROM df
                    WHERE TRY_STRPTIME(CAST("{col}" AS VARCHAR), '{fmt}') IS NULL
                    AND "{col}" IS NOT NULL
                """).df()
            else:
                failing = conn.execute(f"""
                    SELECT {pk_select} AS pk_value,
                           CAST("{col}" AS VARCHAR) AS raw_value
                    FROM df
                    WHERE TRY_CAST("{col}" AS {base_type}) IS NULL
                    AND "{col}" IS NOT NULL
                """).df()
        except Exception as exc:
            results.append(
                IntegrityResult(
                    pk_value=None,
                    column=col,
                    reason=f"Invalid declared type '{declared_type}': {exc}",
                    suggestion=(
                        f"Fix the column_types entry for '{col}'. "
                        f"Valid types: VARCHAR, DOUBLE, BIGINT, BOOLEAN, DATE, TIMESTAMPTZ."
                    ),
                )
            )
            continue

        for _, row in failing.iterrows():
            pk_raw = row["pk_value"]
            pk_value = None if (pk_raw is None or pd.isna(pk_raw)) else str(pk_raw)
            fmt_note = f" using format '{fmt}'" if fmt else ""
            results.append(
                IntegrityResult(
                    pk_value=pk_value,
                    column=col,
                    reason=f"Value '{row['raw_value']}' cannot be cast to {base_type}{fmt_note}.",
                    suggestion=(
                        f"Fix the value in the source file, or run 'vp source edit' "
                        f"to update the declared type for '{col}'."
                    ),
                )
            )

    return results
