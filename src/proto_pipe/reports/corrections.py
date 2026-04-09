"""Corrections module.

Handles the flagged-row correction workflow:

  1. export_flagged — write blocked rows for a table to CSV so the user
                      can review and fix them outside the pipeline.
  2. import_corrections — read the corrected CSV back in, UPDATE matching
                          rows in the table by primary key, and clear the
                          resolved entries from source_block.

Note: export_flagged / import_corrections are the low-level implementation.
The CLI layer (vp flagged open / vp flagged retry) calls build_source_flag_export
from flagging.py for the enriched view. This module handles direct file I/O
and the correction write-back, shared by both CLI paths.

Row identity:
  source_block.pk_value stores the raw primary key as VARCHAR.
  Join: CAST(source.pk_col AS VARCHAR) = source_block.pk_value
  (No md5 at query time — pk_value is stored directly.)
"""
from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import duckdb
import pandas as pd

from proto_pipe.io.ingest import load_file


# ---------------------------------------------------------------------------
# Export flagged rows
# ---------------------------------------------------------------------------

def export_flagged(
    conn: duckdb.DuckDBPyConnection,
    table_name: str,
    output_path: str,
    primary_key: str,
) -> int:
    """Join source_block metadata with actual table rows and write to CSV.

    Uses pk_value column for the join — no md5 computation at query time.
    The output includes all source columns plus _flag_* guide columns.

    Returns the number of rows exported.
    Raises ValueError if no blocked rows exist for the table.
    """
    if not primary_key:
        raise ValueError(
            "primary_key is required for export_flagged. "
            "Set primary_key in sources_config.yaml and pass it here."
        )

    flag_count = conn.execute(
        "SELECT count(*) FROM source_block WHERE table_name = ?",
        [table_name],
    ).fetchone()[0]

    if flag_count == 0:
        raise ValueError(f"No flagged rows found for table '{table_name}'")

    source_df = conn.execute(f"""
        SELECT
            f.id          AS _flag_id,
            f.reason      AS _flag_reason,
            f.bad_columns AS _flag_columns,
            f.check_name  AS _flag_check,
            s.*
        FROM "{table_name}" s
        JOIN source_block f
          ON CAST(s."{primary_key}" AS VARCHAR) = f.pk_value
        WHERE f.table_name = ?
        ORDER BY f.flagged_at
    """, [table_name]).df()

    if source_df.empty:
        raise ValueError(
            f"No source rows matched blocked ids for table '{table_name}'. "
            f"Ensure primary_key='{primary_key}' matches sources_config.yaml."
        )

    flag_cols = ["_flag_id", "_flag_reason", "_flag_columns", "_flag_check"]
    other_cols = [c for c in source_df.columns if c not in flag_cols]
    source_df = source_df[flag_cols + other_cols]

    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    source_df.to_csv(output_path, index=False)
    return len(source_df)


def dated_export_path(output_dir: str, table_name: str) -> str:
    """Build the default export filename including today's date.

    Format: flagged_<table>_YYYY-MM-DD.csv
    """
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    return str(Path(output_dir) / f"flagged_{table_name}_{today}.csv")


# ---------------------------------------------------------------------------
# Import corrections
# ---------------------------------------------------------------------------


def import_corrections(
    conn: "duckdb.DuckDBPyConnection",
    table_name: str,
    corrections_path: str,
    primary_key: str,
) -> dict:
    """Read a corrected CSV and UPDATE matching rows in the table by primary key.

    Clears resolved entries from source_block and validation_block.

    The corrections file is expected to be the output of export_flagged or
    build_source_flag_export — it may contain _flag_* columns which are stripped
    before the UPDATE.

    :param conn:              Open DuckDB connection to pipeline.db.
    :param table_name:        The table to update.
    :param corrections_path:  Path to the corrected CSV file.
    :param primary_key:       Column name to match rows on (e.g. "van_id").
    :return: dict with keys: updated, flagged_cleared, validation_cleared,
             not_found, not_found_keys.
    """
    from pathlib import Path

    from proto_pipe.io.db import get_registry_types, upsert_via_staging
    from proto_pipe.io.ingest import load_file
    from proto_pipe.io.migration import apply_declared_types
    from proto_pipe.pipelines.flagging import clear_flags

    path = Path(corrections_path)
    if not path.exists():
        raise FileNotFoundError(f"Corrections file not found: {corrections_path}")

    df = load_file(path)

    # Apply declared types from column_type_registry so write-back uses
    # confirmed types, not pandas inference from the CSV round-trip.
    user_cols = [c for c in df.columns if not c.startswith("_")]
    registry_types = get_registry_types(conn, user_cols)
    if registry_types:
        df = apply_declared_types(df, registry_types)

    if primary_key not in df.columns:
        raise ValueError(
            f"Primary key column '{primary_key}' not found in corrections file. "
            f"Columns present: {list(df.columns)}"
        )

    # Collect _flag_ids before stripping annotation columns
    flag_ids = df["_flag_id"].dropna().tolist() if "_flag_id" in df.columns else []

    # Strip all _-prefixed columns — never written to the source table
    data_cols = [c for c in df.columns if not c.startswith("_")]
    df = df[data_cols]

    # Get columns that exist in the target table
    table_cols = set(
        conn.execute(
            "SELECT column_name FROM information_schema.columns WHERE table_name = ?",
            [table_name],
        )
        .df()["column_name"]
        .tolist()
    )

    update_cols = [c for c in df.columns if c in table_cols and c != primary_key]

    if not update_cols:
        raise ValueError(
            f"No updatable columns found. Corrections file columns: {list(df.columns)}, "
            f"table columns: {sorted(table_cols)}"
        )

    # Check for keys in corrections file that don't exist in the table
    existing_keys = set(
        conn.execute(f'SELECT DISTINCT "{primary_key}" FROM "{table_name}"')
        .df()[primary_key]
        .tolist()
    )
    correction_keys = set(df[primary_key].dropna().tolist())
    not_found_keys = sorted(correction_keys - existing_keys)
    not_found = len(not_found_keys)

    # UPDATE via shared staging helper
    upsert_via_staging(conn, table_name, df, primary_key)
    updated = len(df) - not_found

    # Clear resolved entries from source_block and validation_block
    flagged_cleared = clear_flags(conn, "source_block", flag_ids)

    try:
        validation_cleared = clear_flags(conn, "validation_block", flag_ids)
    except Exception:
        validation_cleared = 0

    return {
        "updated": updated,
        "flagged_cleared": flagged_cleared,
        "validation_cleared": validation_cleared,
        "not_found": not_found,
        "not_found_keys": not_found_keys[:5],
    }

