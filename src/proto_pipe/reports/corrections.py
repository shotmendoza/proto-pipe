"""Corrections module.
Handles the flagged-row correction workflow:

  1. export_flagged — write flagged rows for a table to a CSV so the user
                       can review and fix them outside the pipeline.
  2. import_corrections — read the corrected CSV back in, UPDATE the matching
                          rows in the table by primary key, and clear the
                          resolved entries from flagged_rows.

The primary key is defined per source in sources_config.yaml under `primary_key`.
It can be overridden at the CLI with --key.

Row identity:
  flagged_rows.id = md5(str(pk_value)), set at flag time in ingest.py.

  At export time, the source table is joined to flagged_rows via:
      md5(CAST(source.pk_col AS VARCHAR)) = flagged_rows.id

  This is drift-free (derived from the data value, not row position),
  requires no extra columns in source tables, and DuckDB computes it
  in one vectorised pass before joining to the small flagged_rows result.
"""

from datetime import datetime, timezone
from pathlib import Path

import duckdb
import pandas as pd  # type: ignore

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
    """ Join flagged_rows metadata with the actual table rows and write to CSV.

    Joins on primary_key instead of row_index when primary_key is
    provided. This avoids drift — if rows are inserted or deleted between
    the time the flag is written and the time the export runs, a row_index
    join would produce the wrong row. Joining on the primary key is stable.

    Falls back to row_index join when primary_key is not provided.

    The output filename includes today's date:
        flagged_<table>_YYYY-MM-DD.csv

    The file includes all source columns plus:
        _flag_id — internal ID, keep this when re-importing
        _flag_reason — why the row was flagged, for reference only

    Returns the number of rows exported.
    Raises ValueError if no flagged rows exist for the table.
    """
    if not primary_key:
        raise ValueError(
            "primary_key is required for export_flagged. "
            "Set primary_key in sources_config.yaml and pass it here."
        )

    # Pull flagged metadata for this table
    flag_count = conn.execute(
        "SELECT count(*) FROM flagged_rows WHERE table_name = ?",
        [table_name],
    ).fetchone()[0]

    if flag_count == 0:
        raise ValueError(f"No flagged rows found for table '{table_name}'")

    # Single JOIN — md5 of the primary key column matches flagged_rows.id
    source_df = conn.execute(
        f"""
        SELECT
            f.id AS _flag_id,
            f.reason AS _flag_reason,
            s.*
        FROM "{table_name}" s
        JOIN flagged_rows f
          ON md5(CAST(s."{primary_key}" AS VARCHAR)) = f.id
        WHERE f.table_name = ?
        ORDER BY f.flagged_at
    """, [table_name]).df()

    if source_df.empty:
        raise ValueError(
            f"No source rows matched flagged ids for table '{table_name}'. "
            f"Ensure primary_key='{primary_key}' matches sources_config.yaml."
        )

    # Annotation columns first so they're visible without scrolling
    other_cols = [c for c in source_df.columns if c not in ("_flag_id", "_flag_reason")]
    source_df = source_df[["_flag_id", "_flag_reason"] + other_cols]

    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    source_df.to_csv(output_path, index=False)

    return len(source_df)


def dated_export_path(
        output_dir: str,
        table_name: str
) -> str:
    """Build the default export filename including today's date.
    Format: flagged_<table>_YYYY-MM-DD.csv
    """
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    return str(Path(output_dir) / f"flagged_{table_name}_{today}.csv")


# ---------------------------------------------------------------------------
# Import corrections
# ---------------------------------------------------------------------------

def import_corrections(
    conn: duckdb.DuckDBPyConnection,
    table_name: str,
    corrections_path: str,
    primary_key: str,
) -> dict:
    """Read a corrected CSV and UPDATE matching rows in the table by primary key.
    Clears resolved entries from flagged_rows.

    The corrections file is expected to be the output of export_flagged —
    it may contain _flag_id and _flag_reason columns, which are stripped
    before the UPDATE so they don't contaminate the source table.

    Args:
        conn: Open DuckDB connection to pipeline.db.
        table_name: The table to update.
        corrections_path: Path to the corrected CSV file.
        primary_key: Column name to match rows on (e.g. "order_id").

    Returns dict with keys: updated (int), flagged_cleared (int).
    Raises FileNotFoundError if the correction file doesn't exist.
    Raises ValueError if the primary key column is missing from the file.
    """
    path = Path(corrections_path)
    if not path.exists():
        raise FileNotFoundError(f"Corrections file not found: {corrections_path}")

    df = load_file(path)

    # Apply declared types from column_type_registry so the write-back
    # uses confirmed types, not pandas inference from the CSV round-trip.
    from proto_pipe.io.db import get_registry_types
    from proto_pipe.pipelines.integrity import apply_declared_types

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

    # Strip annotation columns — they must not be written to the source table
    data_cols = [c for c in df.columns if c not in ("_flag_id", "_flag_reason")]
    df = df[data_cols]

    # Get the columns that exist in the target table
    table_cols = set(
        conn.execute(
            "SELECT column_name FROM information_schema.columns WHERE table_name = ?",
            [table_name],
        ).df()["column_name"].tolist()
    )

    # Only update columns that exist in both the file and the table
    update_cols = [c for c in df.columns if c in table_cols and c != primary_key]

    if not update_cols:
        raise ValueError(
            f"No updatable columns found. Corrections file columns: {list(df.columns)}, "
            f"table columns: {sorted(table_cols)}"
        )

    # Warn about keys in the corrections file that don't exist in the table
    existing_keys = set(
        conn.execute(f'SELECT DISTINCT "{primary_key}" FROM "{table_name}"')
        .df()[primary_key]
        .tolist()
    )
    correction_keys = set(df[primary_key].dropna().tolist())
    not_found_keys = correction_keys - existing_keys
    not_found = len(not_found_keys)
    if not_found_keys:
        print(
            f"  [warn] {not_found} key(s) in corrections file not found in "
            f"'{table_name}': {sorted(not_found_keys)[:5]}"
            + (" ..." if not_found > 5 else "")
        )

    # Batched UPDATE via staging table instead of row-by-row Python loop.
    # Register the corrections DataFrame as a temporary view, then issue a single
    # UPDATE ... FROM ... JOIN which DuckDB executes in one pass.
    conn.execute(
        "CREATE TEMP TABLE IF NOT EXISTS _corrections_staging AS SELECT * FROM df LIMIT 0"
    )
    conn.execute("DELETE FROM _corrections_staging")
    conn.execute("INSERT INTO _corrections_staging SELECT * FROM df")

    set_clause = ", ".join(
        [f'"{col}" = _corrections_staging."{col}"' for col in update_cols]
    )
    conn.execute(f"""
        UPDATE "{table_name}"
        SET {set_clause}
        FROM _corrections_staging
        WHERE "{table_name}"."{primary_key}" = _corrections_staging."{primary_key}"
    """)
    conn.execute("DROP TABLE IF EXISTS _corrections_staging")

    updated = len(df) - not_found

    # Clear resolved entries from both flag tables.
    # _flag_id values from export_flagged point at flagged_rows (ingest conflicts).
    # _flag_id values from export_validation_report point at validation_flags.
    # We try both — each DELETE is a no-op if the id doesn't exist in that table.
    flagged_cleared = 0
    validation_cleared = 0
    if flag_ids:
        placeholders = ", ".join(["?"] * len(flag_ids))

        flagged_cleared = conn.execute(
            f"SELECT count(*) FROM flagged_rows WHERE id IN ({placeholders})",
            flag_ids,
        ).fetchone()[0]
        if flagged_cleared:
            conn.execute(
                f"DELETE FROM flagged_rows WHERE id IN ({placeholders})",
                flag_ids,
            )

        try:
            validation_cleared = conn.execute(
                f"SELECT count(*) FROM validation_flags WHERE id IN ({placeholders})",
                flag_ids,
            ).fetchone()[0]
            if validation_cleared:
                conn.execute(
                    f"DELETE FROM validation_flags WHERE id IN ({placeholders})",
                    flag_ids,
                )
        except Exception as exc:
            # validation_flags table may not exist in older pipeline DBs —
            # skip silently rather than failing the whole correction.
            validation_cleared = 0

    return {
        "updated": updated,
        "flagged_cleared": flagged_cleared,
        "validation_cleared": validation_cleared,
        "not_found": not_found,
    }
