"""
Corrections module.
Handles the flagged-row correction workflow:

  1. export_flagged     — write flagged rows to a dated CSV for manual review.
  2. import_corrections — apply the corrected file back to the source table
                          and clear resolved flagged_rows entries.

Row identity:
  flagged_rows.id = md5(str(pk_value)), set at flag time in ingest.py.

  At export time, the source table's primary key column is hashed with the
  same expression — md5(CAST(pk_col AS VARCHAR)) — and joined directly to
  flagged_rows.id. This is:
  - Fast: DuckDB computes md5 in one vectorised pass, result is immediately
    joined to the small flagged_rows table.
  - Drift-free: derived from the data value, not row position.
  - No extra columns: nothing added to source tables.
"""

from pathlib import Path
from datetime import datetime, timezone
import duckdb
import pandas as pd


# ---------------------------------------------------------------------------
# Export flagged rows
# ---------------------------------------------------------------------------

def export_flagged(
    conn: duckdb.DuckDBPyConnection,
    table_name: str,
    output_path: str,
    primary_key: str,
) -> int:
    """Write flagged rows for a table to a CSV for manual correction.

    Joins flagged_rows to the source table via:
        flagged_rows.id = md5(CAST(source.primary_key AS VARCHAR))

    This is the same hash used when the flag was written, so the join
    is always consistent. No full table scan — DuckDB evaluates md5 in
    one vectorised pass then joins to the small flagged_rows result.

    The file includes all source columns plus:
        _flag_id      — keep this column when re-importing
        _flag_reason  — why the row was flagged, for reference only

    Returns the number of rows exported.
    Raises ValueError if no flagged rows exist for the table.
    """
    # Pull flagged ids for this table
    flagged = conn.execute("""
        SELECT id AS _flag_id, reason AS _flag_reason
        FROM flagged_rows
        WHERE table_name = ?
        ORDER BY flagged_at
    """, [table_name]).df()

    if flagged.empty:
        raise ValueError(f"No flagged rows found for table '{table_name}'")

    # Join source rows to flags via md5(pk_col) = flagged_rows.id
    source_df = conn.execute(f"""
        SELECT s.*, f.id AS _flag_id, f.reason AS _flag_reason
        FROM "{table_name}" s
        JOIN flagged_rows f
          ON md5(CAST(s."{primary_key}" AS VARCHAR)) = f.id
        WHERE f.table_name = ?
        ORDER BY f.flagged_at
    """, [table_name]).df()

    if source_df.empty:
        raise ValueError(
            f"No source rows matched flagged ids for table '{table_name}'. "
            f"Ensure primary_key='{primary_key}' is correct."
        )

    # Annotation columns first
    other_cols = [c for c in source_df.columns if c not in ("_flag_id", "_flag_reason")]
    source_df  = source_df[["_flag_id", "_flag_reason"] + other_cols]

    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    source_df.to_csv(output_path, index=False)

    return len(source_df)


def dated_export_path(output_dir: str, table_name: str) -> str:
    """Return the default export path: flagged_<table>_YYYY-MM-DD.csv"""
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    return str(Path(output_dir) / f"flagged_{table_name}_{today}.csv")


# ---------------------------------------------------------------------------
# Import corrections
# ---------------------------------------------------------------------------

def _load_corrections_file(path: Path) -> pd.DataFrame:
    """Load a corrections file from CSV or Excel."""
    suffix = path.suffix.lower()
    if suffix == ".csv":
        return pd.read_csv(path)
    elif suffix in (".xlsx", ".xls"):
        return pd.read_excel(path)
    else:
        raise ValueError(
            f"Unsupported file type '{path.suffix}'. "
            f"Corrections file must be .csv, .xlsx, or .xls."
        )


def import_corrections(
    conn: duckdb.DuckDBPyConnection,
    table_name: str,
    corrections_path: str,
    primary_key: str,
) -> dict:
    """Apply a corrected file back to the source table and clear resolved flags.

    Accepts .csv, .xlsx, or .xls. Uses a DuckDB staging table for a batched
    UPDATE. Warns when corrected keys are not found in the target table.

    Returns dict: updated (int), flagged_cleared (int), not_found (int).
    """
    path = Path(corrections_path)
    if not path.exists():
        raise FileNotFoundError(f"Corrections file not found: {corrections_path}")

    df = _load_corrections_file(path)

    if primary_key not in df.columns:
        raise ValueError(
            f"Primary key column '{primary_key}' not found in corrections file. "
            f"Columns present: {list(df.columns)}"
        )

    flag_ids  = df["_flag_id"].dropna().tolist() if "_flag_id" in df.columns else []
    data_cols = [c for c in df.columns if c not in ("_flag_id", "_flag_reason")]
    df        = df[data_cols]

    table_cols  = set(
        conn.execute(
            "SELECT column_name FROM information_schema.columns WHERE table_name = ?",
            [table_name],
        ).df()["column_name"].tolist()
    )
    update_cols = [c for c in df.columns if c in table_cols and c != primary_key]

    if not update_cols:
        raise ValueError(
            f"No updatable columns found. Corrections file columns: {list(df.columns)}, "
            f"table columns: {sorted(table_cols)}"
        )

    # Warn about keys not present in the table
    existing_keys   = set(
        conn.execute(f'SELECT DISTINCT "{primary_key}" FROM "{table_name}"')
        .df()[primary_key].tolist()
    )
    correction_keys = set(df[primary_key].dropna().tolist())
    not_found_keys  = correction_keys - existing_keys
    not_found       = len(not_found_keys)
    if not_found_keys:
        shown = sorted(str(k) for k in not_found_keys)[:5]
        print(
            f"  [warn] {not_found} key(s) not found in '{table_name}': {shown}"
            + (" ..." if not_found > 5 else "")
        )

    # Batched UPDATE via staging table
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

    flagged_cleared = 0
    if flag_ids:
        placeholders    = ", ".join(["?"] * len(flag_ids))
        flagged_cleared = conn.execute(
            f"SELECT count(*) FROM flagged_rows WHERE id IN ({placeholders})",
            flag_ids,
        ).fetchone()[0]
        conn.execute(
            f"DELETE FROM flagged_rows WHERE id IN ({placeholders})",
            flag_ids,
        )

    return {"updated": updated, "flagged_cleared": flagged_cleared, "not_found": not_found}
