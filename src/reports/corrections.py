"""Corrections module.
Handles the flagged-row correction workflow:

  1. export_flagged — write flagged rows for a table to a CSV so the user
                       can review and fix them outside the pipeline.
  2. import_corrections — read the corrected CSV back in, UPDATE the matching
                          rows in the table by primary key, and clear the
                          resolved entries from flagged_rows.

The primary key is defined per source in sources_config.yaml under `primary_key`.
It can be overridden at the CLI with --key.
"""

from pathlib import Path
from datetime import datetime, timezone
import duckdb
import pandas as pd  # type: ignore


# ---------------------------------------------------------------------------
# Export flagged rows
# ---------------------------------------------------------------------------

def export_flagged(
    conn: duckdb.DuckDBPyConnection,
    table_name: str,
    output_path: str,
) -> int:
    """Join flagged_rows metadata with the actual table rows and write to CSV.

    The exported file includes all columns from the source table plus two
    annotation columns:
        _flag_reason — the reason the row was flagged
        _flag_id — the flagged_rows.id (used internally on re-import)

    Returns the number of rows exported.
    Raises ValueError if no flagged rows exist for the table.
    """
    # Pull flagged metadata for this table
    flagged = conn.execute("""
        SELECT id AS _flag_id, row_index, reason AS _flag_reason
        FROM flagged_rows
        WHERE table_name = ?
        ORDER BY row_index
    """, [table_name]).df()

    if flagged.empty:
        raise ValueError(f"No flagged rows found for table '{table_name}'")

    # Pull the full source table
    source_df = conn.execute(f'SELECT * FROM "{table_name}"').df()

    if source_df.empty:
        raise ValueError(f"Table '{table_name}' is empty")

    # Join on row_index (DuckDB rowid-equivalent stored at flag time)
    source_df["_row_index"] = source_df.index
    merged = flagged.merge(
        source_df,
        left_on="row_index",
        right_on="_row_index",
        how="left",
    ).drop(columns=["_row_index", "row_index"])

    # Put annotation columns first so they're visible without scrolling
    cols = ["_flag_id", "_flag_reason"] + [
        c for c in merged.columns if c not in ("_flag_id", "_flag_reason")
    ]
    merged = merged[cols]

    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    merged.to_csv(output_path, index=False)

    return len(merged)


# ---------------------------------------------------------------------------
# Import corrections
# ---------------------------------------------------------------------------

def import_corrections(
    conn: duckdb.DuckDBPyConnection,
    table_name: str,
    corrections_path: str,
    primary_key: str,
) -> dict:
    """
    Read a corrected CSV and UPDATE matching rows in the table by primary key.
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

    df = pd.read_csv(path)

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

    updated = 0
    for _, row in df.iterrows():
        key_val = row[primary_key]
        set_clause = ", ".join([f'"{col}" = ?' for col in update_cols])
        values = [row[col] for col in update_cols] + [key_val]

        conn.execute(
            f'UPDATE "{table_name}" SET {set_clause} WHERE "{primary_key}" = ?',
            values,
        )
        updated += 1

    # Clear resolved flagged_rows entries
    flagged_cleared = 0
    if flag_ids:
        placeholders = ", ".join(["?"] * len(flag_ids))
        flagged_cleared = conn.execute(
            f"SELECT count(*) FROM flagged_rows WHERE id IN ({placeholders})",
            flag_ids,
        ).fetchone()[0]
        conn.execute(
            f"DELETE FROM flagged_rows WHERE id IN ({placeholders})",
            flag_ids,
        )

    return {"updated": updated, "flagged_cleared": flagged_cleared}
