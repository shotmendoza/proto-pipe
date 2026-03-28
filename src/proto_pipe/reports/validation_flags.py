"""Validation flags module.

Handles writing and querying per-row validation flags produced during
`vp validate`. This is separate from `flagged_rows`, which tracks ingest
conflicts (rows that arrived with a changed primary key).

Separation rationale:
  - flagged_rows → ingest conflicts; a correction path is export → fix CSV →
                      import-corrections. Hard-blocks the deliverable.
  - validation_flags → check failures; correction path is fix at source →
                        re-ingest → re-validate. Warns but does not block.

Flag identity:
  id = uuid5(NAMESPACE_DNS, f"{report_name}:{check_name}:{pk_value}")

  Using uuid5 makes re-running validate idempotent — the same row failing
  the same check in the same report always produces the same id, so
  ON CONFLICT DO NOTHING deduplicates naturally.

  When pk_value is not available (no primary_key defined for the source),
  uuid4 is used as a fallback — flags are still written but cannot be
  deduplicated across runs.

Result dict conventions (checked by _extract_flagged_rows):

  {"mask": pd.Series[bool], "flag_when": True}
      Row-level. Rows where mask == flag_when are flagged.
      flag_when defaults to True if omitted.

  {"violation_indices": [0, 2, 5]}
      Row-level, backward-compatible. Rows at those DataFrame integer
      indices are flagged. Used by the built-in check_range today.

  Anything else (or if the check raises):
      Summary flag. One entry per check run, no row identifiers.
      reason = the exception message (raises) or a short dict repr (other).
"""

import uuid
from datetime import datetime, timezone
from typing import Any

import duckdb
import pandas as pd


# ---------------------------------------------------------------------------
# Table bootstrap
# ---------------------------------------------------------------------------

def init_validation_flags_table(conn: duckdb.DuckDBPyConnection) -> None:
    """Create the validation_flags table if it doesn't exist.

    Called by `vp db-init`. Safe to call multiple times.
    """
    conn.execute("""
        CREATE TABLE IF NOT EXISTS validation_flags (
            id           VARCHAR PRIMARY KEY,
            report_name  VARCHAR NOT NULL,
            check_name   VARCHAR NOT NULL,
            table_name   VARCHAR NOT NULL,
            pk_col       VARCHAR,
            pk_value     VARCHAR,
            reason       VARCHAR,
            flagged_at   TIMESTAMPTZ NOT NULL
        )
    """)


# ---------------------------------------------------------------------------
# Flag identity
# ---------------------------------------------------------------------------

def _flag_id(report_name: str, check_name: str, pk_value: str | None) -> str:
    """Return a deterministic flag id.

    Deterministic when pk_value is known → idempotent on re-run.
    Falls back to uuid4 when pk_value is None.
    """
    if pk_value is None:
        return str(uuid.uuid4())
    key = f"{report_name}:{check_name}:{pk_value}"
    return str(uuid.uuid5(uuid.NAMESPACE_DNS, key))


# ---------------------------------------------------------------------------
# Result interpretation
# ---------------------------------------------------------------------------

def _extract_flagged_rows(
    result: dict[str, Any],
    df: pd.DataFrame,
    pk_col: str | None,
    check_name: str,
    reason_prefix: str = "",
) -> list[dict]:
    """Inspect a check result dict and return a list of flag dicts.

    Each flag dict has keys: pk_value, reason.
    pk_value is None when the source has no primary_key defined.

    Priority:
      1. "mask" key  → pd.Series[bool], rows where mask == flag_when are flagged
      2. "violation_indices" key → list[int], backward-compat with check_range
      3. Anything else → one summary flag, no pk_value
    """
    flags: list[dict] = []

    # --- Mode 1: boolean mask ---
    if "mask" in result:
        mask = result["mask"]
        if not isinstance(mask, pd.Series):
            # Malformed — fall through to summary
            flags.append({"pk_value": None, "reason": f"'mask' is not a pd.Series: {type(mask)}"})
            return flags

        flag_when = result.get("flag_when", True)
        failing = df[mask == flag_when]

        for _, row in failing.iterrows():
            pk_value = str(row[pk_col]) if pk_col and pk_col in row.index else None
            reason = reason_prefix or f"{check_name}: row failed mask check (flag_when={flag_when})"
            flags.append({"pk_value": pk_value, "reason": reason})
        return flags

    # --- Mode 2: violation_indices (backward compat) ---
    if "violation_indices" in result:
        indices = result["violation_indices"]
        if not indices:
            return []

        failing = df.loc[df.index.isin(indices)]
        col = result.get("col", "")
        min_val = result.get("min_val")
        max_val = result.get("max_val")

        for _, row in failing.iterrows():
            pk_value = str(row[pk_col]) if pk_col and pk_col in row.index else None
            if col and min_val is not None and max_val is not None:
                actual = row.get(col, "?")
                reason = f"{col} out of range [{min_val}, {max_val}]: {actual}"
            else:
                reason = reason_prefix or f"value out of range"
            flags.append({"pk_value": pk_value, "reason": reason})
        return flags

    # --- Mode 3: summary ---
    summary = reason_prefix or _summarise_result(result)
    flags.append({"pk_value": None, "reason": summary})
    return flags


def _summarise_result(result: dict) -> str:
    """Build a short human-readable summary from a free-form result dict."""
    parts = []
    # Highlight the most useful keys if present
    if "has_nulls" in result:
        null_counts = result.get("null_counts", {})
        affected = {k: v for k, v in null_counts.items() if v > 0}
        parts.append(f"null values in: {list(affected.keys())}")
    elif "has_duplicates" in result:
        parts.append(f"{result.get('duplicate_count', '?')} duplicate row(s)")
    elif "missing_cols" in result:
        parts.append(f"missing columns: {result.get('missing_cols')}")
    else:
        # Generic: show up to 3 key-value pairs
        items = list(result.items())[:3]
        parts.append(", ".join(f"{k}={v}" for k, v in items))
    return "; ".join(parts) or str(result)


# ---------------------------------------------------------------------------
# Writing flags
# ---------------------------------------------------------------------------

def write_validation_flags(
    conn: duckdb.DuckDBPyConnection,
    report_name: str,
    check_name: str,
    table_name: str,
    pk_col: str | None,
    flag_rows: list[dict],
) -> int:
    """Insert flag rows into validation_flags. Returns count inserted.

    Uses ON CONFLICT DO NOTHING so re-running validate is safe.
    Batches all flags for one check into a single INSERT.
    """
    if not flag_rows:
        return 0

    now = datetime.now(timezone.utc)
    records = []
    for f in flag_rows:
        pk_value = f.get("pk_value")
        records.append({
            "id":          _flag_id(report_name, check_name, pk_value),
            "report_name": report_name,
            "check_name":  check_name,
            "table_name":  table_name,
            "pk_col":      pk_col,
            "pk_value":    pk_value,
            "reason":      (f.get("reason") or "")[:500],
            "flagged_at":  now,
        })

    flags_df = pd.DataFrame(records)
    conn.execute("""
        INSERT INTO validation_flags
            (id, report_name, check_name, table_name, pk_col, pk_value, reason, flagged_at)
        SELECT id, report_name, check_name, table_name, pk_col, pk_value, reason, flagged_at
        FROM flags_df
        ON CONFLICT (id) DO NOTHING
    """)
    return len(records)


# ---------------------------------------------------------------------------
# Querying flags
# ---------------------------------------------------------------------------

def count_validation_flags(
    conn: duckdb.DuckDBPyConnection,
    report_name: str | None = None,
) -> int:
    """Return the total number of open validation flags, optionally scoped to one report."""
    if report_name:
        return conn.execute(
            "SELECT count(*) FROM validation_flags WHERE report_name = ?",
            [report_name],
        ).fetchone()[0]
    return conn.execute("SELECT count(*) FROM validation_flags").fetchone()[0]


def summary_df(
    conn: duckdb.DuckDBPyConnection,
    report_name: str | None = None,
) -> pd.DataFrame:
    """Return a summary DataFrame: one row per (report, check), with failure count."""
    query = """
        SELECT
            report_name,
            check_name,
            table_name,
            count(*)                        AS flagged_count,
            count(pk_value)                 AS row_level_count,
            min(flagged_at)                 AS first_flagged,
            max(flagged_at)                 AS last_flagged
        FROM validation_flags
        {where}
        GROUP BY report_name, check_name, table_name
        ORDER BY report_name, flagged_count DESC
    """
    where = "WHERE report_name = ?" if report_name else ""
    params = [report_name] if report_name else []
    return conn.execute(query.format(where=where), params).df()


def detail_df(
    conn: duckdb.DuckDBPyConnection,
    report_name: str | None = None,
) -> pd.DataFrame:
    """Return a detail DataFrame: one row per flagged row."""
    query = """
        SELECT
            id AS _flag_id,
            report_name,
            check_name,
            table_name,
            pk_col,
            pk_value,
            reason,
            flagged_at
        FROM validation_flags
        {where}
        ORDER BY report_name, check_name, flagged_at
    """
    where = "WHERE report_name = ?" if report_name else ""
    params = [report_name] if report_name else []
    return conn.execute(query.format(where=where), params).df()


def clear_validation_flags(
    conn: duckdb.DuckDBPyConnection,
    report_name: str | None = None,
    check_name: str | None = None,
) -> int:
    """Delete validation flags. Returns count deleted."""
    query = "DELETE FROM validation_flags WHERE 1=1"
    params = []
    if report_name:
        query += " AND report_name = ?"
        params.append(report_name)
    if check_name:
        query += " AND check_name = ?"
        params.append(check_name)

    before = conn.execute(
        f"SELECT count(*) FROM validation_flags WHERE 1=1"
        + (" AND report_name = ?" if report_name else "")
        + (" AND check_name = ?" if check_name else ""),
        params,
    ).fetchone()[0]
    conn.execute(query, params)
    return before


# ---------------------------------------------------------------------------
# Export (two-sheet Excel)
# ---------------------------------------------------------------------------

def export_validation_report(
    conn: duckdb.DuckDBPyConnection,
    output_path: str,
    report_name: str | None = None,
) -> tuple[int, int]:
    """Write a two-sheet Excel file with Detail and Summary tabs.

    Sheet "Detail" — one row per flagged row; pk column named after actual
                      pk_col value so the user sees their own terminology.
    Sheet "Summary" — one row per (report, check) with counts.

    Returns (detail_row_count, summary_row_count).
    Raises ValueError if no flags exist for the given scope.
    """
    from pathlib import Path
    import pandas as pd

    det = detail_df(conn, report_name)
    summ = summary_df(conn, report_name)

    if det.empty:
        scope = f"report '{report_name}'" if report_name else "any report"
        raise ValueError(f"No validation flags found for {scope}")

    # Rename pk_col/pk_value columns to use the actual column name.
    # If all flags share the same pk_col, rename the column in the export.
    # If mixed (multiple sources), keep generic names.
    pk_cols_present = det["pk_col"].dropna().unique()
    if len(pk_cols_present) == 1:
        pk_col_name = pk_cols_present[0]
        det = det.drop(columns=["pk_col"]).rename(columns={"pk_value": pk_col_name})
    else:
        det = det.rename(columns={"pk_value": "record_id"}).drop(columns=["pk_col"])

    # _flag_id must be the first column — same pattern as export_flagged — so that
    # import_corrections can use it to clear resolved flags from validation_flags.
    # Users should leave this column untouched when fixing values.
    cols = ["_flag_id"] + [c for c in det.columns if c != "_flag_id"]
    det = det[cols]

    # Excel does not support timezone-aware datetimes. Strip tz from any
    # datetime columns in both frames before writing.
    def _strip_tz(df: pd.DataFrame) -> pd.DataFrame:
        for col in df.select_dtypes(include=["datetimetz"]).columns:
            df[col] = df[col].dt.tz_localize(None)
        return df

    det = _strip_tz(det)
    summ = _strip_tz(summ)

    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
        det.to_excel(writer, sheet_name="Detail", index=False)
        summ.to_excel(writer, sheet_name="Summary", index=False)

    return len(det), len(summ)
