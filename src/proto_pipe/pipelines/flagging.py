"""Shared flagging module — source and report layers.

Both the source layer (io/ingest.py) and the report layer
(reports/runner.py) share the same flagging pattern:

  - Computing row hashes for change detection
  - Writing blocked rows to source_block or validation_block
  - Comparing incoming hashes against source_pass or validation_pass
  - Building the enriched flag export view for vp flagged / vp validated

All direct DuckDB flag operations live here. Neither ingest.py nor
runner.py should contain raw SQL against these tables.

Flag table conventions:
  source_block / validation_block  — blocked/failed records
  source_pass / validation_pass    — accepted record state

_flag_* column prefix:
  _flag_reason, _flag_columns, _flag_check — guide columns added to
  exports for the user. Stripped on re-ingest via strip_pipeline_cols.
"""
from __future__ import annotations

import hashlib
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

import duckdb
import pandas as pd


# ---------------------------------------------------------------------------
# Row hashing
# ---------------------------------------------------------------------------

def compute_row_hash(row: dict | pd.Series, cols: list[str]) -> str:
    """Return md5 of pipe-delimited column values for the given columns.

    Produces the same hash as DuckDB's:
        md5(col1 || '|' || col2 || ...)
    when both sides use CAST(... AS VARCHAR) / str().

    NULL / None values are represented as the empty string so that a
    NULL and a missing key produce the same hash contribution.

    :param row:  A dict or pandas Series representing one record.
    :param cols: Ordered list of column names to include in the hash.
    :return:     32-character hex md5 string.
    """
    parts = []
    for col in cols:
        val = row.get(col) if isinstance(row, dict) else row.get(col)
        if val is None or (isinstance(val, float) and pd.isna(val)):
            parts.append("")
        else:
            parts.append(str(val))
    raw = "|".join(parts)
    return hashlib.md5(raw.encode()).hexdigest()


def compute_row_hash_sql(cols: list[str], alias: str | None = None) -> str:
    """Return a DuckDB SQL expression that computes the row hash.

    Produces the same hash as compute_row_hash() when column values are
    CAST to VARCHAR. Used inside classify queries in _handle_duplicates.

    :param cols:  Ordered list of column names.
    :param alias: Optional table alias prefix (e.g. 'e' → e."col").
    :return:      DuckDB SQL expression string.
    """
    def ref(col: str) -> str:
        if alias:
            return f"COALESCE(CAST({alias}.\"{col}\" AS VARCHAR), '')"
        return f"COALESCE(CAST(\"{col}\" AS VARCHAR), '')"

    parts = " || '|' || ".join(ref(c) for c in cols)
    return f"md5({parts})"


# ---------------------------------------------------------------------------
# Flag record dataclass
# ---------------------------------------------------------------------------

@dataclass
class FlagRecord:
    """One blocked row to be written to source_block or validation_block.

    Both tables share this structure. Callers set flag_table to control
    which table receives the write.

    Attributes:
        id:          md5(str(pk_value)) — deterministic, idempotent.
        table_name:  Source or report table name.
        check_name:  'type_conflict' | 'duplicate_conflict' | check function name.
        pk_value:    Raw primary key value as VARCHAR string.
        bad_columns: Pipe-delimited column names e.g. "amount|region".
        reason:      Human-readable description, max 500 chars.
        report_name: Report name — required for validation_block, None for source_block.
        source_file: Full path to originating file — source_block only.
    """
    id: str
    table_name: str
    check_name: str
    pk_value: str | None = None
    bad_columns: str | None = None
    reason: str | None = None
    report_name: str | None = None
    source_file: str | None = None
    flagged_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))


# ---------------------------------------------------------------------------
# Writing flags
# ---------------------------------------------------------------------------

def write_source_flags(
    conn: duckdb.DuckDBPyConnection,
    flags: list[FlagRecord],
) -> int:
    """Bulk INSERT FlagRecords into source_block. Returns count written.

    Uses ON CONFLICT DO NOTHING — idempotent. Running the same file
    twice won't create duplicate flags.
    """
    if not flags:
        return 0

    flags_df = pd.DataFrame([
        {
            "id":           f.id,
            "table_name":   f.table_name,
            "check_name":   f.check_name,
            "pk_value":     f.pk_value,
            "source_file":  f.source_file,
            "source_file_missing": False,
            "bad_columns":  f.bad_columns,
            "reason":       (f.reason or "")[:500],
            "flagged_at":   f.flagged_at,
        }
        for f in flags
    ])

    conn.execute("""
        INSERT INTO source_block
            (id, table_name, check_name, pk_value, source_file,
             source_file_missing, bad_columns, reason, flagged_at)
        SELECT id, table_name, check_name, pk_value, source_file,
               source_file_missing, bad_columns, reason, flagged_at
        FROM flags_df
        ON CONFLICT (id) DO NOTHING
    """)
    return len(flags)


def write_validation_flags(
    conn: duckdb.DuckDBPyConnection,
    flags: list[FlagRecord],
) -> int:
    """Bulk INSERT FlagRecords into validation_block. Returns count written.

    Uses ON CONFLICT DO NOTHING — idempotent.
    report_name must be set on every FlagRecord passed here.

    IDs are recomputed here from (report_name, check_name, pk_value) so that
    the same row failing the same check in the same report always produces the
    same id — regardless of what the caller passed. Falls back to uuid4 when
    pk_value is None.
    """
    if not flags:
        return 0

    import uuid

    def _vid(f: FlagRecord) -> str:
        if f.pk_value is None:
            return str(uuid.uuid4())
        key = f"{f.report_name}:{f.check_name}:{f.pk_value}"
        return hashlib.md5(key.encode()).hexdigest()

    flags_df = pd.DataFrame([
        {
            "id":           _vid(f),
            "table_name":   f.table_name,
            "report_name":  f.report_name,
            "check_name":   f.check_name,
            "pk_value":     f.pk_value,
            "bad_columns":  f.bad_columns,
            "reason":       (f.reason or "")[:500],
            "flagged_at":   f.flagged_at,
        }
        for f in flags
    ])

    conn.execute("""
        INSERT INTO validation_block
            (id, table_name, report_name, check_name, pk_value,
             bad_columns, reason, flagged_at)
        SELECT id, table_name, report_name, check_name, pk_value,
               bad_columns, reason, flagged_at
        FROM flags_df
        ON CONFLICT (id) DO NOTHING
    """)
    return len(flags)


# ---------------------------------------------------------------------------
# Hash comparison
# ---------------------------------------------------------------------------

@dataclass
class HashComparison:
    """Result of comparing incoming records against accepted state.

    Attributes:
        new:       pk_values not present in source_pass/validation_pass.
        changed:   pk_values present but with a different row_hash.
        identical: pk_values present with matching row_hash — skip.
    """
    new: set[str] = field(default_factory=set)
    changed: set[str] = field(default_factory=set)
    identical: set[str] = field(default_factory=set)


def compare_source_hashes(
    conn: duckdb.DuckDBPyConnection,
    table_name: str,
    incoming: dict[str, str],
) -> HashComparison:
    """Compare incoming {pk_value: row_hash} against source_pass.

    :param conn:       Open DuckDB connection.
    :param table_name: Source table name.
    :param incoming:   {pk_value: row_hash} for the chunk being processed.
    :return:           HashComparison with new / changed / identical sets.
    """
    if not incoming:
        return HashComparison()

    from proto_pipe.io.db import get_source_pass_hashes
    existing = get_source_pass_hashes(conn, table_name, list(incoming.keys()))

    result = HashComparison()
    for pk, incoming_hash in incoming.items():
        if pk not in existing:
            result.new.add(pk)
        elif existing[pk] == incoming_hash:
            result.identical.add(pk)
        else:
            result.changed.add(pk)

    return result


def compare_validation_hashes(
    conn: duckdb.DuckDBPyConnection,
    table_name: str,
    report_name: str,
    check_set_hash: str,
    incoming: dict[str, str],
) -> HashComparison:
    """Compare incoming {pk_value: row_hash} against validation_pass.

    Records are pending re-validation if:
    - Not in validation_pass at all (never validated)
    - Row hash differs (source record changed since last validation)
    - check_set_hash differs (handled separately — triggers full revalidation)

    :param conn:           Open DuckDB connection.
    :param table_name:     Source table name.
    :param report_name:    Report name.
    :param check_set_hash: Current check set hash.
    :param incoming:       {pk_value: row_hash} for all source records.
    :return:               HashComparison with new / changed / identical sets.
    """
    if not incoming:
        return HashComparison()

    from proto_pipe.io.db import get_validation_pass_hashes
    existing = get_validation_pass_hashes(
        conn, table_name, report_name, check_set_hash
    )

    result = HashComparison()
    for pk, incoming_hash in incoming.items():
        if pk not in existing:
            result.new.add(pk)
        elif existing[pk] == incoming_hash:
            result.identical.add(pk)
        else:
            result.changed.add(pk)

    return result


# ---------------------------------------------------------------------------
# Flag export — shared by vp flagged and vp validated
# ---------------------------------------------------------------------------

def build_source_flag_export(
    conn: duckdb.DuckDBPyConnection,
    table_name: str,
    pk_col: str,
) -> pd.DataFrame:
    """Build the enriched flag export for vp flagged edit / vp flagged open.

    Groups source_block entries by source_file, globs each file via DuckDB,
    concatenates with union_by_name=True, then INNER JOINs to source_block
    on pk_value. Adds _flag_* guide columns.

    If a source_file is missing, sets source_file_missing=True in source_block
    and excludes those rows from the join (they appear as metadata-only rows).

    :param conn:       Open DuckDB connection (pipeline DB).
    :param table_name: Source table name to filter source_block.
    :param pk_col:     Primary key column name in the source table.
    :return:           DataFrame with source columns + _flag_* guide columns,
                       ready for TUI display or CSV export.
    """
    flags_df = conn.execute("""
        SELECT id, pk_value, source_file, source_file_missing,
               bad_columns, reason, check_name
        FROM source_block
        WHERE table_name = ?
        ORDER BY flagged_at DESC
    """, [table_name]).df()

    if flags_df.empty:
        return flags_df

    # Detect missing source files and update source_block
    available_files = []
    for source_file in flags_df["source_file"].dropna().unique():
        if Path(source_file).exists():
            available_files.append(source_file)
        else:
            conn.execute("""
                UPDATE source_block
                SET source_file_missing = TRUE
                WHERE table_name = ? AND source_file = ?
            """, [table_name, source_file])

    if not available_files:
        # All source files missing — return metadata only
        return (
            flags_df
            .rename(columns={
                "reason":     "_flag_reason",
                "bad_columns": "_flag_columns",
                "check_name": "_flag_check",
                "id":         "_flag_id",
            })
        )

    # Glob available source files and concat via DuckDB
    path_list = ", ".join(f"'{p}'" for p in available_files)
    source_df = conn.execute(
        f"SELECT * FROM read_csv([{path_list}], union_by_name=true)"
    ).df()

    # Cast pk_col to VARCHAR for join
    source_df["_pk_str"] = source_df[pk_col].astype(str)
    flags_df["_pk_str"] = flags_df["pk_value"].astype(str)

    merged = flags_df.merge(source_df, on="_pk_str", how="inner")

    # Build clean output — source columns + guide columns
    source_cols = [c for c in source_df.columns if not c.startswith("_")]
    result = merged[source_cols].copy()
    result["_flag_id"] = merged["id"]
    result["_flag_reason"] = merged["reason"]
    result["_flag_columns"] = merged["bad_columns"]
    result["_flag_check"] = merged["check_name"]

    return result


def build_validation_flag_export(
    conn: duckdb.DuckDBPyConnection,
    table_name: str,
    report_name: str,
    pk_col: str,
) -> pd.DataFrame:
    """Build the enriched flag export for vp validated edit / vp validated open.

    INNER JOINs validation_block to the report table on pk_value.
    Adds _flag_* guide columns. No source file globbing needed —
    the report table has everything required.

    :param conn:        Open DuckDB connection (pipeline DB).
    :param table_name:  Report table name (e.g. 'van_report').
    :param report_name: Report name to filter validation_block.
    :param pk_col:      Primary key column name in the report table.
    :return:            DataFrame with report columns + _flag_* guide columns.
    """
    # Pull validation_block entries for this report
    flags_df = conn.execute("""
        SELECT id, pk_value, bad_columns, reason, check_name
        FROM validation_block
        WHERE table_name = ?
        AND report_name = ?
        ORDER BY flagged_at DESC
    """, [table_name, report_name]).df()

    if flags_df.empty:
        return flags_df

    # INNER JOIN to report table
    pk_values = flags_df["pk_value"].dropna().tolist()
    if not pk_values:
        return flags_df

    placeholders = ", ".join(["?"] * len(pk_values))
    report_df = conn.execute(
        f'SELECT * FROM "{table_name}" WHERE CAST("{pk_col}" AS VARCHAR) IN ({placeholders})',
        pk_values,
    ).df()

    if report_df.empty:
        return flags_df

    report_df["_pk_str"] = report_df[pk_col].astype(str)
    flags_df["_pk_str"] = flags_df["pk_value"].astype(str)

    merged = flags_df.merge(report_df, on="_pk_str", how="inner")

    source_cols = [c for c in report_df.columns if not c.startswith("_")]
    result = merged[source_cols].copy()
    result["_flag_id"] = merged["id"]
    result["_flag_reason"] = merged["reason"]
    result["_flag_columns"] = merged["bad_columns"]
    result["_flag_check"] = merged["check_name"]

    return result


# ---------------------------------------------------------------------------
# check_set_hash computation
# ---------------------------------------------------------------------------

def compute_check_set_hash(
    check_entries: list[dict],
    check_registry,
    alias_map: list[dict] | None = None,
) -> str:
    """Compute a stable hash of the current check set for a report.

    Hash covers: check names + function source code hashes + alias_map contents.
    If a check is added, removed, its function body changes, or any alias_map
    routing changes (column-backed ↔ constant), the hash changes and vp validate
    will notify the user and trigger revalidation.

    :param check_entries:   List of check config dicts from reports_config.yaml.
    :param check_registry:  CheckRegistry instance with loaded checks.
    :param alias_map:       Report alias_map list — included so routing changes
                            trigger revalidation. Pass None for legacy callers
                            (alias_map excluded from hash until runner is updated).
    :return:                32-character hex md5 string.
    """
    import inspect
    parts = []
    for entry in sorted(check_entries, key=lambda e: e.get("name", "")):
        name = entry.get("name", "")
        func = check_registry.get(name)
        if func is None:
            parts.append(f"{name}:missing")
            continue
        try:
            src_hash = hashlib.md5(
                inspect.getsource(func).encode()
            ).hexdigest()
        except (OSError, TypeError):
            src_hash = "unknown"
        parts.append(f"{name}:{src_hash}")

    # Include alias_map so any routing change (column-backed ↔ constant,
    # or column reassignment) triggers revalidation. Sorted for determinism.
    if alias_map:
        alias_str = "|".join(
            f"{e.get('param')}:{e.get('column')}"
            for e in sorted(alias_map, key=lambda e: (e.get("param", ""), e.get("column", "")))
        )
        parts.append(f"alias_map:{alias_str}")

    combined = "|".join(parts)
    return hashlib.md5(combined.encode()).hexdigest()


def get_raw_flags(
    conn: "duckdb.DuckDBPyConnection",
    flag_table: str,
    filters: dict[str, str] | None = None,
    limit: int | None = None,
) -> "pd.DataFrame":
    """Return raw flag rows with standard _flag_* column aliases.

    Works for both source_block and validation_block. Filters is a dict
    of {column_name: value} pairs used as AND-ed WHERE clauses.

    Renames: id → _flag_id, check_name → _flag_check,
             bad_columns → _flag_columns, reason → _flag_reason.
    """
    from proto_pipe.io.db import coerce_for_display

    query = f'SELECT * FROM "{flag_table}"'
    params: list = []
    clauses: list[str] = []
    for col, val in (filters or {}).items():
        clauses.append(f'"{col}" = ?')
        params.append(val)
    if clauses:
        query += " WHERE " + " AND ".join(clauses)
    query += " ORDER BY flagged_at DESC"
    if limit:
        query += f" LIMIT {limit}"

    df = conn.execute(query, params).df()
    rename = {
        "id": "_flag_id",
        "check_name": "_flag_check",
        "bad_columns": "_flag_columns",
        "reason": "_flag_reason",
    }
    df = df.rename(columns={k: v for k, v in rename.items() if k in df.columns})
    return coerce_for_display(df)


def clear_flags(
    conn: "duckdb.DuckDBPyConnection",
    flag_table: str,
    flag_ids: list[str] | set[str],
) -> int:
    """DELETE flags by id from the given flag table.

    Returns the count of rows actually deleted. Safe to call with an
    empty list (returns 0, no query executed).
    """
    flag_ids = list(flag_ids)
    if not flag_ids:
        return 0
    placeholders = ", ".join(["?"] * len(flag_ids))
    count = conn.execute(
        f'SELECT count(*) FROM "{flag_table}" WHERE id IN ({placeholders})',
        flag_ids,
    ).fetchone()[0]
    if count:
        conn.execute(
            f'DELETE FROM "{flag_table}" WHERE id IN ({placeholders})',
            flag_ids,
        )
    return count
