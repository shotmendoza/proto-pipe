"""Ingestion module.

Scans a directory for CSV and Excel files, matches each file to a source
definition by pattern, and loads the data into DuckDB tables.

Data layer invariants (see Spec):
- ALL data loading goes through DuckDB. Pandas is never used in the data path.
- CSV files loaded via DuckDB read_csv() with dtype from column_type_registry.
- Excel files loaded via DuckDB read_xlsx() (spatial extension).
- load_file() is retained only for the correction workflow (strip_pipeline_cols path).
- Unknown columns (not in column_type_registry) fail the file.
- Bad row values (TRY_CAST fails) are written to source_block and removed from the
  insert set. Clean rows proceed.
- Duplicate handling compares against source_pass, not the source table directly.

Table lifecycle:
- vp new source  — confirms column types, writes column_type_registry
- vp db-init     — creates pipeline tables including source_pass and source_block
- First ingest   — CREATE TABLE AS SELECT, writes source_pass for all rows
- Subsequent     — auto_migrate new columns, _handle_duplicates via source_pass

Duplicate row handling (on_duplicate in sources_config.yaml):
- flag   — compare hash against source_pass; changed rows → source_block (default)
- append — insert all rows, no conflict checking, no source_pass update
- upsert — overwrite existing; update source_pass only when hash changed
- skip   — ignore rows already in source_pass

Each ingest run logs results to ingest_state (was: ingest_log).
"""
import csv
import warnings
from datetime import datetime, timezone
from fnmatch import fnmatch
from pathlib import Path
from typing import Literal, Callable

import duckdb
import pandas as pd  # type: ignore

from proto_pipe.constants import NUMERIC_DUCKDB_TYPES
from proto_pipe.io.db import (
    get_columns as get_existing_columns,
    get_column_types as get_existing_column_types,
    table_exists,
    flag_id_for,
    get_registry_types,
    log_ingest_state,
    get_source_pass_hashes,
    bulk_upsert_source_pass,
    clear_source_block_for_pks,
)
from proto_pipe.io.migration import auto_migrate
from proto_pipe.pipelines.flagging import (
    FlagRecord,
    write_source_flags,
    compute_row_hash_sql,
)


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_SUPPORTED_FILE_SUFFIXES = {".csv", ".xlsx", ".xls"}
_MAX_DIFF_COLS = 5
CHUNK_SIZE = 1000


# ---------------------------------------------------------------------------
# Metadata bootstrap
# ---------------------------------------------------------------------------

def _populate_builtin_metadata(conn: duckdb.DuckDBPyConnection) -> None:
    """Write metadata for all built-in checks. Safe to call multiple times."""
    from proto_pipe.checks.built_in import BUILT_IN_CHECKS
    from proto_pipe.checks.registry import CheckParamInspector

    for name, func in BUILT_IN_CHECKS.items():
        inspector = CheckParamInspector(func)
        inspector.write_to_db(conn, name)


# ---------------------------------------------------------------------------
# File support
# ---------------------------------------------------------------------------

def _is_supported_file(path: Path) -> bool:
    """Return True if the file has a supported extension."""
    return path.suffix.lower() in _SUPPORTED_FILE_SUFFIXES


# ---------------------------------------------------------------------------
# Pattern matching
# ---------------------------------------------------------------------------

def _pattern_specificity(pattern: str) -> int:
    """Return the number of literal (non-wildcard) characters in a pattern.

    Used to rank competing matches — more literal characters = more specific.
    Example: "foo you*.csv" → 11 beats "foo*.csv" → 7.
    """
    return sum(1 for c in pattern if c not in ("*", "?", "[", "]"))


def resolve_source(
    filename: str,
    sources: list[dict],
) -> dict | None:
    """Return the source whose pattern most specifically matches the filename.

    Picks the source whose best-matching pattern has the highest specificity
    score (most literal characters). "foo you*.csv" always beats "foo*.csv"
    for "foo you bar.csv", regardless of config order.

    Ties fall back to config order with a warning.
    """
    matches: list[tuple[dict, int]] = []
    for source in sources:
        best = max(
            (_pattern_specificity(p) for p in source["patterns"] if fnmatch(filename, p)),
            default=None,
        )
        if best is not None:
            matches.append((source, best))

    if not matches:
        return None

    best_score = max(score for _, score in matches)
    winners = [s for s, score in matches if score == best_score]

    if len(winners) > 1:
        names = ", ".join(f"'{w['name']}'" for w in winners)
        print(
            f"[warn] '{filename}' matches multiple sources with equal specificity: "
            f"{names}. Using '{winners[0]['name']}' — make patterns more specific "
            f"to resolve this ambiguity."
        )

    return winners[0]


# ---------------------------------------------------------------------------
# load_file — correction path only
# ---------------------------------------------------------------------------

def load_file(path: Path) -> pd.DataFrame:
    """Load a CSV or Excel file into a DataFrame.

    Used ONLY for the correction workflow (strip_pipeline_cols path) where
    pipeline metadata columns need to be stripped before re-ingest.
    Not used in the primary data loading path — all ingestion goes through DuckDB.

    :param path: Path to the file.
    :return: DataFrame. Types are re-inferred by DuckDB during subsequent processing.
    :raises ValueError: If the file type is unsupported.
    """
    suffix = path.suffix.lower()

    if suffix == ".csv":
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", pd.errors.DtypeWarning)
            return pd.read_csv(path)

    if suffix in {".xlsx", ".xls"}:
        return pd.read_excel(path)

    raise ValueError(f"Unsupported file type: {path.suffix}")


# ---------------------------------------------------------------------------
# DB initialisation
# ---------------------------------------------------------------------------

def init_db(db_path: str) -> None:
    """Initialise the pipeline database and bootstrap all pipeline tables."""
    from proto_pipe.io.db import init_all_pipeline_tables

    Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    with duckdb.connect(db_path) as conn:
        init_all_pipeline_tables(conn)
        _populate_builtin_metadata(conn)


def init_source_tables(db_path: str, sources: list[dict]) -> None:
    """Create placeholder tables for user-defined sources."""
    with duckdb.connect(db_path) as conn:
        for source in sources:
            table = source["target_table"]
            if table_exists(conn, table):
                print(f"[skip] Table '{table}' already exists")
                continue
            conn.execute(f'CREATE TABLE IF NOT EXISTS "{table}" (_placeholder VARCHAR)')
            print(f"[ok] Created table '{table}'")


# ---------------------------------------------------------------------------
# Ingest directory
# ---------------------------------------------------------------------------

def ingest_directory(
    directory: str,
    sources: list[dict],
    db_path: str,
    mode: str = "append",
    run_checks: bool = False,
    check_registry=None,
    report_registry=None,
    on_file_start: "Callable[[str], None] | None" = None,
    on_file_done: "Callable[[str, dict], None] | None" = None,
) -> dict:
    """Scan a directory, match files to source definitions, load into DuckDB.

    :param on_file_start: Optional callback called with the filename before
        each file is processed. Used by the CLI progress display.
    :param on_file_done:  Optional callback called with (filename, result)
        after each file completes. Used by the CLI progress display.
    """
    from proto_pipe.io.db import (
        init_ingest_state as _init_ingest_state,
        already_ingested as _already_ingested,
    )

    directory_path = Path(directory)
    if not directory_path.exists():
        raise ValueError(
            f"Incoming directory not found: '{directory}'\n"
            f"Check the incoming_dir setting in pipeline.yaml or pass --incoming-dir."
        )
    if not directory_path.is_dir():
        raise ValueError(f"incoming_dir '{directory}' exists but is not a directory.")

    conn = duckdb.connect(db_path)
    _init_ingest_state(conn)

    # Ensure all pipeline tables (source_pass, source_block, validation_block, …)
    # exist regardless of whether the caller ran init_db first.
    from proto_pipe.io.db import init_all_pipeline_tables as _init_all
    _init_all(conn)

    summary: dict[str, dict] = {}
    unmatched: list[str] = []

    for path in sorted(Path(directory).iterdir()):
        if not _is_supported_file(path):
            continue

        source = resolve_source(path.name, sources)
        if source is None:
            unmatched.append(path.name)
            continue

        if mode != "replace" and _already_ingested(conn, path.name):
            print(f"[skipped] '{path.name}' — already ingested")
            summary[path.name] = {"status": "skipped", "message": "already ingested"}
            continue

        if on_file_start:
            on_file_start(path.name)

        result = ingest_single_file(conn, path, source, mode=mode)
        summary[path.name] = result

        if on_file_done:
            on_file_done(path.name, result)

        if (
            run_checks
            and check_registry
            and report_registry
            and result.get("status") == "ok"
        ):
            _run_inline_checks(
                conn,
                source["target_table"],
                source,
                check_registry,
                report_registry,
                path.name,
            )

    if unmatched:
        print(f"[warn] No source match for: {', '.join(unmatched)}")
        for filename in unmatched:
            log_ingest_state(
                conn, filename, None, "skipped", message="No matching source pattern"
            )

    conn.close()
    return summary


# ---------------------------------------------------------------------------
# Inline checks (post-ingest)
# ---------------------------------------------------------------------------

def _run_inline_checks(
    conn,
    table,
    source,
    check_registry,
    report_registry,
    filename,
) -> None:
    """Run registered checks against a table immediately after ingest."""
    matching = [
        r for r in report_registry.all()
        if r.get("source", {}).get("table") == table
    ]
    if not matching:
        return

    df = conn.execute(f'SELECT * FROM "{table}"').df()
    context = {"df": df}

    for report in matching:
        for check_name in report.get("resolved_checks", []):
            try:
                result = check_registry.run(check_name, context)
                print(f"[check] {check_name}: {result}")
            except Exception as e:
                print(f"[check-fail] {check_name}: {e}")


# ---------------------------------------------------------------------------
# _handle_duplicates — compares against source_pass, not source table
# ---------------------------------------------------------------------------

def _handle_duplicates(
    conn: duckdb.DuckDBPyConnection,
    table: str,
    df: pd.DataFrame,
    primary_key: str,
    on_duplicate: Literal["append", "upsert", "skip", "flag"] = "flag",
    source_file: str = "",
) -> tuple[pd.DataFrame, int, int]:
    """Process incoming rows against source_pass by primary key.

    Compares incoming row hashes against source_pass (not the source table)
    to avoid phantom type-mismatch conflicts. Writes conflicts to source_block.
    Updates source_pass for accepted rows.

    Returns (rows_to_insert, blocked_count, skipped_count).

    on_duplicate modes:
        append — insert everything, no checking, no source_pass update
        upsert — overwrite existing; update source_pass when hash changed
        skip   — ignore rows already in source_pass
        flag   — identical hash → skip; changed hash → source_block; new → insert
    """
    if on_duplicate == "append":
        return df, 0, 0

    if primary_key not in df.columns:
        print(
            f"[warn] primary_key '{primary_key}' not in incoming columns "
            f"— appending all rows"
        )
        return df, 0, 0

    non_null_df = df[df[primary_key].notna()].copy()
    if non_null_df.empty:
        return df, 0, 0

    comparable_cols = [
        c for c in non_null_df.columns
        if c != primary_key
        and c in get_existing_columns(conn, table)
        and not c.startswith("_")
    ]

    rows_to_insert: list[tuple] = []
    pass_records: list[dict] = []
    total_blocked = 0
    total_skipped = 0

    for chunk_start in range(0, len(non_null_df), CHUNK_SIZE):
        chunk = non_null_df.iloc[chunk_start: chunk_start + CHUNK_SIZE]
        chunk_pk_strs = [str(pk) for pk in chunk[primary_key].tolist()]

        # Compare against source_pass — not the source table
        existing_hashes = get_source_pass_hashes(conn, table, chunk_pk_strs)

        # ── upsert ────────────────────────────────────────────────────────
        if on_duplicate == "upsert":
            placeholders = ", ".join(["?"] * len(chunk[primary_key].tolist()))
            conn.execute(
                f'DELETE FROM "{table}" WHERE "{primary_key}" IN ({placeholders})',
                chunk[primary_key].tolist(),
            )
            rows_to_insert.extend(chunk.itertuples(index=False, name=None))
            # Compute all hashes in one DuckDB query — replaces per-row Python calls.
            hash_expr = (
                compute_row_hash_sql(comparable_cols)
                if comparable_cols else "md5('')"
            )
            chunk_hashes = conn.execute(f"""
                SELECT CAST("{primary_key}" AS VARCHAR) as pk_value,
                       {hash_expr} as row_hash
                FROM chunk
            """).df()
            for _, hr in chunk_hashes.iterrows():
                if existing_hashes.get(hr["pk_value"]) != hr["row_hash"]:
                    pass_records.append({
                        "pk_value": hr["pk_value"],
                        "row_hash": hr["row_hash"],
                        "source_file": source_file,
                    })
            continue

        # ── skip ──────────────────────────────────────────────────────────
        if on_duplicate == "skip":
            new_rows = chunk[~chunk[primary_key].astype(str).isin(existing_hashes.keys())]
            skipped = len(chunk) - len(new_rows)
            if skipped:
                print(f"[skip] {skipped} row(s) already in source_pass — skipped")
            rows_to_insert.extend(new_rows.itertuples(index=False, name=None))
            total_skipped += skipped
            if not new_rows.empty:
                hash_expr = (
                    compute_row_hash_sql(comparable_cols)
                    if comparable_cols else "md5('')"
                )
                new_row_hashes = conn.execute(f"""
                    SELECT CAST("{primary_key}" AS VARCHAR) as pk_value,
                           {hash_expr} as row_hash
                    FROM new_rows
                """).df()
                for _, hr in new_row_hashes.iterrows():
                    pass_records.append({
                        "pk_value": hr["pk_value"],
                        "row_hash": hr["row_hash"],
                        "source_file": source_file,
                    })
            continue

        # ── flag ──────────────────────────────────────────────────────────
        if on_duplicate == "flag":
            if not comparable_cols:
                rows_to_insert.extend(chunk.itertuples(index=False, name=None))
                continue

            # Compute all hashes in one DuckDB query — replaces per-row Python calls.
            hash_expr = compute_row_hash_sql(comparable_cols)
            chunk_hashes = conn.execute(f"""
                SELECT CAST("{primary_key}" AS VARCHAR) as pk_value,
                       {hash_expr} as row_hash
                FROM chunk
            """).df()
            pk_to_hash = dict(zip(chunk_hashes["pk_value"], chunk_hashes["row_hash"]))

            conflict_pks: list[str] = []
            for _, row in chunk.iterrows():
                pk_str = str(row[primary_key])
                row_hash = pk_to_hash.get(pk_str, "")

                if pk_str not in existing_hashes:
                    # New row — insert and track in source_pass
                    rows_to_insert.append(tuple(row))
                    pass_records.append({
                        "pk_value": pk_str,
                        "row_hash": row_hash,
                        "source_file": source_file,
                    })
                elif existing_hashes[pk_str] == row_hash:
                    # Identical — skip silently
                    pass
                else:
                    # Changed — block and flag
                    conflict_pks.append(pk_str)

            if conflict_pks:
                # Compute diff strings for conflicting rows via DuckDB
                # (queries source table for current values — only for display purposes)
                col_diff_exprs = [
                    f"CASE"
                    f"  WHEN COALESCE(CAST(e.\"{c}\" AS VARCHAR), '__NULL__')"
                    f"    != COALESCE(CAST(c.\"{c}\" AS VARCHAR), '__NULL__')"
                    f"  THEN '{c}: '"
                    f"    || COALESCE(CAST(e.\"{c}\" AS VARCHAR), 'NULL')"
                    f"    || ' -> '"
                    f"    || COALESCE(CAST(c.\"{c}\" AS VARCHAR), 'NULL')"
                    f"  END"
                    for c in comparable_cols
                ]
                reason_expr = (
                    "array_to_string(list_filter(["
                    + ", ".join(col_diff_exprs)
                    + "], x -> x IS NOT NULL), ' | ')"
                )

                conflict_chunk = chunk[chunk[primary_key].astype(str).isin(conflict_pks)]
                placeholders = ", ".join(["?"] * len(conflict_pks))

                diff_sql = f"""
                    SELECT
                        CAST(c."{primary_key}" AS VARCHAR) AS pk_str,
                        {reason_expr} AS reason,
                        array_to_string(list_filter([
                            {", ".join(
                                f"CASE WHEN COALESCE(CAST(e.\"{c}\" AS VARCHAR), '__NULL__') "
                                f"!= COALESCE(CAST(c.\"{c}\" AS VARCHAR), '__NULL__') "
                                f"THEN '{c}' END"
                                for c in comparable_cols
                            )}
                        ], x -> x IS NOT NULL), '|') AS bad_cols
                    FROM conflict_chunk c
                    LEFT JOIN (
                        SELECT DISTINCT ON ("{primary_key}") *
                        FROM "{table}"
                        WHERE CAST("{primary_key}" AS VARCHAR) IN ({placeholders})
                    ) e ON CAST(c."{primary_key}" AS VARCHAR)
                          = CAST(e."{primary_key}" AS VARCHAR)
                    WHERE CAST(c."{primary_key}" AS VARCHAR) IN ({placeholders})
                """
                try:
                    diffs_df = conn.execute(diff_sql, conflict_pks + conflict_pks).df()
                except Exception:
                    # Fallback: plain reason without diff details
                    diffs_df = pd.DataFrame({
                        "pk_str": conflict_pks,
                        "reason": ["values changed"] * len(conflict_pks),
                        "bad_cols": [None] * len(conflict_pks),
                    })

                flag_records = []
                for _, diff_row in diffs_df.iterrows():
                    pk_str = str(diff_row["pk_str"])
                    reason = str(diff_row.get("reason", ""))[:500]
                    bad_cols = str(diff_row.get("bad_cols", "")) or None
                    flag_records.append(FlagRecord(
                        id=flag_id_for(pk_str),
                        table_name=table,
                        check_name="duplicate_conflict",
                        pk_value=pk_str,
                        source_file=source_file,
                        bad_columns=bad_cols,
                        reason=reason,
                    ))
                    print(
                        f"  [blocked] key={pk_str} — "
                        f"{reason[:80]}{'...' if len(reason) > 80 else ''}"
                    )

                write_source_flags(conn, flag_records)
                total_blocked += len(flag_records)

            continue

        # Unknown mode
        print(f"[warn] Unknown on_duplicate '{on_duplicate}' — appending chunk")
        rows_to_insert.extend(chunk.itertuples(index=False, name=None))

    if total_blocked:
        print(f"[blocked] {total_blocked} conflict(s) → source_block (original rows kept)")

    # Bulk update source_pass for all accepted rows
    if pass_records:
        bulk_upsert_source_pass(conn, table, pass_records)
        # Invariant: a row accepted into source_pass must not remain in
        # source_block. Clear any stale conflict entries for these pks.
        accepted_pks = [r["pk_value"] for r in pass_records]
        clear_source_block_for_pks(conn, table, accepted_pks)

    result_df = (
        pd.DataFrame(rows_to_insert, columns=df.columns)
        if rows_to_insert
        else pd.DataFrame(columns=df.columns)
    )
    return result_df, total_blocked, total_skipped


# ---------------------------------------------------------------------------
# _load_csv_with_types — DuckDB-native typed CSV loading
# ---------------------------------------------------------------------------

def _load_csv_with_types(
    conn: duckdb.DuckDBPyConnection,
    path: Path,
    registry_types: dict[str, str],
) -> pd.DataFrame:
    """Load a CSV via DuckDB with declared column types applied.

    When registry_types is non-empty, loads with all_varchar=True so DuckDB
    never hard-fails on bad values (e.g. "N/A" in a BIGINT column).
    _validate_row_types then does row-level TRY_CAST and sends bad rows to
    source_block. When registry_types is empty, falls back to DuckDB type
    inference.

    :param conn:           Open DuckDB connection.
    :param path:           Path to the CSV file.
    :param registry_types: {column: declared_type} — may include format suffix.
    :return: DataFrame typed per declared types.
    """
    from proto_pipe.io.migration import apply_declared_types

    with open(path, newline="") as _f:
        csv_columns = set(next(csv.reader(_f)))

    date_format_cols: dict[str, str] = {
        col: t for col, t in registry_types.items()
        if col in csv_columns and "|" in t
    }

    if registry_types:
        # Load as all-varchar so DuckDB never hard-fails on bad values.
        # apply_declared_types + row filtering happen in ingest_single_file
        # AFTER _validate_row_types has blocked bad rows.
        df = conn.read_csv(str(path), all_varchar=True).df()
    else:
        # No declared types — let DuckDB infer column types naturally.
        df = conn.read_csv(str(path)).df()

    return df

def _load_excel_with_types(
    conn: duckdb.DuckDBPyConnection,
    path: Path,
    registry_types: dict[str, str],
) -> pd.DataFrame:
    """Load an Excel file via DuckDB spatial extension with declared types.

    Requires: INSTALL spatial; LOAD spatial (run once at db-init).
    Falls back to pandas read_excel + apply_declared_types if spatial
    extension is not available.

    :param conn:           Open DuckDB connection.
    :param path:           Path to the Excel file.
    :param registry_types: {column: declared_type} — may include format suffix.
    :return: DataFrame typed per declared types.
    """
    from proto_pipe.io.migration import apply_declared_types

    try:
        conn.execute("LOAD spatial")
        df = conn.execute(f"SELECT * FROM read_xlsx('{path}')").df()
    except Exception:
        # Spatial extension not available — fall back to pandas
        import warnings
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            df = pd.read_excel(path)

    # Apply declared types for date format columns
    date_format_cols = {col: dt for col, dt in registry_types.items() if "|" in dt and col in df.columns}
    plain_types = {col: dt for col, dt in registry_types.items() if "|" not in dt and col in df.columns}

    if plain_types or date_format_cols:
        all_types = {**plain_types, **date_format_cols}
        df = apply_declared_types(df, all_types)

    return df


# ---------------------------------------------------------------------------
# Row-level TRY_CAST validation
# ---------------------------------------------------------------------------

def _validate_row_types(
    conn: duckdb.DuckDBPyConnection,
    df: pd.DataFrame,
    registry_types: dict[str, str],
    table_name: str,
    primary_key: str | None,
    source_file: str,
) -> tuple[pd.DataFrame, int]:
    """Find rows where any column fails TRY_CAST to its declared type.

    Writes blocked rows to source_block via write_source_flags.
    Returns (clean_df, blocked_count).

    Runs entirely in DuckDB — no pandas type inference.
    """
    if not registry_types:
        return df, 0

    blocked_pks: dict[str, list[str]] = {}  # pk_str → list[reason strings]
    blocked_cols: dict[str, list[str]] = {}  # pk_str → list[col names]

    pk_select = (
        f'CAST("{primary_key}" AS VARCHAR)'
        if primary_key and primary_key in df.columns
        else "'unknown'"
    )

    for col, declared_type in registry_types.items():
        if col not in df.columns:
            continue
        base_type = declared_type.split("|")[0].upper() if "|" in declared_type else declared_type.upper()

        try:
            if "|" in declared_type:
                # Date with format suffix — use TRY_STRPTIME
                _, fmt = declared_type.split("|", 1)
                failing = conn.execute(f"""
                    SELECT {pk_select} AS pk_val,
                           CAST("{col}" AS VARCHAR) AS raw_value
                    FROM df
                    WHERE TRY_STRPTIME(CAST("{col}" AS VARCHAR), '{fmt}') IS NULL
                    AND "{col}" IS NOT NULL
                    AND CAST("{col}" AS VARCHAR) != ''
                """).df()
            else:
                failing = conn.execute(f"""
                    SELECT {pk_select} AS pk_val,
                           CAST("{col}" AS VARCHAR) AS raw_value
                    FROM df
                    WHERE TRY_CAST("{col}" AS {base_type}) IS NULL
                    AND "{col}" IS NOT NULL
                    AND CAST("{col}" AS VARCHAR) != ''
                """).df()
        except Exception as exc:
            print(f"  [warn] TRY_CAST check failed for column '{col}': {exc}")
            continue

        for _, row in failing.iterrows():
            pk_str = str(row["pk_val"]) if row["pk_val"] is not None else "unknown"
            reason = f"[{col}] '{row['raw_value']}' cannot be cast to {base_type}"
            blocked_pks.setdefault(pk_str, []).append(reason)
            blocked_cols.setdefault(pk_str, []).append(col)

    if not blocked_pks:
        return df, 0

    flag_records = [
        FlagRecord(
            id=flag_id_for(pk),
            table_name=table_name,
            check_name="type_conflict",
            pk_value=pk,
            source_file=source_file,
            bad_columns="|".join(blocked_cols.get(pk, [])),
            reason=" | ".join(reasons)[:500],
        )
        for pk, reasons in blocked_pks.items()
    ]
    blocked_count = write_source_flags(conn, flag_records)

    # Remove bad rows from df
    if primary_key and primary_key in df.columns:
        bad_pk_strs = set(blocked_pks.keys())
        df = df[~df[primary_key].astype(str).isin(bad_pk_strs)].copy()
    else:
        # No PK — can't safely remove individual rows; fail the whole batch
        print(
            f"  [warn] {blocked_count} row(s) failed type validation but no primary_key "
            f"defined — cannot remove individual rows. All rows blocked."
        )
        return pd.DataFrame(columns=df.columns), blocked_count

    print(f"  [type_conflict] {blocked_count} row(s) blocked → source_block")
    return df, blocked_count


# ---------------------------------------------------------------------------
# ingest_single_file — primary entry point
# ---------------------------------------------------------------------------

def ingest_single_file(
    conn: duckdb.DuckDBPyConnection,
    path: Path,
    source: dict,
    mode: str = "append",
    on_duplicate_override: str | None = None,
    log_status_override: str | None = None,
    strip_pipeline_cols: bool = False,
) -> dict:
    """Process one file for a known source. Returns a summary dict.

    All data loading goes through DuckDB. Pandas is not used in the data path.

    :param conn:                  Open DuckDB connection.
    :param path:                  Path to the file.
    :param source:                Source definition from sources_config.yaml.
    :param mode:                  'append' or 'replace'.
    :param on_duplicate_override: Overrides source.on_duplicate when set.
                                  Pass 'upsert' from vp flagged retry.
    :param log_status_override:   Overrides the ingest_state status when set.
                                  Pass 'correction' from vp flagged retry.
    :param strip_pipeline_cols:   When True, drops all _-prefixed columns after
                                  load. Pass True from vp flagged retry to strip
                                  _flag_id, _flag_reason, _flag_columns etc.
    :return: {status, table, rows, new_cols, flagged, skipped} or
             {status: 'failed', message} on failure.
    """
    from proto_pipe.io.db import ensure_pipeline_tables
    ensure_pipeline_tables(conn)

    table = source["target_table"]
    primary_key = source.get("primary_key")
    on_duplicate = on_duplicate_override or source.get(
        "on_duplicate", "flag" if primary_key else "append"
    )
    is_csv = path.suffix.lower() == ".csv"
    is_excel = path.suffix.lower() in {".xlsx", ".xls"}
    source_file = str(path)

    # ── Step 1: Structural checks from file header ────────────────────────
    # Check for empty file, missing PK column, missing timestamp column.
    # All checks run from the file header — no DuckDB or pandas load yet.
    try:
        if is_csv:
            with open(path, newline="") as _f:
                reader = csv.reader(_f)
                try:
                    file_cols = next(reader)
                    has_data = next(reader, None) is not None
                except StopIteration:
                    file_cols = []
                    has_data = False
        else:
            # Excel — peek via DuckDB spatial for column names
            try:
                conn.execute("LOAD spatial")
                peek = conn.execute(
                    f"SELECT * FROM read_xlsx('{path}') LIMIT 1"
                ).df()
                file_cols = list(peek.columns)
                has_data = len(peek) > 0
            except Exception:
                # Fallback to pandas for structural peek only
                peek = pd.read_excel(path, nrows=1)
                file_cols = list(peek.columns)
                has_data = len(peek) > 0
    except Exception as exc:
        message = f"Could not read file: {exc}"
        print(f"[fail] '{path.name}': {message}")
        log_ingest_state(conn, path.name, table, "failed", message=message)
        return {"status": "failed", "message": message}

    if not has_data:
        message = "File is empty"
        print(f"[fail] '{path.name}': {message}")
        log_ingest_state(conn, path.name, table, "failed", message=message)
        return {"status": "failed", "message": message}

    timestamp_col = source.get("timestamp_col")
    if timestamp_col and timestamp_col not in file_cols:
        message = f"Missing required timestamp column '{timestamp_col}'"
        print(f"[fail] '{path.name}': {message}")
        log_ingest_state(conn, path.name, table, "failed", message=message)
        return {"status": "failed", "message": message}

    if primary_key and primary_key not in file_cols:
        message = f"Missing primary key column '{primary_key}'"
        print(f"[fail] '{path.name}': {message}")
        log_ingest_state(conn, path.name, table, "failed", message=message)
        return {"status": "failed", "message": message}

    # ── Step 2: Registry type lookup + unknown column check ───────────────
    # Strip blank/whitespace-only column names before all further checks.
    # These are CSV artefacts (trailing commas, empty header cells) and are
    # never registered in column_type_registry. Without this strip they
    # incorrectly trigger the unknown column error on every ingest.
    user_cols = [
        c for c in file_cols
        if not c.startswith("_") and c.strip()
    ]

    if not user_cols and file_cols:
        # All non-_ columns were blank-named — almost certainly a malformed file.
        blank_count = len([c for c in file_cols if not c.startswith("_")])
        print(
            f"  [warn] '{path.name}': {blank_count} column(s) with blank names"
            f" stripped — check for trailing commas in the CSV header."
        )

    registry_types = get_registry_types(conn, user_cols)

    # Unknown column check — only for normal ingest, not correction path.
    # Correction path (strip_pipeline_cols=True) uses confirmed registry types
    # from when the source was first created; new columns in corrections are
    # not expected and would be unusual.
    if not strip_pipeline_cols and registry_types:
        unknown_cols = [c for c in user_cols if c not in registry_types]
        if unknown_cols:
            message = (
                f"Unknown column(s) with no declared type: {', '.join(sorted(unknown_cols))}. "
                f"Run 'vp edit column-type' to confirm the type(s), then re-ingest."
            )
            print(f"[fail] '{path.name}': {message}")
            log_ingest_state(conn, path.name, table, "failed", message=message)
            return {"status": "failed", "message": message}

    # ── Step 3: Load via DuckDB ───────────────────────────────────────────
    try:
        if is_csv:
            df = _load_csv_with_types(conn, path, registry_types)
        else:
            df = _load_excel_with_types(conn, path, registry_types)
    except Exception as exc:
        message = f"Could not load file via DuckDB: {exc}"
        print(f"[fail] '{path.name}': {message}")
        log_ingest_state(conn, path.name, table, "failed", message=message)
        return {"status": "failed", "message": message}

    # Strip pipeline columns for correction path
    if strip_pipeline_cols:
        df = df[[c for c in df.columns if not c.startswith("_")]]

    # ── Step 4: NULL primary key check ───────────────────────────────────
    if primary_key and primary_key in df.columns:
        null_count = conn.execute(
            f'SELECT count(*) FROM df WHERE "{primary_key}" IS NULL'
        ).fetchone()[0]
        if null_count:
            message = (
                f"{null_count} row(s) have NULL in primary key '{primary_key}' — file rejected"
            )
            print(f"[fail] '{path.name}': {message}")
            log_ingest_state(conn, path.name, table, "failed", message=message)
            return {"status": "failed", "message": message}

    # ── Step 5: Row-level TRY_CAST validation ────────────────────────────
    # When no registry types are declared but the table already exists,
    # use the table's own column types to validate incoming values.
    effective_types = dict(registry_types)
    if not effective_types and table_exists(conn, table):
        from proto_pipe.io.db import get_column_types
        existing_table_types = get_column_types(conn, table)
        effective_types = {
            col: t for col, t in existing_table_types.items()
            if col in file_cols and not col.startswith("_")
        }

    df, blocked_count = _validate_row_types(
        conn, df, effective_types, table, primary_key, source_file
    )

    # Apply declared types to clean rows so DuckDB INSERT succeeds.
    # Done here, AFTER bad rows are removed, so apply_declared_types never
    # sees values like "N/A" that would be silently coerced to NULL.
    if effective_types and not df.empty:
        from proto_pipe.io.migration import apply_declared_types
        plain_types = {
            col: t for col, t in effective_types.items()
            if col in df.columns and "|" not in t
        }
        date_fmt_types = {
            col: t for col, t in effective_types.items()
            if col in df.columns and "|" in t
        }
        if plain_types or date_fmt_types:
            df = apply_declared_types(df, {**plain_types, **date_fmt_types})

    # Add pipeline timestamp
    df["_ingested_at"] = datetime.now(timezone.utc)

    new_cols: list[str] = []
    flagged_count = 0
    skipped_count = 0

    # ── Scenario A: first ingest or replace mode ──────────────────────────
    if mode == "replace" or not table_exists(conn, table):
        try:
            conn.execute(f'DROP TABLE IF EXISTS "{table}"')
            conn.execute(f'CREATE TABLE "{table}" AS SELECT * FROM df')
            print(f"[ok] '{path.name}' → '{table}' ({len(df)} rows, created)")
        except Exception as exc:
            message = f"DuckDB write failed: {exc}"
            print(f"[fail] '{path.name}': {message}")
            log_ingest_state(conn, path.name, table, "failed", message=message)
            return {"status": "failed", "message": message}

        # Seed source_pass for all inserted rows
        if primary_key and primary_key in df.columns and not df.empty:
            comparable_cols = [
                c for c in df.columns
                if not c.startswith("_") and c != primary_key
            ]
            # Compute all row hashes in one DuckDB query instead of per-row Python
            # calls — critical for large first ingests (e.g. 650k rows).
            hash_expr = (
                compute_row_hash_sql(comparable_cols)
                if comparable_cols else "md5('')"
            )
            hashes_df = conn.execute(f"""
                SELECT CAST("{primary_key}" AS VARCHAR) as pk_value,
                       {hash_expr} as row_hash
                FROM df
            """).df()
            pass_records = [
                {
                    "pk_value": row["pk_value"],
                    "row_hash": row["row_hash"],
                    "source_file": source_file,
                }
                for _, row in hashes_df.iterrows()
            ]
            bulk_upsert_source_pass(conn, table, pass_records)
            # Invariant: a row accepted into source_pass must not remain in
            # source_block. Clear any stale conflict entries for these pks.
            accepted_pks = [r["pk_value"] for r in pass_records]
            clear_source_block_for_pks(conn, table, accepted_pks)

    # ── Scenario B: subsequent ingest ─────────────────────────────────────
    else:
        try:
            new_cols = auto_migrate(conn, table, df)

            if primary_key:
                df, flagged_count, skipped_count = _handle_duplicates(
                    conn, table, df, primary_key, on_duplicate, source_file
                )
            else:
                if on_duplicate not in ("append", "flag"):
                    print(
                        f"[warn] on_duplicate='{on_duplicate}' ignored for "
                        f"'{table}' — no primary_key defined"
                    )

            if not df.empty:
                col_list = ", ".join(f'"{c}"' for c in df.columns)
                conn.execute(
                    f'INSERT INTO "{table}" ({col_list}) SELECT {col_list} FROM df'
                )

            print(
                f"[ok] '{path.name}' → '{table}' "
                f"({len(df)} inserted, "
                f"{flagged_count + blocked_count} blocked, "
                f"{skipped_count} skipped)"
            )
        except Exception as exc:
            message = f"DuckDB write failed: {exc}"
            print(f"[fail] '{path.name}': {message}")
            log_ingest_state(conn, path.name, table, "failed", message=message)
            return {"status": "failed", "message": message}

    final_status = log_status_override or "ok"
    log_ingest_state(
        conn, path.name, table, final_status,
        rows=len(df), new_cols=new_cols
    )
    return {
        "status": "ok",
        "table": table,
        "rows": len(df),
        "new_cols": new_cols,
        "flagged": flagged_count + blocked_count,
        "skipped": skipped_count,
    }


# ---------------------------------------------------------------------------
# Utility commands
# ---------------------------------------------------------------------------

def check_null_overwrites(
    conn: duckdb.DuckDBPyConnection,
    table: str,
    primary_key: str,
) -> int:
    """Scan the table for primary keys that appear more than once with
    different row content. Flags new conflicts to source_block.

    Returns the number of new conflicts flagged.
    """
    duplicate_keys = conn.execute(f"""
        SELECT "{primary_key}"
        FROM "{table}"
        GROUP BY "{primary_key}"
        HAVING count(*) > 1
    """).df()

    if duplicate_keys.empty:
        return 0

    all_cols = conn.execute(
        "SELECT column_name FROM information_schema.columns WHERE table_name = ?",
        [table],
    ).df()["column_name"].tolist()
    non_key_cols = [
        c for c in all_cols
        if c != primary_key and not c.startswith("_")
    ]

    keys = duplicate_keys[primary_key].tolist()
    flag_records: list[FlagRecord] = []

    for chunk_start in range(0, len(keys), CHUNK_SIZE):
        chunk_keys = keys[chunk_start: chunk_start + CHUNK_SIZE]
        placeholders = ", ".join(["?"] * len(chunk_keys))
        rows = conn.execute(
            f'SELECT * FROM "{table}" WHERE "{primary_key}" IN ({placeholders})',
            chunk_keys,
        ).df()

        for key_val, group in rows.groupby(primary_key):
            if len(group) < 2:
                continue
            first = group.iloc[0]
            for _, later in group.iloc[1:].iterrows():
                diffs = []
                for col in non_key_cols:
                    old_val = first.get(col)
                    new_val = later.get(col)
                    try:
                        old_null = pd.isna(old_val)
                        new_null = pd.isna(new_val)
                    except (TypeError, ValueError):
                        old_null = old_val is None
                        new_null = new_val is None
                    if old_null and new_null:
                        continue
                    if old_null != new_null or old_val != new_val:
                        old_str = "NULL" if old_null else str(old_val)
                        new_str = "NULL" if new_null else str(new_val)
                        diffs.append(f"{col}: {old_str} -> {new_str}")

                if diffs:
                    shown = diffs[:_MAX_DIFF_COLS]
                    leftover = len(diffs) - len(shown)
                    reason = " | ".join(shown)
                    if leftover:
                        reason += f" +{leftover} more"
                    flag_records.append(FlagRecord(
                        id=flag_id_for(str(key_val)),
                        table_name=table,
                        check_name="duplicate_conflict",
                        pk_value=str(key_val),
                        bad_columns=None,
                        reason=reason,
                    ))

    if flag_records:
        write_source_flags(conn, flag_records)

    return len(flag_records)


def reset_report(table_name: str, db_path: str) -> None:
    """Drop a report table and clear its ingest_state entries so files are re-ingested.

    After calling this, the next vp ingest run will recreate the table
    from source files as if it had never been ingested.
    """
    conn = duckdb.connect(db_path)
    try:
        if table_exists(conn, table_name):
            conn.execute(f'DROP TABLE "{table_name}"')
            print(f"  [reset] Dropped table '{table_name}'")
        else:
            print(f"  [reset] Table '{table_name}' does not exist — nothing to drop")

        deleted = conn.execute(
            "DELETE FROM ingest_state WHERE table_name = ? RETURNING filename",
            [table_name],
        ).fetchall()
        if deleted:
            filenames = [row[0] for row in deleted]
            print(
                f"  [reset] Cleared {len(filenames)} ingest_state entry/entries: "
                f"{', '.join(filenames)}"
            )

        # Also clear source_pass for this table
        from proto_pipe.io.db import clear_source_pass_for_table
        cleared = clear_source_pass_for_table(conn, table_name)
        if cleared:
            print(f"  [reset] Cleared {cleared} source_pass entry/entries")

    finally:
        conn.close()


def load_macros(conn: duckdb.DuckDBPyConnection, macros_dir: str) -> None:
    """Register all SQL macros found in macros_dir with the DuckDB connection."""
    p = Path(macros_dir)
    if not p.exists():
        print(f"  [warn] macros_dir '{macros_dir}' not found — skipping macro loading")
        return

    sql_files = sorted(p.glob("*.sql"))
    if not sql_files:
        return

    for sql_file in sql_files:
        try:
            sql = sql_file.read_text().strip()
            if sql:
                conn.execute(sql)
                print(f"  [macro] Loaded '{sql_file.name}'")
        except Exception as e:
            print(f"  [macro-fail] '{sql_file.name}': {e} — skipped")
