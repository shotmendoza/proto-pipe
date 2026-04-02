"""Ingestion module.

Scans a directory for CSV and Excel files, matches each file to a source
definition by pattern, and loads the data into DuckDB tables.

Table lifecycle:
- `db-init` — creates the DuckDB file and bootstraps empty tables from sources_config.yaml
- First ingest of a new source — creates the table lazily from the file's schema
- Subsequent ingests — appends rows, auto-migrating new columns if the schema grew

File deduplication:
- Files already logged as 'ok' in ingest_log are skipped automatically.
- Files that previously failed are retried on every run until they succeed.

Duplicate row handling (on_duplicate in sources_config.yaml):
- flag
    — (default when primary_key defined) compute md5 row hash; if hash
        differs from existing row, flag the conflict in flagged_rows with
        the changed column names in the reason, and skip the incoming row
- append
    — insert all rows regardless of duplicates
- upsert
    — delete existing rows by primary key, then insert incoming rows
- skip
    — silently discard rows whose primary key already exists

Each ingest run logs results (including failures) to the `ingest_log` table
so failures are visible without stopping the entire run.
"""
import csv
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
    table_exists, flag_id_for,
)
from proto_pipe.io.migration import auto_migrate
from proto_pipe.pipelines.integrity import (
    apply_declared_types,
    check_type_compatibility,
    IntegrityResult,
    write_integrity_flags
)


# ---------------------------------------------------------------------------
# Structural checks — lightweight, run per file before loading
# ---------------------------------------------------------------------------
_SUPPORTED_FILE_SUFFIXES = {".csv", ".xlsx", ".xls"}
"""GLOBAL: Supported file suffixes for ingestion"""

_MAX_DIFF_COLS = 5  # cap reason string to avoid wall-of-text in flagged_rows
"""GLOBAL: Maximum number of columns to include in the flaggin reason"""

# Number of primary keys processed per SQL IN (...) clause.
CHUNK_SIZE = 1000
"""GLOBAL: Maximum number of rows to process in a single SQL IN (...) clause"""


def _populate_builtin_metadata(conn: duckdb.DuckDBPyConnection) -> None:
    """Write metadata for all built-in checks. Safe to call multiple times —
    write_to_db is idempotent and only updates when the function source changes.
    """
    from proto_pipe.checks.built_in import BUILT_IN_CHECKS
    from proto_pipe.checks.registry import CheckParamInspector

    for name, func in BUILT_IN_CHECKS.items():
        inspector = CheckParamInspector(func)
        inspector.write_to_db(conn, name)


# ---------------------------------------------------------------------------
# Flag identity
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# Structural checks
# ---------------------------------------------------------------------------
def _is_supported_file(path: Path) -> bool:
    """Checks if the given file has a supported file extension.

    This function evaluates whether the file specified by the given path has a
    suffix that matches any of the supported file suffixes.

    :param path: The file path to be checked.
    :type path: Path
    :return: True if the file's suffix is in the list of supported suffixes,
        False otherwise.
    :rtype: bool
    """
    return path.suffix.lower() in _SUPPORTED_FILE_SUFFIXES


def _structural_checks(
        df: pd.DataFrame,
        source: dict
) -> list[str]:
    """Perform structural integrity checks on the provided DataFrame based on the source metadata.

    This check evaluates and runs before the existing database tables are touched.

    This function evaluates the DataFrame to ensure it is not empty and verifies the
    existence of a required timestamp column if specified in the given source dictionary.

    :param df: The input DataFrame subject to validation.
    :type df: pandas.DataFrame
    :param source: A dictionary containing metadata configuration, where the key
        "timestamp_col" defines the expected name of the timestamp column.
    :type source: dict

    :return: A list of string messages describing any structural issues found
        in the DataFrame. The list will be empty if no issues are detected.
    :rtype: list[str]
    """
    issues: list[str] = []

    if df.empty:
        issues.append("File is empty")

    timestamp_col = source.get("timestamp_col")
    if timestamp_col and timestamp_col not in df.columns:
        issues.append(f"Missing required timestamp column '{timestamp_col}'")

    primary_key = source.get("primary_key")
    if primary_key and primary_key in df.columns:
        null_count = int(df[primary_key].isna().sum())
        if null_count:
            issues.append(
                f"{null_count} row(s) have NULL value in primary key "
                f"column '{primary_key}' — file rejected"
            )
    return issues


# ---------------------------------------------------------------------------
# ingest_log table
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def resolve_source(
        filename: str,
        sources: list[dict]
) -> dict | None:
    """Return the first source whose patterns match the filename.

    This function iterates through a list of source dictionaries and checks if the
    provided filename matches any patterns specified in the sources. If a match is
    found, it returns the matching source dictionary. If no matches are found,
    it returns None. Pattern matching is performed using the `fnmatch` module.

    :param filename: The name of the file to resolve against the source patterns.
    :param sources: A list of dictionaries. Each dictionary should include a key "patterns", which contains a list of filename patterns.
    :return: The first matching source dictionary if a match is found, otherwise None.
    """
    for source in sources:
        if any(fnmatch(filename, pattern) for pattern in source["patterns"]):
            return source
    return None


def load_file(path: Path) -> pd.DataFrame:
    """Load a CSV or Excel file into a DataFrame.

    This function supports loading files in CSV or Excel formats. Depending on
    the file extension, the appropriate pandas function will be used to load
    the file. If the file extension is not supported, the function raises
    a ValueError.

    :param path: The path to the file to be loaded.
    :type path: Path
    :return: A pandas DataFrame containing the file data.
    :rtype: pd.DataFrame
    :raises ValueError: If the file type is unsupported (neither .csv, .xlsx, nor .xls).
    """
    suffix = path.suffix.lower()

    if suffix == ".csv":
        return pd.read_csv(path)

    if suffix in {".xlsx", ".xls"}:
        return pd.read_excel(path)

    raise ValueError(f"Unsupported file type: {path.suffix}")


# ---------------------------------------------------------------------------
# DB initialisation
# ---------------------------------------------------------------------------


def init_db(db_path: str) -> None:
    """Initializes the database by creating the necessary directory structure
    and establishing a connection to the database file.

    :param db_path: The path to the database file to be initialized.
    :type db_path: str
    :return: None
    """
    from proto_pipe.io.db import init_all_pipeline_tables

    Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    with duckdb.connect(db_path) as conn:
        init_all_pipeline_tables(conn)
        _populate_builtin_metadata(conn)


def init_source_tables(db_path: str, sources: list[dict]):
    """Creates placeholder tables for the user-defined tables,
    given source definitions from the config."""
    with duckdb.connect(db_path) as conn:
        for source in sources:
            table = source["target_table"]
            if table_exists(conn, table):
                print(f"[skip] Table '{table}' already exists")
                continue
            conn.execute(f'CREATE TABLE IF NOT EXISTS "{table}" (_placeholder VARCHAR)')
            print(f"[ok] Created table '{table}'")


# ---------------------------------------------------------------------------
# Ingest
# ---------------------------------------------------------------------------


def ingest_directory(
    directory: str,
    sources: list[dict],
    db_path: str,
    mode: str = "append",
    run_checks: bool = False,
    check_registry=None,
    report_registry=None,
) -> dict:
    """Scan a directory, match files to source definitions, and load into DuckDB.

    Two-scenario ingest:
      Scenario A (first ingest / replace mode):
        - Queries column_type_registry for user-confirmed types.
        - If registry entries exist, pre-scans with TRY_CAST. Mismatch fails the file.
        - CSV: reloaded via DuckDB read_csv with declared types.
        - Excel: apply_declared_types.
        - If no registry entries, falls back to inference.

      Scenario B (subsequent ingest, table exists):
        - CSV: loaded via DuckDB read_csv with declared types before validators run.
        - Excel: apply_declared_types.
        - _ROW_VALIDATORS pre-scan; failing rows go to flagged_rows.
        - Clean rows through duplicate handling and INSERT.
    """
    from proto_pipe.io.db import (
        init_ingest_log as _init_ingest_log,
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
    _init_ingest_log(conn)

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

        result = ingest_single_file(conn, path, source, mode=mode)
        summary[path.name] = result

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
        from proto_pipe.io.db import log_ingest as _log_ingest

        for filename in unmatched:
            _log_ingest(
                conn, filename, None, "skipped", message="No matching source pattern"
            )

    conn.close()
    return summary


def _run_inline_checks(
        conn,
        table,
        source,
        check_registry,
        report_registry,
        filename
):
    """Executes inline checks for a given table and source by leveraging the check
    and report registries to find matching reports and checks to run for the table.

    :param conn: The database connection object used to execute queries.
    :type conn: Any
    :param table: The name of the table to query and perform checks on.
    :type table: str
    :param source: The source identifier associated with the table.
    :type source: Any
    :param check_registry: The registry containing defined check logic to execute.
    :type check_registry: Any
    :param report_registry: The registry containing reports that define resolved checks.
    :type report_registry: Any
    :param filename: Unused parameter; might be reserved for future functionality.
    :type filename: Any
    :return: None
    """
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
# NEW — file deduplication
# ---------------------------------------------------------------------------


def _row_hash_expr(cols: list[str], alias: str | None = None) -> str:
    """Build a DuckDB SQL expression that computes an md5 hash over all given
    columns, coercing each to VARCHAR. Uses '|' as a separator so that
    different column values can't accidentally produce the same string.

    When `alias` is provided, will prefix each column with the table alias.

    The alias is used when computing hashes in a JOIN so the correct table's column
    is referenced — e.g. alias='e' gives CAST(e."price" AS VARCHAR)
    not CAST("e."price"" AS VARCHAR).
    """
    if alias:
        parts = " || '|' || ".join(
            [f"COALESCE(CAST({alias}.\"{c}\" AS VARCHAR), '')" for c in cols]
        )
    else:
        parts = " || '|' || ".join(
            [f"COALESCE(CAST(\"{c}\" AS VARCHAR), '')" for c in cols]
        )
    return f"md5({parts})"


def _write_flag(
    conn: duckdb.DuckDBPyConnection,
    table_name: str,
    diffs: list[str],
    pk_value: str | int | float | None = None,
) -> None:
    """Insert one conflict entry into flagged_rows.

    id = md5(str(pk_value)) when pk_value is provided — deterministic,
    computable in DuckDB SQL at export time, no extra columns needed.

    Falls back to uuid4 when pk_value is None (validation check flags).
    ON CONFLICT (id) DO NOTHING makes this idempotent.

    diffs is a list of "col: old -> new" strings, capped at _MAX_DIFF_COLS.
    Example: ["price: -50.0 -> 75.0", "region: EMEA -> APAC"]
    """
    shown = diffs[:_MAX_DIFF_COLS]
    leftover = len(diffs) - len(shown)
    reason = " | ".join(shown) + (f" +{leftover} more" if leftover else "")
    flag_id = flag_id_for(pk_value=pk_value)

    conn.execute("""
        INSERT INTO flagged_rows
            (id, table_name, check_name, reason, flagged_at)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT (id) DO NOTHING
    """, [
        flag_id,
        table_name,
        "duplicate_conflict",
        reason,
        datetime.now(timezone.utc),
    ])


def _changed_columns(
    existing_row: pd.Series,
    incoming_row: pd.Series,
    cols: list[str],
) -> list[str]:
    """Return a list of "col: old -> new" diff strings for columns whose
    values differ between the existing and incoming row.
    """
    diffs = []
    for col in cols:
        old_val = existing_row.get(col)
        new_val = incoming_row.get(col)
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
            continue
    return diffs


def _handle_duplicates(
    conn: duckdb.DuckDBPyConnection,
    table: str,
    df: pd.DataFrame,
    primary_key: str,
    on_duplicate: Literal["append", "upsert", "skip", "flag"] = "flag",
) -> tuple[pd.DataFrame, int, int]:
    """Process incoming rows against existing rows by primary key.

    For flag mode, a single SQL query per chunk categorises every incoming
    row as 'new', 'identical', or 'conflict' and computes the per-column
    diff reason string entirely inside DuckDB. Conflicts are then written
    to flagged_rows in one batched INSERT.

    Processes keys in chunks of CHUNK_SIZE to avoid large IN (...) clauses.
    Returns (rows_to_insert, flagged_count, skipped_count).

    on_duplicate:
        - append: insert everything, no conflict checking
        - upsert: replace matching rows
        - skip: ignore matching rows. Keeps old rows
        - flag: compare row contents; identical rows are ignored, changed rows are recorded as conflicts
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
        column for column in non_null_df.columns
        if column != primary_key and
           column in get_existing_columns(conn, table) and not
        str(column).startswith("_")
    ]

    rows_to_insert: list[tuple] = []
    total_flagged = 0
    total_skipped = 0

    for chunk_start in range(0, len(non_null_df), CHUNK_SIZE):
        chunk = non_null_df.iloc[chunk_start: chunk_start + CHUNK_SIZE]
        chunk_keys = chunk[primary_key].tolist()
        placeholders = ", ".join(["?"] * len(chunk_keys))

        existing_df = conn.execute(
            f'SELECT * FROM "{table}" WHERE "{primary_key}" IN ({placeholders})',
            chunk_keys,
        ).df()

        if existing_df.empty:
            rows_to_insert.extend(chunk.itertuples(index=False, name=None))
            continue

        if on_duplicate == "upsert":
            conn.execute(
                f'DELETE FROM "{table}" WHERE "{primary_key}" IN ({placeholders})',
                chunk_keys,
            )
            rows_to_insert.extend(chunk.itertuples(index=False, name=None))
            continue

        if on_duplicate == "skip":
            existing_keys = set(existing_df[primary_key].tolist())
            keep = chunk[~chunk[primary_key].isin(existing_keys)]
            skipped = len(chunk) - len(keep)
            if skipped:
                print(f"[skip] {skipped} row(s) already exist — skipped")
            rows_to_insert.extend(keep.itertuples(index=False, name=None))
            total_skipped += skipped
            continue

        if on_duplicate == "flag":
            if not comparable_cols:
                rows_to_insert.extend(chunk.itertuples(index=False, name=None))
                continue

            dup_existing = len(existing_df) - existing_df[primary_key].nunique()
            if dup_existing:
                print(
                    f"[warn] {dup_existing} duplicate primary key(s) in existing "
                    f"table — using first occurrence for conflict comparison"
                )

            # Build one CASE WHEN expression per column.
            # Produces NULL when unchanged, 'col: old -> new' when it differs.
            # __NULL__ sentinel distinguishes SQL NULL from the string 'NULL'.
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

            # list_filter removes NULLs (unchanged cols),
            # array_to_string joins the remaining diffs with ' | '
            reason_expr = (
                "array_to_string(list_filter(["
                + ", ".join(col_diff_exprs)
                + "], x -> x IS NOT NULL), ' | ')"
            )

            e_hash = _row_hash_expr(cols=comparable_cols, alias="e")
            c_hash = _row_hash_expr(cols=comparable_cols, alias="c")

            # Single query categorizes all rows in the chunk.
            # DISTINCT ON in the subquery handles duplicate existing keys.
            # chunk_keys passed twice: once for the subquery, once for outer WHERE.
            classify_sql = f"""
                SELECT
                    c."{primary_key}",
                    CASE
                        WHEN e."{primary_key}" IS NULL THEN 'new'
                        WHEN {e_hash} = {c_hash} THEN 'identical'
                        ELSE 'conflict'
                    END AS status,
                    {reason_expr} AS reason
                FROM chunk c
                LEFT JOIN (
                    SELECT DISTINCT ON ("{primary_key}") *
                    FROM "{table}"
                    WHERE "{primary_key}" IN ({placeholders})
                ) e ON CAST(c."{primary_key}" AS VARCHAR)
                      = CAST(e."{primary_key}" AS VARCHAR)
                WHERE c."{primary_key}" IN ({placeholders})
            """
            classified = conn.execute(classify_sql, chunk_keys + chunk_keys).df()

            # New rows — insert directly, no conflict
            new_keys = set(
                classified.loc[classified["status"] == "new", primary_key].tolist()
            )
            if new_keys:
                new_rows = chunk[chunk[primary_key].isin(new_keys)]
                rows_to_insert.extend(new_rows.itertuples(index=False, name=None))

            # Conflicts — build flags DataFrame and INSERT in one statement.
            # One round-trip to DuckDB regardless of how many conflicts exist.
            conflicts = classified[classified["status"] == "conflict"].copy()
            if not conflicts.empty:
                flags_df = pd.DataFrame({
                    "id": conflicts[primary_key].apply(flag_id_for),
                    "table_name": table,
                    "check_name": "duplicate_conflict",
                    "reason": conflicts["reason"].fillna("").str[:500],
                    "flagged_at": datetime.now(timezone.utc),
                })
                conn.execute("""
                    INSERT INTO flagged_rows
                        (id, table_name, check_name, reason, flagged_at)
                    SELECT id, table_name, check_name, reason, flagged_at
                    FROM flags_df
                    ON CONFLICT (id) DO NOTHING
                """)
                total_flagged += len(conflicts)
                for key_val, reason_str in zip(
                    conflicts[primary_key], conflicts["reason"].fillna("")
                ):
                    diffs = [d.strip() for d in str(reason_str).split(" | ") if d.strip()]
                    print(
                        f"  [flag] key={key_val} — "
                        f"{' | '.join(diffs[:3])}{'...' if len(diffs) > 3 else ''}"
                    )
            continue

        print(f"[warn] Unknown on_duplicate '{on_duplicate}' — appending chunk")
        rows_to_insert.extend(chunk.itertuples(index=False, name=None))

    # FIXED: previously this block was indented inside the chunk loop,
    # causing early return after the first chunk for unknown on_duplicate modes.
    if total_flagged:
        print(
            f"[flag] {total_flagged} conflict(s) flagged — original rows kept"
        )

    result = (
        pd.DataFrame(rows_to_insert, columns=df.columns)
        if rows_to_insert
        else pd.DataFrame(columns=df.columns)
    )
    return result, total_flagged, total_skipped


def check_null_overwrites(
    conn: duckdb.DuckDBPyConnection,
    table: str,
    primary_key: str,
) -> int:
    """Manual re-check: scan the table for primary keys that appear more than
    once with different row content, and flag any new conflicts not already
    recorded in flagged_rows.

    Idempotent — md5(pk_value) + ON CONFLICT DO NOTHING means running twice
    is safe; already-open flags are silently skipped rather than duplicated.
    The old LIKE-based duplicate check is removed — UUID idempotency replaces it.

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
    non_key_cols = [c for c in all_cols if c != primary_key]

    keys = duplicate_keys[primary_key].tolist()
    flagged = 0

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
                changed = _changed_columns(first, later, non_key_cols)
                if not changed:
                    continue
                _write_flag(conn, table, changed, pk_value=str(key_val))
                flagged += 1
    return flagged


def reset_report(table_name: str, db_path: str) -> None:
    """Drop a report table and clear its ingest_log entries so files are re-ingested.

    This is the implementation backing `vp table-reset`. After calling this,
    the next `vp ingest` run will recreate the table from source files as if
    it had never been ingested, including re-applying any transforms.

    :param table_name: The DuckDB table name to reset (from sources_config target_table).
    :param db_path:    Path to the pipeline DuckDB file.
    """
    conn = duckdb.connect(db_path)
    try:
        if table_exists(conn, table_name):
            conn.execute(f'DROP TABLE "{table_name}"')
            print(f"  [reset] Dropped table '{table_name}'")
        else:
            print(f"  [reset] Table '{table_name}' does not exist — nothing to drop")

        # Clear ingest_log so source files are picked up again on next ingest
        deleted = conn.execute(
            "DELETE FROM ingest_log WHERE table_name = ? RETURNING filename",
            [table_name],
        ).fetchall()
        if deleted:
            filenames = [row[0] for row in deleted]
            print(f"  [reset] Cleared {len(filenames)} ingest_log entry/entries: {', '.join(filenames)}")
    finally:
        conn.close()


def load_macros(conn: duckdb.DuckDBPyConnection, macros_dir: str) -> None:
    """Register all SQL macros found in macros_dir with the DuckDB connection.

    Scans all *.sql files in macros_dir and executes them. Files should contain
    CREATE OR REPLACE MACRO statements so re-running is idempotent.

    Missing macros_dir produces a warning, not an error — the pipeline continues
    without macros. Individual file errors are also warned and skipped so one
    broken macro file doesn't block the whole pipeline.

    :param conn:       An open DuckDB connection to register macros against.
    :param macros_dir: Path to the directory containing .sql macro files.
    """
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


#############################
# PRE-SCAN FUNCTIONS
#############################


def _load_csv_with_types(
    conn: duckdb.DuckDBPyConnection,
    path: Path,
    registry_types: dict[str, str],
) -> pd.DataFrame:
    """Load a CSV file via DuckDB with declared column types applied natively.

    Uses DuckDB's read_csv with a dtype mapping so type enforcement happens
    inside DuckDB rather than in pandas. This eliminates float64 artefacts
    (e.g. 12500.0 for a BIGINT column) that cause phantom duplicate-conflict
    flags when comparing against DuckDB-stored integer values.

    Columns with a date format suffix (e.g. DATE|%m-%d-%y) are loaded as
    VARCHAR first, then converted via apply_declared_types — DuckDB's read_csv
    dtype parameter does not support strptime format strings.

    :param conn:           Open DuckDB connection.
    :param path:           Path to the CSV file.
    :param registry_types: {column: declared_type} — may include format suffix.
    :return: DataFrame with columns typed per declared types.
    """
    with open(path, newline="") as f:
        csv_columns = set(next(csv.reader(f)))

    dtype: dict[str, str] = {}
    date_format_cols: dict[str, str] = {}

    for col, declared_type in registry_types.items():
        if col not in csv_columns:
            continue

        if "|" in declared_type:
            # Date with format suffix — load as VARCHAR, convert after
            date_format_cols[col] = declared_type
            dtype[col] = "VARCHAR"
        else:
            dtype[col] = declared_type

    df = conn.read_csv(str(path), dtype=dtype).df()

    if date_format_cols:
        df = apply_declared_types(df, date_format_cols)

    return df


def ingest_single_file(
    conn: duckdb.DuckDBPyConnection,
    path: Path,
    source: dict,
    mode: str = "append",
    on_duplicate_override: str | None = None,
    log_status_override: str | None = None,
    strip_pipeline_cols: bool = False,
) -> dict:
    """Process one file for a known source. Returns a summary dict entry.

    Extracted from ingest_directory to allow direct invocation for correction
    retries without going through the full directory scan.

    :param conn:                  Open DuckDB connection.
    :param path:                  Path to the file to ingest.
    :param source:                Source definition dict from sources_config.yaml.
    :param on_duplicate_override: When set, overrides source.on_duplicate.
                                  Pass 'upsert' from vp flagged retry to force
                                  correction rows to overwrite existing data.
    :param log_status_override:   When set, overrides the ingest_log status.
                                  Pass 'correction' from vp flagged retry for
                                  auditability — corrections are distinguishable
                                  from normal ingests in ingest_log.
    :param strip_pipeline_cols:   When True, drops all _-prefixed columns after
                                  load. Pass True from vp flagged retry to strip
                                  flag metadata columns (_flag_id, _flag_reason,
                                  etc.) from the correction CSV before ingest.
    :return: Summary dict: {status, table, rows, new_cols, flagged, skipped}
             or {status: 'failed', message} on failure.
    """
    from proto_pipe.io.db import (
        table_exists as _table_exists,
        get_registry_types,
        log_ingest as _log_ingest,
    )
    from proto_pipe.pipelines.integrity import (
        check_type_compatibility,
        IntegrityResult,
        write_integrity_flags,
        _ROW_VALIDATORS,
    )

    # Load file via pandas — used for structural checks and type compat scan.
    # CSV files are reloaded via DuckDB after validation passes.
    try:
        df = load_file(path)
    except Exception as exc:
        message = f"Could not load file: {exc}"
        print(f"[fail] '{path.name}': {message}")
        _log_ingest(conn, path.name, None, "failed", message=message)
        return {"status": "failed", "message": message}

    # Strip all pipeline (_-prefixed) columns when requested.
    # Used by vp flagged retry to remove flag metadata columns from
    # the correction CSV (e.g. _flag_id, _flag_reason, _flagged_at)
    # before re-ingesting through the normal type-check and upsert path.
    if strip_pipeline_cols:
        df = df[[c for c in df.columns if not c.startswith("_")]]

    # Structural checks
    issues = _structural_checks(df, source)
    if issues:
        message = "; ".join(issues)
        print(f"[fail] '{path.name}': {message}")
        _log_ingest(conn, path.name, source["target_table"], "failed", message=message)
        return {"status": "failed", "message": message}

    table = source["target_table"]
    primary_key = source.get("primary_key")
    on_duplicate = on_duplicate_override or source.get(
        "on_duplicate", "flag" if primary_key else "append"
    )
    is_csv = path.suffix.lower() == ".csv"

    integrity_flagged = 0
    new_cols: list[str] = []
    flagged_count = 0
    skipped_count = 0

    # ── Scenario A: first ingest ──────────────────────────────────────────
    if mode == "replace" or not _table_exists(conn, table):
        user_cols = [c for c in df.columns if not c.startswith("_")]
        registry_types = get_registry_types(conn, user_cols)

        if registry_types:
            type_issues = check_type_compatibility(
                df, registry_types, conn, primary_key
            )
            if type_issues:
                cols_affected = sorted({i.column for i in type_issues})
                message = (
                    f"Type mismatch for column(s): {', '.join(cols_affected)}. "
                    f"Fix the file values or run 'vp edit source' to review "
                    f"the registered types."
                )
                print(f"[fail] '{path.name}': {message}")
                _log_ingest(conn, path.name, table, "failed", message=message)
                return {"status": "failed", "message": message}

            if is_csv:
                df = _load_csv_with_types(conn, path, registry_types)
            else:
                df = apply_declared_types(df, registry_types)

        df["_ingested_at"] = datetime.now(timezone.utc)

        try:
            conn.execute(f'DROP TABLE IF EXISTS "{table}"')
            conn.execute(f'CREATE TABLE "{table}" AS SELECT * FROM df')
            print(f"[ok] '{path.name}' → '{table}' ({len(df)} rows, created)")
        except Exception as exc:
            message = f"DuckDB write failed: {exc}"
            print(f"[fail] '{path.name}': {message}")
            _log_ingest(conn, path.name, table, "failed", message=message)
            return {"status": "failed", "message": message}

    # ── Scenario B: subsequent ingest ─────────────────────────────────────
    else:
        user_cols = [c for c in df.columns if not c.startswith("_")]
        registry_types = get_registry_types(conn, user_cols)

        if registry_types:
            if is_csv:
                df = _load_csv_with_types(conn, path, registry_types)
            else:
                df = apply_declared_types(df, registry_types)

        df["_ingested_at"] = datetime.now(timezone.utc)

        integrity_issues: list[IntegrityResult] = []
        for validator in _ROW_VALIDATORS:
            issues = validator(conn, table, df, primary_key)
            for issue in issues:
                issue.filename = path.name
            integrity_issues.extend(issues)

        if integrity_issues:
            integrity_flagged = write_integrity_flags(conn, table, integrity_issues)
            if primary_key:
                bad_pks = {
                    i.pk_value for i in integrity_issues if i.pk_value is not None
                }
                if bad_pks:
                    df = df[~df[primary_key].astype(str).isin(bad_pks)].copy()
            print(
                f"  [integrity] {integrity_flagged} row(s) flagged for type conflicts "
                f"— see flagged_rows for details"
            )

        try:
            new_cols = auto_migrate(conn, table, df)

            if primary_key:
                df, flagged_count, skipped_count = _handle_duplicates(
                    conn, table, df, primary_key, on_duplicate
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
                f"{flagged_count + integrity_flagged} flagged, "
                f"{skipped_count} skipped)"
            )
        except Exception as exc:
            message = f"DuckDB write failed: {exc}"
            print(f"[fail] '{path.name}': {message}")
            _log_ingest(conn, path.name, table, "failed", message=message)
            return {"status": "failed", "message": message}

    final_status = log_status_override or "ok"
    _log_ingest(conn, path.name, table, final_status, rows=len(df), new_cols=new_cols)
    return {
        "status": "ok",
        "table": table,
        "rows": len(df),
        "new_cols": new_cols,
        "flagged": flagged_count + integrity_flagged,
        "skipped": skipped_count,
    }
