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
import hashlib
import json
import uuid
from datetime import datetime, timezone
from fnmatch import fnmatch
from pathlib import Path
from typing import Literal

import duckdb
import pandas as pd  # type: ignore

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


def _init_check_registry_metadata(conn: duckdb.DuckDBPyConnection) -> None:
    """Create check_registry_metadata table if it doesn't exist."""
    conn.execute("""
        CREATE TABLE IF NOT EXISTS check_registry_metadata (
            id                      VARCHAR PRIMARY KEY,
            check_name              VARCHAR NOT NULL UNIQUE,
            check_key               VARCHAR NOT NULL,
            is_multiselect_eligible BOOLEAN NOT NULL,
            column_params           VARCHAR,
            scalar_params           VARCHAR,
            recorded_at             TIMESTAMPTZ NOT NULL
        )
    """)


def _populate_builtin_metadata(conn: duckdb.DuckDBPyConnection) -> None:
    """Write metadata for all built-in checks. Safe to call multiple times —
    write_to_db is idempotent and only updates when the function source changes.
    """
    from proto_pipe.checks.built_in import BUILT_IN_CHECKS
    from proto_pipe.checks.inspector import CheckParamInspector

    for name, func in BUILT_IN_CHECKS.items():
        inspector = CheckParamInspector(func)
        inspector.write_to_db(conn, name)


# ---------------------------------------------------------------------------
# Flag identity
# ---------------------------------------------------------------------------
def flag_id_for(
        pk_value: str | int | float | None,
) -> str:
    """Return the deterministic flag id for a given primary key value.

    id = md5(str(pk_value))

    Using md5 means the same expression is computable in DuckDB SQL:
        md5(CAST(source.pk_col AS VARCHAR))

    If pk_value is null, then will return a UUID4 with a string wrap.
    """
    if pk_value is None:
        return str(uuid.uuid4())
    return hashlib.md5(str(pk_value).encode()).hexdigest()


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

def _init_ingest_log(conn: duckdb.DuckDBPyConnection) -> None:
    """Initializes the ingest log table in the provided DuckDB connection.

    This table is used for logging details about data ingestion activities,
    such as the status, number of rows processed, and additional columns added during the
    process.

    The ingest log table includes the following columns:
        - id: Primary key for identifying each log entry.
        - filename: The name of the file being ingested.
        - table_name: Name of the destination table, if applicable.
        - status: Status of the ingestion (e.g., 'ok', 'failed', 'skipped').
        - rows: Number of rows processed during the ingestion.
        - new_cols: JSON list of columns added during the ingestion.
        - message: Additional information or error message for logging.
        - ingested_at: Timestamp of when the ingestion occurred.

    :param conn: The DuckDB connection where the ingest log table should be initialized.
    :type conn: duckdb.DuckDBPyConnection
    :return: None
    """
    conn.execute("""
        CREATE TABLE IF NOT EXISTS ingest_log (
            id          VARCHAR PRIMARY KEY,
            filename    VARCHAR NOT NULL,
            table_name  VARCHAR,
            status      VARCHAR NOT NULL,   -- 'ok' | 'failed' | 'skipped'
            rows        INTEGER,
            new_cols    VARCHAR,            -- JSON list of added columns
            message     VARCHAR,
            ingested_at TIMESTAMPTZ NOT NULL
        )
    """)


def _log_ingest(
        conn: duckdb.DuckDBPyConnection,
        filename: str,
        table_name: str | None,
        status: str,
        rows: int | None = None,
        new_cols: list[str] | None = None,
        message: str | None = None
) -> None:
    """Logs a single ingestion detail into the ingestion_log database.

    This function inserts information about a data ingestion process into
    an `ingest_log` database table. It keeps a record of filenames,
    table names, statuses, row counts, new columns, and optional messages
    related to the ingestion. Every log entry is timestamped with the
    current UTC time.

    :param conn: A database connection object used to execute the SQL insert statement.
    :param filename: The name of the file being ingested.
    :param table_name: The name of the database table associated with the ingestion.
    :param status: The status of the ingestion operation.
    :param rows: The count of rows ingested. Optional.
    :param new_cols: A list of new columns added during ingestion. Optional.
    :param message: An additional message or comment about the ingestion process. Optional.
    :return: None
    """
    conn.execute("""
        INSERT INTO ingest_log
            (id, filename, table_name, status, rows, new_cols, message, ingested_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """, [
        str(uuid.uuid4()),
        filename,
        table_name,
        status,
        rows,
        json.dumps(new_cols) if new_cols else None,
        message,
        datetime.now(timezone.utc),
    ])


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


def _table_exists(conn: duckdb.DuckDBPyConnection, table: str) -> bool:
    """Checks if a table exists in the given database connection.

    This function queries the `information_schema.tables` to determine whether the
    specified table exists in the database connected through the DuckDB connection.

    :param conn: The DuckDB connection object used to execute the query.
    :param table: The name of the table to check for existence.
    :return: Boolean value indicating whether the table exists in the database.
    :rtype: bool
    """
    result = conn.execute(
        "SELECT count(*) FROM information_schema.tables WHERE table_name = ?",
        [table],
    ).fetchone()
    return result[0] > 0


def _get_existing_columns(conn: duckdb.DuckDBPyConnection, table: str) -> set[str]:
    """Retrieve the existing column names of a specified table within a DuckDB database.

    This function queries the information schema to gather a set of all column names
    that exist in the specified table. The returned set can be used to verify, process,
    or compare column structures within the database.

    :param conn: The DuckDBPyConnection object used to interact with the DuckDB database.
    :param table: The name of the table whose column names are to be retrieved.
    :return: A set containing the names of columns in the specified table.
    :rtype: set[str]
    """
    rows = conn.execute(
        "SELECT column_name FROM information_schema.columns WHERE table_name = ?",
        [table],
    ).fetchall()
    return {row[0] for row in rows}


def _auto_migrate(conn: duckdb.DuckDBPyConnection, table: str, df: pd.DataFrame) -> list[str]:
    """Automatically migrates the schema of an existing DuckDB table to align with the columns of a given
    Pandas DataFrame. Any new columns in the DataFrame that are not present in the table will be added
    to the table with an appropriate data type.

    :param conn: DuckDB connection object.
    :type conn: duckdb.DuckDBPyConnection
    :param table: Name of the table to be migrated.
    :type table: str
    :param df: DataFrame containing the desired schema.
    :type df: pd.DataFrame
    :return: List of column names that were added to the table.
    :rtype: list[str]
    """
    existing = _get_existing_columns(conn, table)
    new_cols = [col for col in df.columns if col not in existing]

    for col in new_cols:
        dtype = df[col].dtype
        if pd.api.types.is_integer_dtype(dtype):
            sql_type = "BIGINT"
        elif pd.api.types.is_float_dtype(dtype):
            sql_type = "DOUBLE"
        elif pd.api.types.is_bool_dtype(dtype):
            sql_type = "BOOLEAN"
        elif pd.api.types.is_datetime64_any_dtype(dtype):
            sql_type = "TIMESTAMPTZ"
        else:
            sql_type = "VARCHAR"

        conn.execute(f'ALTER TABLE "{table}" ADD COLUMN "{col}" {sql_type}')
        print(f"  [migrate] Added column '{col}' ({sql_type}) to '{table}'")

    return new_cols


# ---------------------------------------------------------------------------
# DB initialisation
# ---------------------------------------------------------------------------

def init_db(db_path: str) -> None:
    """Initializes the database by creating the necessary directory structure
    and establishing a connection to the database file. This function also
    prepares the ingest log for future use. The connection to the database
    is closed after the initialization process is completed.

    :param db_path: The path to the database file to be initialized.
    :type db_path: str
    :return: None
    """
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    with duckdb.connect(db_path) as conn:
        _init_ingest_log(conn)
        _init_check_registry_metadata(conn)
        _populate_builtin_metadata(conn)


def init_source_tables(db_path: str, sources: list[dict]):
    """Creates placeholder tables for the user-defined tables,
    given source definitions from the config."""
    with duckdb.connect(db_path) as conn:
        for source in sources:
            table = source["target_table"]
            if _table_exists(conn, table):
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
    mode: Literal["append", "replace"] = "append",
    run_checks: bool = False,
    check_registry=None,
    report_registry=None,
) -> dict:
    """Scan a directory, match files to source definitions, and load into DuckDB.

    :param directory: The directory containing files to ingest.
    :param sources: A list of source definitions mapping file patterns to target database tables.
    :param db_path: Path to the DuckDB database file used for data storage and ingestion.
    :param mode: The ingestion mode, either "append" to add records to existing tables or "replace" to overwrite them.
    :param run_checks: A flag indicating whether to execute validation checks after ingestion.
    :param check_registry: Registry of available checks to validate ingested data, if enabled.
    :param report_registry: Registry used to log the results of executed checks, if enabled.
    :return: A dictionary summarizing the results of the ingestion process for each file.
    """
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
        # skips if file is not supported
        if not _is_supported_file(path):
            continue

        source = resolve_source(path.name, sources)
        if source is None:
            unmatched.append(path.name)
            continue

        if _already_ingested(conn, path.name):
            print(f"[skipped] '{path.name}' — already ingested")
            summary[path.name] = {"status": "skipped", "message": "already ingested"}
            continue

        # Load file
        try:
            df = load_file(path)
        except Exception as exc:
            message = f"Could not load file: {exc}"
            print(f"[fail] '{path.name}': {message}")
            _log_ingest(conn, path.name, None, "failed", message=message)
            summary[path.name] = {"status": "failed", "message": message}
            continue

        # Structural checks
        issues = _structural_checks(df, source)
        if issues:
            message = "; ".join(issues)
            print(f"[fail] '{path.name}': {message}")
            _log_ingest(conn, path.name, source["target_table"], "failed", message=message)
            summary[path.name] = {"status": "failed", "message": message}
            continue

        table = source["target_table"]
        primary_key = source.get("primary_key")
        on_duplicate = source.get("on_duplicate", "flag" if primary_key else "append")

        # Adding the pipeline tracking column
        # `_` denotes a pipeline run column, not a real column in the table.
        df["_ingested_at"] = datetime.now(timezone.utc)

        # Load into DuckDB
        if mode == "replace" or not _table_exists(conn, table):
            conn.execute(f'DROP TABLE IF EXISTS "{table}"')
            conn.execute(f'CREATE TABLE "{table}" AS SELECT * FROM df')
            new_cols: list[str] = []
            flagged_count = 0
            skipped_count = 0
            print(f"[ok] '{path.name}' → '{table}' ({len(df)} rows, created)")
        else:
            new_cols = _auto_migrate(conn, table, df)

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
                flagged_count = 0
                skipped_count = 0

            if not df.empty:
                col_list = ", ".join(f'"{c}"' for c in df.columns)
                conn.execute(
                    f'INSERT INTO "{table}" ({col_list}) SELECT {col_list} FROM df'
                )

            print(
                f"[ok] '{path.name}' → '{table}' "
                f"({len(df)} inserted, {flagged_count} flagged, {skipped_count} skipped)"
            )

        _log_ingest(conn, path.name, table, "ok", rows=len(df), new_cols=new_cols)
        summary[path.name] = {
            "table": table,
            "rows": len(df),
            "new_cols": new_cols,
            "flagged": flagged_count,
            "skipped": skipped_count,
            "status": "ok"
        }

        # Optional validation checks
        if run_checks and check_registry and report_registry:
            _run_inline_checks(conn, table, source, check_registry, report_registry, path.name)

    if unmatched:
        print(f"[warn] No source match for: {', '.join(unmatched)}")
        for filename in unmatched:
            _log_ingest(conn, filename, None, "skipped", message="No matching source pattern")

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
def _already_ingested(
        conn: duckdb.DuckDBPyConnection,
        filename: str
) -> bool:
    """Return True if this filename was already successfully ingested.
    Only 'ok' status counts — failed files are retried on every run.
    """
    result = conn.execute("""
            SELECT count(*) FROM ingest_log
            WHERE filename = ? AND status = 'ok'
        """, [filename]).fetchone()
    return result[0] > 0  # type: ignore


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
        column in _get_existing_columns(conn, table) and not
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
        if _table_exists(conn, table_name):
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
