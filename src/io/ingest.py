"""
Ingestion module.
Scans a directory for CSV and Excel files, matches each file to a source
definition by pattern, and loads the data into DuckDB tables.

Table lifecycle:
- `db-init`  — creates the DuckDB file and bootstraps empty tables from sources_config.yaml
- First ingest of a new source — creates the table lazily from the file's schema
- Subsequent ingests — appends rows, auto-migrating new columns if the schema grew

Each ingest run logs results (including failures) to the `ingest_log` table
so failures are visible without stopping the entire run.
"""

import json
import uuid
import duckdb
import pandas as pd
from datetime import datetime, timezone
from pathlib import Path
from fnmatch import fnmatch


# ---------------------------------------------------------------------------
# Structural checks — lightweight, run per file before loading
# ---------------------------------------------------------------------------

def _structural_checks(df: pd.DataFrame, source: dict) -> list[str]:
    """
    Perform structural integrity checks on the provided DataFrame based on the source metadata.

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
    issues = []
    if df.empty:
        issues.append("File is empty")
    ts_col = source.get("timestamp_col")
    if ts_col and ts_col not in df.columns:
        issues.append(f"Missing required timestamp column '{ts_col}'")
    return issues


# ---------------------------------------------------------------------------
# ingest_log table
# ---------------------------------------------------------------------------

def _init_ingest_log(conn: duckdb.DuckDBPyConnection) -> None:
    """
    Initializes the ingest log table in the provided DuckDB connection. This table
    is used for logging details about data ingestion activities, such as the
    status, number of rows processed, and additional columns added during the
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

    :param conn: The DuckDB connection where the ingest log table should be
                 initialized.
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
    conn, filename, table_name, status, rows=None, new_cols=None, message=None
) -> None:
    """
    Logs ingestion details into the database.

    This function inserts information about a data ingestion process into
    an `ingest_log` database table. It keeps a record of filenames,
    table names, statuses, row counts, new columns, and optional messages
    related to the ingestion. Every log entry is timestamped with the
    current UTC time.

    :param conn: A database connection object used to execute the SQL
                 insert statement.
    :param filename: The name of the file being ingested.
    :param table_name: The name of the database table associated with the
                       ingestion.
    :param status: The status of the ingestion operation.
    :param rows: The count of rows ingested. Optional.
    :param new_cols: A list of new columns added during ingestion.
                     Optional.
    :param message: An additional message or comment about the ingestion
                    process. Optional.
    :return: None
    """
    conn.execute("""
        INSERT INTO ingest_log
            (id, filename, table_name, status, rows, new_cols, message, ingested_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """, [
        str(uuid.uuid4()), filename, table_name, status,
        rows,
        json.dumps(new_cols) if new_cols else None,
        message,
        datetime.now(timezone.utc),
    ])


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def resolve_source(filename: str, sources: list[dict]) -> dict | None:
    """
    Determines and returns the matching source from a list of sources based on filename patterns.

    This function iterates through a list of source dictionaries and checks if the
    provided filename matches any patterns specified in the sources. If a match is
    found, it returns the matching source dictionary. If no matches are found,
    it returns None. Pattern matching is performed using the `fnmatch` module.

    :param filename: The name of the file to resolve against the source patterns.
    :param sources: A list of dictionaries. Each dictionary should include a key
        "patterns", which contains a list of filename patterns.
    :return: The first matching source dictionary if a match is found, otherwise None.
    """
    for source in sources:
        if any(fnmatch(filename, p) for p in source["patterns"]):
            return source
    return None


def _load_file(path: Path) -> pd.DataFrame:
    """
    Loads a file from the given path and returns its content as a DataFrame.

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
    if path.suffix.lower() == ".csv":
        return pd.read_csv(path)
    elif path.suffix.lower() in (".xlsx", ".xls"):
        return pd.read_excel(path)
    else:
        raise ValueError(f"Unsupported file type: {path.suffix}")


def _table_exists(conn: duckdb.DuckDBPyConnection, table: str) -> bool:
    """
    Checks if a table exists in the given database connection.

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
    """
    Retrieve the existing column names of a specified table within a DuckDB database.

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
    """
    Automatically migrates the schema of an existing DuckDB table to align with the columns of a given
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

def init_db(db_path: str, sources: list[dict]) -> None:
    """
    Initializes a database and ensures the presence of target tables specified in the sources.

    This function creates the directory for the database file if it does not exist. It then establishes
    a connection to the database, initializes the ingest log, and iterates over the provided list of
    source definitions. For each source, if the target table does not already exist in the database,
    the function creates it.

    :param db_path: Path to the database file.
    :type db_path: str
    :param sources: A list of source definitions, each containing information about
        the target table to be created in the database.
    :type sources: list[dict]

    :return: None
    """
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    conn = duckdb.connect(db_path)
    _init_ingest_log(conn)

    for source in sources:
        table = source["target_table"]
        if _table_exists(conn, table):
            print(f"  [skip] Table '{table}' already exists")
            continue
        conn.execute(f'CREATE TABLE IF NOT EXISTS "{table}" (_placeholder VARCHAR)')
        print(f"  [ok]   Created table '{table}'")

    conn.close()


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
    """
    Processes and ingests data files from a specified directory into a database, performing
    optional structural checks, validation, and handling unmatched files. Each successfully
    processed file's data is inserted or appended into the associated table specified in the
    source definitions. Failed and unmatched files are appropriately logged.

    :param directory: The directory containing files to ingest.
    :param sources: A list of source definitions mapping file patterns to target database
        tables.
    :param db_path: Path to the DuckDB database file used for data storage and ingestion.
    :param mode: The ingestion mode, either "append" to add records to existing tables or
        "replace" to overwrite them.
    :param run_checks: A flag indicating whether to execute validation checks after ingestion.
    :param check_registry: Registry of available checks to validate ingested data, if enabled.
    :param report_registry: Registry used to log the results of executed checks, if enabled.
    :return: A dictionary summarizing the results of the ingestion process for each file.
    """
    conn = duckdb.connect(db_path)
    _init_ingest_log(conn)
    summary = {}
    unmatched = []

    for path in sorted(Path(directory).iterdir()):
        if path.suffix.lower() not in (".csv", ".xlsx", ".xls"):
            continue

        source = resolve_source(path.name, sources)
        if source is None:
            unmatched.append(path.name)
            continue

        # Load file
        try:
            df = _load_file(path)
        except Exception as e:
            msg = f"Could not load file: {e}"
            print(f"  [fail] '{path.name}': {msg}")
            _log_ingest(conn, path.name, None, "failed", message=msg)
            summary[path.name] = {"status": "failed", "message": msg}
            continue

        # Structural checks
        issues = _structural_checks(df, source)
        if issues:
            msg = "; ".join(issues)
            print(f"  [fail] '{path.name}': {msg}")
            _log_ingest(conn, path.name, source["target_table"], "failed", message=msg)
            summary[path.name] = {"status": "failed", "message": msg}
            continue

        table = source["target_table"]

        # Load into DuckDB
        if mode == "replace" or not _table_exists(conn, table):
            conn.execute(f'DROP TABLE IF EXISTS "{table}"')
            conn.execute(f'CREATE TABLE "{table}" AS SELECT * FROM df')
            new_cols = []
            print(f"  [ok] '{path.name}' → '{table}' ({len(df)} rows, created)")
        else:
            new_cols = _auto_migrate(conn, table, df)
            conn.execute(f'INSERT INTO "{table}" SELECT * FROM df')
            print(f"  [ok] '{path.name}' → '{table}' ({len(df)} rows appended)")

        _log_ingest(conn, path.name, table, "ok", rows=len(df), new_cols=new_cols)
        summary[path.name] = {"table": table, "rows": len(df), "new_cols": new_cols, "status": "ok"}

        # Optional validation checks
        if run_checks and check_registry and report_registry:
            _run_inline_checks(conn, table, source, check_registry, report_registry, path.name)

    if unmatched:
        print(f"  [warn] No source match for: {', '.join(unmatched)}")
        for f in unmatched:
            _log_ingest(conn, f, None, "skipped", message="No matching source pattern")

    conn.close()
    return summary


def _run_inline_checks(conn, table, source, check_registry, report_registry, filename):
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
