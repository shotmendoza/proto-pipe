"""Query builder.

Translates filter definitions from deliverables_config.yaml into
parameterised DuckDB SQL. Keeps all SQL construction in one place
so the CLI and runner never build queries by hand.

Two query paths:
  1. sql_file — load and execute a .sql file directly against the connection.
                 Date logic lives in the SQL itself (DuckDB functions).
                 YAML filters and CLI overrides are ignored.
  2. filters — auto-build SELECT * WHERE from date_filters / field_filters.
                 Supports dynamic date tokens (e.g. "end_of_last_month").
                 CLI overrides can replace per-column filter values at runtime.
"""
import calendar
import re
from datetime import datetime, timezone, date, timedelta
from pathlib import Path

import duckdb
import pandas as pd


def build_filter_clause(filters: dict) -> tuple[str, list]:
    """Build a WHERE clause and params list from a filters dict.

    filters shape:
        date_filters:
          - col: "order_date"
            from: "2026-01-01" # inclusive, optional
            to: "2026-03-31" # inclusive, optional
        field_filters:
          - col: "region"
            values: ["EMEA", "APAC"]

    Returns (clause_str, params_list) ready for duckdb.execute(sql, params).
    If no filters, returns ("", []).
    """
    filters = resolve_filter_dates(filters)

    clauses = []
    params = []

    for f in filters.get("date_filters", []):
        col = f["col"]
        if "from" in f:
            clauses.append(f'"{col}" >= ?')
            params.append(str(f["from"]))
        if "to" in f:
            clauses.append(f'"{col}" <= ?')
            params.append(str(f["to"]))

    for f in filters.get("field_filters", []):
        col = f["col"]
        values = f["values"]
        placeholders = ", ".join(["?"] * len(values))
        clauses.append(f'"{col}" IN ({placeholders})')
        params.extend(values)

    if not clauses:
        return "", []

    return "WHERE " + " AND ".join(clauses), params


def query_table(
    conn: duckdb.DuckDBPyConnection,
    table: str | None = None,
    filters: dict | None = None,
    cli_overrides: dict | None = None,
    sql_file: str | None = None,
) -> pd.DataFrame:
    """Query a DuckDB table or execute a sql_file, returning a DataFrame.

    If sql_file is provided:
        - Execute the SQL file directly. filters and cli_overrides are ignored.
        - Date logic lives inside the SQL file itself.

    If no sql_file:
        - Build SELECT * WHERE from filters + cli_overrides.
        - Dynamic date tokens in filter values are resolved at call time.
        - Missing or empty filters return all rows.

    cli_overrides shape (mirrors filters, merged on top of config filters):
        {
            "date_filters": [{"col": "order_date", "from": "2026-03-01"}],
            "field_filters": [{"col": "region", "values": ["EMEA"]}]
        }
    """
    # Happy Path, executes a Sql File if one is available, skipping the filters
    if sql_file:
        return execute_sql_file(conn, sql_file)

    merged_filters = {}

    if filters:
        merged_filters["date_filters"]  = list(filters.get("date_filters", []))
        merged_filters["field_filters"] = list(filters.get("field_filters", []))

    if cli_overrides:
        # CLI overrides replace config filters for the same column
        _merge_filters(merged_filters, cli_overrides)

    where_clause, params = build_filter_clause(merged_filters)
    sql = f'SELECT * FROM "{table}" {where_clause}'.strip()

    return conn.execute(sql, params).df()


# ---------------------------------------------------------------------------
# Filter merge helper
# ---------------------------------------------------------------------------
def _merge_filters(base: dict, overrides: dict) -> None:
    """
    Merge override filters into base, replacing any entry with the same col.
    Mutates base in place.
    """
    for filter_type in ("date_filters", "field_filters"):
        if filter_type not in overrides:
            continue
        base.setdefault(filter_type, [])
        override_cols = {f["col"] for f in overrides[filter_type]}
        # Remove base entries for cols that are being overridden
        base[filter_type] = [
            f for f in base[filter_type] if f["col"] not in override_cols
        ]
        base[filter_type].extend(overrides[filter_type])


def _log_run(
    conn,
        deliverable_name,
        report_name,
        filename,
        output_dir,
        filters,
        row_count,
        fmt,
        run_date
) -> None:
    """Logs the execution details of a report run into a database table named
    'report_runs'. This function tracks information such as deliverable name,
    report name, the filename of the output, output directory, filters applied,
    row count, file format, and the date of execution. It utilizes database
    connection to insert the data into the appropriate table.

    :param conn: The database connection object used to execute the query.
    :param deliverable_name: Name of the deliverable associated with the report.
    :param report_name: The name of the report being logged.
    :param filename: The name of the file generated by the report run.
    :param output_dir: The output directory where the file is stored.
    :param filters: A dictionary or list representing filters applied during
        the report generation. This is serialized to JSON during logging.
    :param row_count: The number of rows in the generated report.
    :param fmt: The file format of the generated report, for example, CSV or PDF.
    :param run_date: The date and time of the report run execution.

    :return: None
    """
    import json
    import uuid

    conn.execute("""
        INSERT INTO report_runs
            (id, deliverable_name, report_name, filename, output_dir,
             filters_applied, row_count, format, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, [
        str(uuid.uuid4()),
        deliverable_name,
        report_name,
        filename,
        output_dir,
        json.dumps(filters) if filters else None,
        row_count,
        fmt,
        datetime.now(timezone.utc),
    ])


def init_report_runs_table(conn: duckdb.DuckDBPyConnection) -> None:
    """Initializes the `report_runs` table within the specified DuckDB connection. This function creates
    the table if it does not exist, ensuring the schema includes various columns to track report runs,
    such as deliverable details, filename, output directory, applied filters, and creation timestamp.

    :param conn: The active DuckDB connection object where the statement will be executed.
    :type conn: duckdb.DuckDBPyConnection
    :return: None
    :rtype: None
    """
    conn.execute("""
        CREATE TABLE IF NOT EXISTS report_runs (
            id               VARCHAR PRIMARY KEY,
            deliverable_name VARCHAR NOT NULL,
            report_name      VARCHAR NOT NULL,
            filename         VARCHAR NOT NULL,
            output_dir       VARCHAR NOT NULL,
            filters_applied  VARCHAR,           -- JSON string of filters used
            row_count        INTEGER,
            format           VARCHAR,
            created_at       TIMESTAMPTZ NOT NULL
        )
    """)


# ---------------------------------------------------------------------------
# Dynamic date token resolution
# ---------------------------------------------------------------------------
def resolve_date_token(value: str) -> str:
    """Resolve a dynamic date token to a YYYY-MM-DD string.
    If the value is not a recognized token, return it unchanged
    (so hardcoded dates like "2026-01-01" pass through safely).

    Supported tokens:
        today
        start_of_month / end_of_month
        start_of_last_month / end_of_last_month
        start_of_quarter / end_of_quarter
        start_of_last_quarter / end_of_last_quarter
        today-Nd (e.g. today-7d, today-30d)
        today+Nd (e.g. today+7d)
    """
    if not isinstance(value, str):
        return value

    today = date.today()
    v = value.strip().lower()

    if v == "today":
        return today.isoformat()

    if v == "start_of_month":
        return today.replace(day=1).isoformat()

    if v == "end_of_month":
        last_day = calendar.monthrange(today.year, today.month)[1]
        return today.replace(day=last_day).isoformat()

    if v == "start_of_last_month":
        first_of_this = today.replace(day=1)
        last_month = first_of_this - timedelta(days=1)
        return last_month.replace(day=1).isoformat()

    if v == "end_of_last_month":
        return (today.replace(day=1) - timedelta(days=1)).isoformat()

    if v == "start_of_quarter":
        quarter_start_month = ((today.month - 1) // 3) * 3 + 1
        return today.replace(month=quarter_start_month, day=1).isoformat()

    if v == "end_of_quarter":
        quarter_end_month = ((today.month - 1) // 3) * 3 + 3
        last_day = calendar.monthrange(today.year, quarter_end_month)[1]
        return today.replace(month=quarter_end_month, day=last_day).isoformat()

    if v == "start_of_last_quarter":
        current_q_start = ((today.month - 1) // 3) * 3 + 1
        first_of_current_q = today.replace(month=current_q_start, day=1)
        last_q_end = first_of_current_q - timedelta(days=1)
        last_q_start_month = ((last_q_end.month - 1) // 3) * 3 + 1
        return last_q_end.replace(month=last_q_start_month, day=1).isoformat()

    if v == "end_of_last_quarter":
        current_q_start = ((today.month - 1) // 3) * 3 + 1
        first_of_current_q = today.replace(month=current_q_start, day=1)
        return (first_of_current_q - timedelta(days=1)).isoformat()

    # today±Nd
    if v.startswith("today"):
        m = re.fullmatch(r"today([+-])(\d+)d", v)
        if m:
            sign, days = m.group(1), int(m.group(2))
            delta = timedelta(days=days)
            result = today + delta if sign == "+" else today - delta
            return result.isoformat()

    # Not a token — return as-is (hardcoded date string or unrecognised value)
    return value


def resolve_filter_dates(filters: dict) -> dict:
    """
    Return a copy of filters with all date token strings resolved to
    YYYY-MM-DD. Non-date filters are passed through unchanged.
    """
    resolved = dict(filters)
    resolved["date_filters"] = [
        {
            **f,
            **({} if "from" not in f else {"from": resolve_date_token(f["from"])}),
            **({} if "to"   not in f else {"to":   resolve_date_token(f["to"])}),
        }
        for f in filters.get("date_filters", [])
    ]
    return resolved


# ---------------------------------------------------------------------------
# sql_file execution
# ---------------------------------------------------------------------------
# Matches a statement separator NOT inside a string literal.
# Simple approach: strip trailing semicolons from the whole SQL, then
# check that no mid-statement semicolons remain (which would indicate
# multiple statements).
_SEMICOLON_RE = re.compile(r";")


def execute_sql_file(
    conn: duckdb.DuckDBPyConnection,
    sql_file: str,
) -> pd.DataFrame:
    """Load a .sql file and execute it against the connection.

    Validates that the file contains exactly one SELECT statement:
    - Trailing semicolons are stripped automatically.
    - A semicolon anywhere else in the file raises ValueError, because
      it almost certainly means two statements were written (e.g. a SET
      followed by a SELECT), which DuckDB will reject with a cryptic error.

    Date logic is written directly in the SQL using DuckDB functions.
    """
    path = Path(sql_file)
    if not path.exists():
        raise FileNotFoundError(f"SQL file not found: {sql_file}")

    sql = path.read_text().strip()
    if not sql:
        raise ValueError(f"SQL file is empty: {sql_file}")

    # Strip a single trailing semicolon if present
    if sql.endswith(";"):
        sql = sql[:-1].rstrip()

    # If a semicolon still remains, there are multiple statements
    if ";" in sql:
        raise ValueError(
            f"SQL file contains multiple statements (found ';' after stripping "
            f"trailing semicolon): {sql_file}\n"
            f"Each sql_file must contain exactly one SELECT statement."
        )

    return conn.execute(sql).df()
