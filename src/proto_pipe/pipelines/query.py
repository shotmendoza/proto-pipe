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


def log_run(
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


def query_pipeline_events(
    conn,
    severity: str | None,
    since: str | None,
    order_desc: bool = True,
) -> "pd.DataFrame":
    """Query pipeline_events with optional severity and since filters.

    :param since: Date string in YYYY-MM-DD format.
    :param severity: Severity level filter.
    :param conn: Database connection.
    :param order_desc: True = most recent first (view use case).
                       False = chronological (export/archive use case).
    :raises ValueError: if since is not in YYYY-MM-DD format.
    """
    import pandas as pd

    query = "SELECT * FROM pipeline_events WHERE 1=1"
    params: list = []

    if severity:
        query += " AND severity = ?"
        params.append(severity)

    if since:
        since_dt = datetime.strptime(since, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        query += " AND occurred_at >= ?"
        params.append(since_dt)

    query += " ORDER BY occurred_at " + ("DESC" if order_desc else "ASC")
    return conn.execute(query, params).df()


def _safe_count(conn, query: str, params: list | None = None) -> int:
    """Execute a COUNT query, returning 0 if the table does not exist.

    Used for impact summaries before destructive operations — a missing
    table (fresh DB, pre-migration) should show 0, not raise an error.
    """
    try:
        return conn.execute(query, params or []).fetchone()[0]
    except Exception:
        return 0


def query_delete_source_impact(
    conn, table_name: str
) -> list[tuple[str, int, str]]:
    """Return row counts for all tables affected by vp delete source.

    Returns a list of (label, count, unit) tuples ready for
    prompt_delete_impact. Missing tables return 0, not an error.
    """
    return [
        (
            f"table '{table_name}'",
            _safe_count(conn, f'SELECT count(*) FROM "{table_name}"'),
            "rows",
        ),
        (
            "ingest_state",
            _safe_count(
                conn,
                "SELECT count(*) FROM ingest_state WHERE table_name = ?",
                [table_name],
            ),
            "entries",
        ),
        (
            "source_block",
            _safe_count(
                conn,
                "SELECT count(*) FROM source_block WHERE table_name = ?",
                [table_name],
            ),
            "open flags",
        ),
        (
            "source_pass",
            _safe_count(
                conn,
                "SELECT count(*) FROM source_pass WHERE table_name = ?",
                [table_name],
            ),
            "entries",
        ),
    ]


def query_delete_report_impact(
    conn, report_name: str, target_table: str
) -> list[tuple[str, int, str]]:
    """Return row counts for all tables affected by vp delete report.

    Returns a list of (label, count, unit) tuples ready for
    prompt_delete_impact. Missing tables return 0, not an error.
    """
    return [
        (
            f"table '{target_table}'",
            _safe_count(conn, f'SELECT count(*) FROM "{target_table}"'),
            "rows",
        ),
        (
            "validation_block",
            _safe_count(
                conn,
                "SELECT count(*) FROM validation_block WHERE report_name = ?",
                [report_name],
            ),
            "entries",
        ),
        (
            "validation_pass",
            _safe_count(
                conn,
                "SELECT count(*) FROM validation_pass WHERE report_name = ?",
                [report_name],
            ),
            "entries",
        ),
    ]


def query_delete_table_impact(
    conn, table_name: str
) -> list[tuple[str, int, str]]:
    """Return row count for a table affected by vp delete table.

    Returns a list of (label, count, unit) tuples ready for
    prompt_delete_impact. Missing table returns 0, not an error.
    """
    return [
        (
            f"table '{table_name}'",
            _safe_count(conn, f'SELECT count(*) FROM "{table_name}"'),
            "rows",
        ),
    ]


# ---------------------------------------------------------------------------
# Append to pipelines/query.py
# Query functions for vp status and vp errors commands.
# ---------------------------------------------------------------------------

from dataclasses import dataclass, field


# ---------------------------------------------------------------------------
# Structured return types (Rule 19)
# ---------------------------------------------------------------------------

@dataclass
class ErrorOverview:
    """Counts for bare `vp errors` summary."""
    source_count: int
    source_table_count: int
    file_failure_count: int
    file_failure_table_count: int
    report_count: int
    report_name_count: int


@dataclass
class PipelineHealth:
    """DB-derived health data for bare `vp status`."""
    source_tables: list = field(default_factory=list)
    source_error_count: int = 0
    report_names: list = field(default_factory=list)
    validation_failure_count: int = 0


@dataclass
class SourceStatus:
    """One-line summary for a source table."""
    table_name: str
    file_count: int
    total_rows: int
    last_ingest: str | None
    error_count: int


@dataclass
class SourceDetail:
    """Detail view for one source table."""
    name: str
    row_count: int | None
    error_count: int
    history: list = field(default_factory=list)   # [{filename, status, rows, ingested_at}]
    last_failure_message: str | None = None        # most recent ingest_state failure


@dataclass
class ReportStatus:
    """One-line summary for a report."""
    report_name: str
    record_count: int
    last_validated: str | None
    failure_count: int


@dataclass
class ReportDetail:
    """Detail view for one report."""
    name: str
    row_count: int | None
    failure_count: int
    last_validated: str | None
    checks: list = field(default_factory=list)   # [(check_name, status, count)]


# ---------------------------------------------------------------------------
# vp errors queries
# ---------------------------------------------------------------------------

# CTE that filters ingest_state to only the most recent attempt per file.
# Used by error queries to show current state, not historical accumulation.
# A file that failed 3 times then succeeded shows 0 failures, not 3.
_LATEST_FILE_STATUS_CTE = """
    WITH _latest_file_status AS (
        SELECT *,
               ROW_NUMBER() OVER (
                   PARTITION BY filename ORDER BY ingested_at DESC
               ) AS _rn
        FROM ingest_state
    )
"""

def query_error_overview(conn) -> ErrorOverview:
    """Counts for the bare `vp errors` summary display."""
    return ErrorOverview(
        source_count=_safe_count(conn, "SELECT count(*) FROM source_block"),
        source_table_count=_safe_count(
            conn, "SELECT count(DISTINCT table_name) FROM source_block"
        ),
        file_failure_count=_safe_count(
            conn,
            _LATEST_FILE_STATUS_CTE
            + "SELECT count(*) FROM _latest_file_status "
            "WHERE _rn = 1 AND status = 'failed'"
        ),
        file_failure_table_count=_safe_count(
            conn,
            _LATEST_FILE_STATUS_CTE
            + "SELECT count(DISTINCT table_name) FROM _latest_file_status "
            "WHERE _rn = 1 AND status = 'failed'"
        ),
        report_count=_safe_count(conn, "SELECT count(*) FROM validation_block"),
        report_name_count=_safe_count(
            conn, "SELECT count(DISTINCT report_name) FROM validation_block"
        ),
    )


def query_error_groups(
    conn, stage: str, name: str | None = None
) -> dict[str, list[tuple[str, int]]]:
    """Error rows grouped by scope and cause for prescriptive display.

    When check_registry_metadata has a func_name column (post-migration),
    LEFT JOINs to resolve UUIDs to human-readable function names.
    Falls back to raw check_name on pre-migration databases.

    Args:
        stage: "source" or "report".
        name: optional table_name (source) or report_name (report) filter.

    Returns:
        dict mapping scope_name → [(display_name, count), ...].
    """
    from proto_pipe.io.db import column_exists

    if stage == "source":
        flag_table = "source_block"
        group_col = "table_name"
    else:
        flag_table = "validation_block"
        group_col = "report_name"

    has_func_name = column_exists(conn, "check_registry_metadata", "func_name")

    where = ""
    params: list = []
    if name:
        where_col = f"f.{group_col}" if has_func_name else group_col
        where = f"WHERE {where_col} = ?"
        params = [name]

    if has_func_name:
        rows = conn.execute(f"""
            SELECT
                f.{group_col},
                COALESCE(m.func_name, f.check_name) AS display_name,
                count(*) AS cnt
            FROM {flag_table} f
            LEFT JOIN check_registry_metadata m
                ON m.check_name = f.check_name
            {where}
            GROUP BY f.{group_col}, display_name
            ORDER BY f.{group_col}, display_name
        """, params).fetchall()
    else:
        rows = conn.execute(f"""
            SELECT {group_col}, check_name, count(*) AS cnt
            FROM {flag_table}
            {where}
            GROUP BY {group_col}, check_name
            ORDER BY {group_col}, check_name
        """, params).fetchall()

    from collections import defaultdict
    by_scope: dict[str, list[tuple[str, int]]] = defaultdict(list)
    for scope_name, check_display, cnt in rows:
        by_scope[scope_name].append((check_display, cnt))
    return dict(by_scope)


@dataclass
class FileFailure:
    """One file-level ingest failure from ingest_state."""
    table_name: str
    filename: str
    message: str
    ingested_at: str | None


def query_file_failures(
    conn, name: str | None = None
) -> list[FileFailure]:
    """File-level ingest failures from ingest_state.

    These are file-level errors (unknown columns, bad type declarations)
    that never reach source_block — they fail the entire file.

    Only includes files whose most recent ingest attempt is 'failed'.
    Files that were re-ingested successfully are excluded.

    Args:
        name: optional table_name filter.

    Returns:
        list of FileFailure, most recent first.
    """
    where_extra = ""
    params: list = []
    if name:
        where_extra = " AND table_name = ?"
        params.append(name)

    try:
        rows = conn.execute(
            _LATEST_FILE_STATUS_CTE
            + "SELECT table_name, filename, message, ingested_at "
            "FROM _latest_file_status "
            f"WHERE _rn = 1 AND status = 'failed'{where_extra} "
            "ORDER BY ingested_at DESC",
            params,
        ).fetchall()
    except Exception:
        return []

    return [
        FileFailure(
            table_name=r[0] or "",
            filename=r[1],
            message=r[2] or "unknown error",
            ingested_at=str(r[3])[:10] if r[3] else None,
        )
        for r in rows
    ]


@dataclass
class SourceErrorSummary:
    """One-line summary for vp errors source (no name)."""
    table_name: str
    blocked_count: int
    file_failure_count: int


@dataclass
class ReportErrorSummary:
    """One-line summary for vp errors report (no name)."""
    report_name: str
    failure_count: int


def query_source_error_summary(conn) -> list[SourceErrorSummary]:
    """One row per source table with error counts for list view.

    Merges source_block row counts and ingest_state file failure counts.
    File failures only count files whose most recent attempt is 'failed'.
    """
    blocked: dict[str, int] = {}
    try:
        for name, cnt in conn.execute(
            "SELECT table_name, count(*) FROM source_block GROUP BY table_name"
        ).fetchall():
            blocked[name] = cnt
    except Exception:
        pass

    file_fails: dict[str, int] = {}
    try:
        for name, cnt in conn.execute(
            _LATEST_FILE_STATUS_CTE
            + "SELECT table_name, count(*) FROM _latest_file_status "
            "WHERE _rn = 1 AND status = 'failed' GROUP BY table_name"
        ).fetchall():
            file_fails[name or "(unknown)"] = cnt
    except Exception:
        pass

    all_names = sorted(set(blocked) | set(file_fails))
    return [
        SourceErrorSummary(
            table_name=n,
            blocked_count=blocked.get(n, 0),
            file_failure_count=file_fails.get(n, 0),
        )
        for n in all_names
    ]


def query_report_error_summary(conn) -> list[ReportErrorSummary]:
    """One row per report with failure counts for list view."""
    try:
        rows = conn.execute(
            "SELECT report_name, count(*) FROM validation_block "
            "GROUP BY report_name ORDER BY report_name"
        ).fetchall()
    except Exception:
        return []

    return [ReportErrorSummary(report_name=n, failure_count=c) for n, c in rows]


# ---------------------------------------------------------------------------
# vp status queries
# ---------------------------------------------------------------------------

def query_pipeline_health(conn) -> PipelineHealth:
    """DB-derived health data for the bare `vp status` display."""
    health = PipelineHealth()

    try:
        health.source_tables = conn.execute(
            "SELECT DISTINCT table_name FROM ingest_state WHERE status = 'ok'"
        ).df()["table_name"].tolist()
    except Exception:
        pass

    health.source_error_count = _safe_count(
        conn, "SELECT count(*) FROM source_block"
    )

    try:
        health.report_names = conn.execute(
            "SELECT DISTINCT report_name FROM validation_pass"
        ).df()["report_name"].tolist()
    except Exception:
        pass

    health.validation_failure_count = _safe_count(
        conn, "SELECT count(*) FROM validation_block"
    )

    return health


def query_source_statuses(conn) -> list[SourceStatus]:
    """One-line summary per source table for `vp status source`."""
    try:
        df = conn.execute("""
            SELECT
                table_name,
                count(*) AS file_count,
                sum(rows) AS total_rows,
                max(ingested_at) AS last_ingest
            FROM ingest_state
            WHERE status = 'ok'
            GROUP BY table_name
            ORDER BY table_name
        """).df()
    except Exception:
        return []

    # Error counts per table
    try:
        errors = dict(conn.execute("""
            SELECT table_name, count(*) FROM source_block
            GROUP BY table_name
        """).fetchall())
    except Exception:
        errors = {}

    return [
        SourceStatus(
            table_name=row["table_name"],
            file_count=int(row["file_count"]),
            total_rows=int(row["total_rows"]) if pd.notna(row["total_rows"]) else 0,
            last_ingest=str(row["last_ingest"])[:10] if pd.notna(row["last_ingest"]) else None,
            error_count=errors.get(row["table_name"], 0),
        )
        for _, row in df.iterrows()
    ]


def query_source_detail(conn, name: str) -> SourceDetail:
    """Detailed data for one source table."""
    from proto_pipe.io.db import table_exists

    if table_exists(conn, name):
        row_count = _safe_count(conn, f'SELECT count(*) FROM "{name}"')
    else:
        row_count = None

    error_count = _safe_count(
        conn, "SELECT count(*) FROM source_block WHERE table_name = ?", [name]
    )

    history = []
    try:
        hdf = conn.execute("""
            SELECT filename, status, rows, ingested_at
            FROM ingest_state
            WHERE table_name = ?
            ORDER BY ingested_at DESC
            LIMIT 10
        """, [name]).df()
        for _, row in hdf.iterrows():
            history.append({
                "filename": row["filename"],
                "status": row["status"],
                "rows": int(row["rows"]) if pd.notna(row["rows"]) else None,
                "ingested_at": str(row["ingested_at"])[:10] if pd.notna(row["ingested_at"]) else None,
            })
    except Exception:
        pass

    # Most recent failure message from ingest_state (for prescriptive output
    # when the table doesn't exist yet).
    last_failure_message = None
    if row_count is None:
        try:
            msg_row = conn.execute("""
                SELECT message FROM ingest_state
                WHERE table_name = ? AND status = 'failed'
                ORDER BY ingested_at DESC LIMIT 1
            """, [name]).fetchone()
            if msg_row and msg_row[0]:
                last_failure_message = msg_row[0]
        except Exception:
            pass

    return SourceDetail(
        name=name,
        row_count=row_count,
        error_count=error_count,
        history=history,
        last_failure_message=last_failure_message,
    )


def query_report_statuses(conn) -> list[ReportStatus]:
    """One-line summary per report for `vp status report`."""
    try:
        df = conn.execute("""
            SELECT
                report_name,
                count(*) AS record_count,
                max(validated_at) AS last_validated
            FROM validation_pass
            GROUP BY report_name
            ORDER BY report_name
        """).df()
    except Exception:
        return []

    try:
        failures = dict(conn.execute("""
            SELECT report_name, count(*) FROM validation_block
            GROUP BY report_name
        """).fetchall())
    except Exception:
        failures = {}

    return [
        ReportStatus(
            report_name=row["report_name"],
            record_count=int(row["record_count"]),
            last_validated=str(row["last_validated"])[:10] if row["last_validated"] else None,
            failure_count=failures.get(row["report_name"], 0),
        )
        for _, row in df.iterrows()
    ]


def query_report_detail(conn, name: str) -> ReportDetail:
    """Detailed data for one report."""
    from proto_pipe.io.db import column_exists

    row_count = _safe_count(conn, f'SELECT count(*) FROM "{name}"')
    failure_count = _safe_count(
        conn, "SELECT count(*) FROM validation_block WHERE report_name = ?", [name]
    )

    last_validated = None
    try:
        val = conn.execute(
            "SELECT max(validated_at) FROM validation_pass WHERE report_name = ?",
            [name],
        ).fetchone()[0]
        last_validated = str(val)[:10] if val else None
    except Exception:
        pass

    has_func_name = column_exists(conn, "check_registry_metadata", "func_name")

    checks = []
    try:
        if has_func_name:
            checks = conn.execute("""
                SELECT
                    COALESCE(m.func_name, vp.check_name) AS display_name,
                    vp.status,
                    count(*) AS cnt
                FROM validation_pass vp
                LEFT JOIN check_registry_metadata m
                    ON m.check_name = vp.check_name
                WHERE vp.report_name = ?
                GROUP BY display_name, vp.status
                ORDER BY display_name, vp.status
            """, [name]).fetchall()
        else:
            checks = conn.execute("""
                SELECT check_name, status, count(*) AS cnt
                FROM validation_pass
                WHERE report_name = ?
                GROUP BY check_name, status
                ORDER BY check_name, status
            """, [name]).fetchall()
    except Exception:
        pass

    return ReportDetail(
        name=name,
        row_count=row_count if row_count else None,
        failure_count=failure_count,
        last_validated=last_validated,
        checks=list(checks),
    )


# ---------------------------------------------------------------------------
# Table-with-status query  (moved from cli/commands/view.py, Rule 16/18)
# ---------------------------------------------------------------------------

def query_table_with_status(
    conn: duckdb.DuckDBPyConnection,
    table: str,
    pk_col: str | None,
    limit: int,
) -> pd.DataFrame:
    """Fetch table rows with a _status column (flagged / ingested).

    Joins to source_block via md5 identity when a primary key is available.
    Falls back to plain SELECT when no primary key is defined.
    """
    try:
        if pk_col:
            df = conn.execute(f"""
                SELECT
                    CASE WHEN f.id IS NOT NULL THEN 'flagged'
                         ELSE 'ingested'
                    END AS _status,
                    s.*
                FROM "{table}" s
                LEFT JOIN source_block f
                    ON md5(CAST(s."{pk_col}" AS VARCHAR)) = f.id
                    AND f.table_name = '{table}'
                LIMIT {limit}
            """).df()
        else:
            df = conn.execute(
                f'SELECT * FROM "{table}" LIMIT {limit}'
            ).df()
    except Exception:
        df = conn.execute(f'SELECT * FROM "{table}" LIMIT {limit}').df()
    return df


# ---------------------------------------------------------------------------
# Block count query  (for run_all flag gating, Rule 16)
# ---------------------------------------------------------------------------

def query_block_count(conn, stage: str) -> int:
    """Return count of rows in source_block or validation_block.

    stage: "source" or "report".
    Returns 0 if the table does not exist.
    """
    table = "source_block" if stage == "source" else "validation_block"
    return _safe_count(conn, f"SELECT count(*) FROM {table}")


def query_validation_report_names(conn) -> list[str]:
    """Return distinct report names with validation failures, sorted."""
    return conn.execute(
        "SELECT DISTINCT report_name FROM validation_block ORDER BY report_name"
    ).df()["report_name"].tolist()


def query_validation_detail_fallback(
    conn,
    report_name: str,
) -> pd.DataFrame:
    """Return validation_block rows for a report, formatted for export.

    Used as a fallback when pk_col is not available and
    build_validation_flag_export cannot be called.
    """
    return conn.execute("""
        SELECT
            id          AS _flag_id,
            report_name,
            check_name  AS _flag_check,
            pk_value,
            bad_columns AS _flag_columns,
            reason      AS _flag_reason,
            flagged_at
        FROM validation_block
        WHERE report_name = ?
        ORDER BY flagged_at DESC
    """, [report_name]).df()


def query_validation_summary(
    conn,
    report_name: str | None = None,
) -> pd.DataFrame:
    """Return failure counts grouped by report + check for export summary sheet.

    Resolves check UUIDs to function names via check_registry_metadata
    when the func_name column exists (post-migration). Falls back to raw
    check_name on pre-migration databases.
    """
    from proto_pipe.io.db import column_exists

    has_func_name = column_exists(conn, "check_registry_metadata", "func_name")

    where = ""
    params: list = []
    if report_name:
        where_col = "vb.report_name" if has_func_name else "report_name"
        where = f"WHERE {where_col} = ?"
        params = [report_name]

    if has_func_name:
        return conn.execute(f"""
            SELECT
                vb.report_name,
                COALESCE(m.func_name, vb.check_name) AS check_name,
                count(*) AS failure_count,
                min(vb.flagged_at) AS first_flagged,
                max(vb.flagged_at) AS last_flagged
            FROM validation_block vb
            LEFT JOIN check_registry_metadata m
                ON m.check_name = vb.check_name
            {where}
            GROUP BY vb.report_name, COALESCE(m.func_name, vb.check_name)
            ORDER BY vb.report_name, check_name
        """, params).df()
    else:
        return conn.execute(f"""
            SELECT
                report_name,
                check_name,
                count(*) AS failure_count,
                min(flagged_at) AS first_flagged,
                max(flagged_at) AS last_flagged
            FROM validation_block
            {where}
            GROUP BY report_name, check_name
            ORDER BY report_name, check_name
        """, params).df()


# ---------------------------------------------------------------------------
# Lineage graph  (moved from cli/commands/view.py, Rule 16)
# ---------------------------------------------------------------------------

@dataclass
class LineageGraph:
    """Dependency graph structures for vp view lineage rendering."""
    target_to_report: dict      # report target_table → report name
    table_to_source: dict       # source target_table → source config dict
    report_by_name: dict        # report name → report config dict
    report_dependents: dict     # report name → [downstream report names]
    report_upstream: dict       # report name → upstream report name or None


def build_lineage_graph(
    sources: list[dict],
    reports: list[dict],
    get_target_table,
) -> LineageGraph:
    """Build dependency graph from config data.

    get_target_table: callable that takes a report dict and returns
    its target table name (imported from reports.runner).
    """
    target_to_report = {get_target_table(r): r["name"] for r in reports}
    table_to_source = {s["target_table"]: s for s in sources}
    report_by_name = {r["name"]: r for r in reports}

    report_dependents: dict[str, list[str]] = {r["name"]: [] for r in reports}
    report_upstream: dict[str, str | None] = {}

    for r in reports:
        src_table = r.get("source", {}).get("table", "")
        upstream_report = target_to_report.get(src_table)
        if upstream_report and upstream_report != r["name"]:
            report_upstream[r["name"]] = upstream_report
            report_dependents[upstream_report].append(r["name"])
        else:
            report_upstream[r["name"]] = None

    return LineageGraph(
        target_to_report=target_to_report,
        table_to_source=table_to_source,
        report_by_name=report_by_name,
        report_dependents=report_dependents,
        report_upstream=report_upstream,
    )


def resolve_report_root_table(graph: LineageGraph, report_name: str) -> str:
    """Walk upstream through the lineage graph to find the root source table."""
    visited: set[str] = set()
    current = report_name
    while True:
        if current in visited:
            break
        visited.add(current)
        up = graph.report_upstream.get(current)
        if up is None:
            return graph.report_by_name[current].get("source", {}).get("table", "")
        current = up
    return ""


def query_lineage_timestamps(
    conn,
) -> tuple[dict[str, str], dict[str, str], dict[str, str]]:
    """Query last-run timestamps for lineage display.

    Returns (last_ingested, last_validated, last_produced) dicts
    mapping name → YYYY-MM-DD string. Missing entries not included.
    """
    def _fmt(ts) -> str:
        if ts is None:
            return "never"
        try:
            return str(ts)[:10]
        except Exception:
            return str(ts)

    last_ingested: dict[str, str] = {}
    last_validated: dict[str, str] = {}
    last_produced: dict[str, str] = {}

    try:
        rows = conn.execute(
            "SELECT table_name, max(ingested_at) FROM ingest_state GROUP BY table_name"
        ).fetchall()
        last_ingested = {r[0]: _fmt(r[1]) for r in rows}
    except Exception:
        pass

    try:
        rows = conn.execute(
            "SELECT report_name, max(validated_at) FROM validation_pass GROUP BY report_name"
        ).fetchall()
        last_validated = {r[0]: _fmt(r[1]) for r in rows}
    except Exception:
        pass

    try:
        rows = conn.execute(
            "SELECT deliverable_name, max(created_at) FROM report_runs GROUP BY deliverable_name"
        ).fetchall()
        last_produced = {r[0]: _fmt(r[1]) for r in rows}
    except Exception:
        pass

    return last_ingested, last_validated, last_produced
