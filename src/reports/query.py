"""
Query builder.
Translates filter definitions from deliverables_config.yaml into
parameterised DuckDB SQL. Keeps all SQL construction in one place
so the CLI and runner never build queries by hand.
"""

from datetime import datetime, timezone

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
    table: str,
    filters: dict | None = None,
    cli_overrides: dict | None = None,
) -> pd.DataFrame:
    """
    Query a DuckDB table, applying config filters and any CLI overrides.

    cli_overrides shape (mirrors filters, merged on top of config filters):
        {
            "date_filters": [{"col": "order_date", "from": "2026-03-01"}],
            "field_filters": [{"col": "region", "values": ["EMEA"]}]
        }
    """
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
