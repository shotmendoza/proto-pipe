"""Data loading utilities for the report layer.

Loads rows from DuckDB for report execution. Called by run_report
in the legacy path (no validation_pass tracking). The primary data
loading path for ingest goes through io/ingest.py, not here.
"""
from __future__ import annotations

from datetime import datetime

import duckdb


def load_from_duckdb(
    source: dict,
    last_run: datetime | None,
):
    """Return a DataFrame containing only rows newer than last_run.

    Used by the legacy run_report path when pipeline_db is not provided.
    When pipeline_db is provided, run_report queries the source table
    directly and uses validation_pass for incremental tracking instead.

    :param source:   Source config dict — must have 'path' and 'table' keys.
                     'timestamp_col' is optional; falls back to '_ingested_at'.
    :param last_run: Watermark from the previous run. When None, all rows
                     are returned (first run or reset).
    :return: DataFrame of rows newer than last_run, or all rows if None.
    """
    conn = duckdb.connect(source["path"])
    table = source["table"]
    ts_col = source.get("timestamp_col") or "_ingested_at"

    try:
        if last_run:
            query = f'SELECT * FROM "{table}" WHERE "{ts_col}"::TIMESTAMPTZ > ?'
            df = conn.execute(query, [last_run]).df()
        else:
            df = conn.execute(f'SELECT * FROM "{table}"').df()
    finally:
        conn.close()

    return df