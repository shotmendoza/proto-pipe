
"""Pipeline runner.

- Pulls report definitions from ReportRegistry
- Loads only new/modified rows from DuckDB using watermarks
- Runs checks via CheckRegistry (no param knowledge needed here)
- Advances the watermark only on success
- Parallelizes across reports and optionally across checks within a report
"""

from datetime import datetime

import duckdb


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------
def load_from_duckdb(
        source: dict,
        last_run: datetime | None
):
    """Return a DataFrame containing only rows newer than last_run.

    :param source: Source DataFrame
    :param last_run: Last run datetime

    """
    conn = duckdb.connect(source["path"])
    table = source["table"]
    ts_col = source["timestamp_col"]

    if last_run:
        query = f'SELECT * FROM "{table}" WHERE "{ts_col}"::TIMESTAMPTZ > ?'
        df = conn.execute(query, [last_run]).df()
    else:
        df = conn.execute(f'SELECT * FROM "{table}"').df()

    conn.close()
    return df
