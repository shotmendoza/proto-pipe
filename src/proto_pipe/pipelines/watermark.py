"""Watermark store backed by a DuckDB table.

Tracks the last successfully processed timestamp per report,
so only new or modified rows are checked on subsequent runs.
"""

from datetime import datetime, timezone
from pathlib import Path
from threading import Lock

import duckdb


class WatermarkStore:
    """stores the timestamp of when the report last successfully processed
    """
    def __init__(self, db_path: str | Path):
        self.db_path = db_path
        self._init_table()

    def _connect(self) -> duckdb.DuckDBPyConnection:
        return duckdb.connect(self.db_path)

    def _init_table(self) -> None:
        """Create the watermark table if it doesn't exist."""
        with self._connect() as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS watermarks (
                    report_name  VARCHAR PRIMARY KEY,
                    last_run_at  TIMESTAMPTZ NOT NULL
                )
            """)

    def get(self, report_name: str) -> datetime | None:
        """Return the last run timestamp for a report, or None if first run."""
        with self._connect() as conn:
            result = conn.execute(
                "SELECT last_run_at FROM watermarks WHERE report_name = ?",
                [report_name],
            ).fetchone()
        if result is None:
            return None
        return result[0].astimezone(timezone.utc)

    def set(self, report_name: str, timestamp: datetime) -> None:
        """Advance the watermark for a report to the given timestamp."""
        with self._connect() as conn:
            conn.execute("""
                INSERT INTO watermarks (report_name, last_run_at)
                VALUES (?, ?)
                ON CONFLICT (report_name)
                DO UPDATE SET last_run_at = excluded.last_run_at
            """, [report_name, timestamp])

    def all(self) -> dict[str, datetime]:
        """Return all watermarks as a dict."""
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT report_name, last_run_at FROM watermarks"
            ).fetchall()
        return {row[0]: row[1] for row in rows}


watermark_lock = Lock()
