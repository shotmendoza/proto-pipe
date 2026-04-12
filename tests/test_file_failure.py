"""Tests for file failure current-state queries.

Behavioral guarantees:
- vp errors shows file failures only for files whose MOST RECENT
  ingest attempt is 'failed'. Files re-ingested successfully show 0.
- Applies to: query_error_overview, query_file_failures,
  query_source_error_summary.
"""
from __future__ import annotations

from datetime import datetime, timezone, timedelta

import duckdb
import pytest

from proto_pipe.io.db import init_all_pipeline_tables


@pytest.fixture()
def conn(tmp_path):
    db = duckdb.connect(str(tmp_path / "test.db"))
    init_all_pipeline_tables(db)
    return db


def _insert_ingest(conn, filename, table_name, status, message=None, offset_hours=0):
    """Insert an ingest_state row with a controlled timestamp."""
    import uuid
    ts = datetime.now(timezone.utc) + timedelta(hours=offset_hours)
    conn.execute("""
        INSERT INTO ingest_state (id, filename, table_name, status, rows, message, ingested_at)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """, [str(uuid.uuid4()), filename, table_name, status, 0, message, ts])


# ---------------------------------------------------------------------------
# query_error_overview
# ---------------------------------------------------------------------------


class TestErrorOverviewCurrentState:

    def test_resolved_file_not_counted(self, conn):
        """File that failed then succeeded → 0 file failures."""
        from proto_pipe.pipelines.query import query_error_overview

        _insert_ingest(conn, "a.csv", "sales", "failed", "bad cols", offset_hours=-2)
        _insert_ingest(conn, "a.csv", "sales", "failed", "bad cols", offset_hours=-1)
        _insert_ingest(conn, "a.csv", "sales", "ok", offset_hours=0)

        overview = query_error_overview(conn)

        assert overview.file_failure_count == 0
        assert overview.file_failure_table_count == 0

    def test_still_failing_file_counted(self, conn):
        """File whose latest attempt is 'failed' → counted."""
        from proto_pipe.pipelines.query import query_error_overview

        _insert_ingest(conn, "a.csv", "sales", "failed", "bad cols", offset_hours=-1)
        _insert_ingest(conn, "a.csv", "sales", "failed", "still bad", offset_hours=0)

        overview = query_error_overview(conn)

        assert overview.file_failure_count == 1
        assert overview.file_failure_table_count == 1

    def test_mix_of_resolved_and_failing(self, conn):
        """One resolved file + one still failing → 1 failure."""
        from proto_pipe.pipelines.query import query_error_overview

        # Resolved: failed then ok
        _insert_ingest(conn, "a.csv", "sales", "failed", "bad", offset_hours=-2)
        _insert_ingest(conn, "a.csv", "sales", "ok", offset_hours=-1)
        # Still failing
        _insert_ingest(conn, "b.csv", "sales", "failed", "bad", offset_hours=0)

        overview = query_error_overview(conn)

        assert overview.file_failure_count == 1

    def test_no_failures_at_all(self, conn):
        """Only successful ingests → 0."""
        from proto_pipe.pipelines.query import query_error_overview

        _insert_ingest(conn, "a.csv", "sales", "ok")

        overview = query_error_overview(conn)

        assert overview.file_failure_count == 0


# ---------------------------------------------------------------------------
# query_file_failures
# ---------------------------------------------------------------------------


class TestFileFailuresCurrentState:

    def test_resolved_file_excluded(self, conn):
        """File that failed 3x then succeeded → not in results."""
        from proto_pipe.pipelines.query import query_file_failures

        _insert_ingest(conn, "a.csv", "sales", "failed", "err1", offset_hours=-3)
        _insert_ingest(conn, "a.csv", "sales", "failed", "err2", offset_hours=-2)
        _insert_ingest(conn, "a.csv", "sales", "failed", "err3", offset_hours=-1)
        _insert_ingest(conn, "a.csv", "sales", "ok", offset_hours=0)

        failures = query_file_failures(conn)

        assert len(failures) == 0

    def test_still_failing_included(self, conn):
        """File whose latest is 'failed' → in results."""
        from proto_pipe.pipelines.query import query_file_failures

        _insert_ingest(conn, "a.csv", "sales", "failed", "bad cols", offset_hours=0)

        failures = query_file_failures(conn)

        assert len(failures) == 1
        assert failures[0].filename == "a.csv"
        assert failures[0].message == "bad cols"

    def test_filter_by_table_name(self, conn):
        """name= filter works with current-state logic."""
        from proto_pipe.pipelines.query import query_file_failures

        _insert_ingest(conn, "a.csv", "sales", "failed", "err", offset_hours=0)
        _insert_ingest(conn, "b.csv", "claims", "failed", "err", offset_hours=0)

        failures = query_file_failures(conn, name="sales")

        assert len(failures) == 1
        assert failures[0].table_name == "sales"


# ---------------------------------------------------------------------------
# query_source_error_summary
# ---------------------------------------------------------------------------


class TestSourceErrorSummaryCurrentState:

    def test_resolved_file_not_counted(self, conn):
        """File that failed then succeeded → 0 file failures in summary."""
        from proto_pipe.pipelines.query import query_source_error_summary

        _insert_ingest(conn, "a.csv", "sales", "failed", "bad", offset_hours=-1)
        _insert_ingest(conn, "a.csv", "sales", "ok", offset_hours=0)

        summaries = query_source_error_summary(conn)

        # No errors at all → empty list (no source_block rows either)
        assert len(summaries) == 0

    def test_still_failing_counted(self, conn):
        """File whose latest is 'failed' → counted in summary."""
        from proto_pipe.pipelines.query import query_source_error_summary

        _insert_ingest(conn, "a.csv", "sales", "failed", "bad", offset_hours=0)

        summaries = query_source_error_summary(conn)

        assert len(summaries) == 1
        assert summaries[0].table_name == "sales"
        assert summaries[0].file_failure_count == 1

    def test_three_fails_then_success_shows_zero(self, conn):
        """3 failures followed by success → file_failure_count is 0, not 3."""
        from proto_pipe.pipelines.query import query_source_error_summary

        _insert_ingest(conn, "a.csv", "sales", "failed", "err", offset_hours=-3)
        _insert_ingest(conn, "a.csv", "sales", "failed", "err", offset_hours=-2)
        _insert_ingest(conn, "a.csv", "sales", "failed", "err", offset_hours=-1)
        _insert_ingest(conn, "a.csv", "sales", "ok", offset_hours=0)

        summaries = query_source_error_summary(conn)

        # No errors → empty (no source_block rows either)
        assert len(summaries) == 0
