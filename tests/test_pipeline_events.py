"""Tests for pipeline_events table and vp export log.

Covers:
- init_pipeline_events: table created by init_all_pipeline_tables
- write_pipeline_events: events written correctly, fire-and-forget on bad db
- export log: basic export, --severity filter, --since filter, --clear flag
- skipped ingest files produce no events
- validation events reflect correct severity
"""

from datetime import datetime, timezone, timedelta
from pathlib import Path

import duckdb
import pandas as pd
import pytest

from proto_pipe.io.db import (
    init_all_pipeline_tables,
    write_pipeline_events,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_db(tmp_path: Path) -> str:
    db_path = str(tmp_path / "pipeline.db")
    conn = duckdb.connect(db_path)
    init_all_pipeline_tables(conn)
    conn.close()
    return db_path


def _read_events(db_path: str) -> pd.DataFrame:
    conn = duckdb.connect(db_path)
    try:
        return conn.execute(
            "SELECT * FROM pipeline_events ORDER BY occurred_at"
        ).df()
    finally:
        conn.close()


def _count_events(db_path: str) -> int:
    return len(_read_events(db_path))


# ---------------------------------------------------------------------------
# Table initialisation
# ---------------------------------------------------------------------------

class TestInitPipelineEvents:
    def test_table_created_by_db_init(self, tmp_path):
        db_path = _make_db(tmp_path)
        conn = duckdb.connect(db_path)
        try:
            result = conn.execute(
                "SELECT count(*) FROM information_schema.tables "
                "WHERE table_name = 'pipeline_events'"
            ).fetchone()
            assert result[0] == 1
        finally:
            conn.close()

    def test_idempotent_second_init(self, tmp_path):
        db_path = _make_db(tmp_path)
        conn = duckdb.connect(db_path)
        # Should not raise
        init_all_pipeline_tables(conn)
        conn.close()

    def test_table_starts_empty(self, tmp_path):
        db_path = _make_db(tmp_path)
        assert _count_events(db_path) == 0


# ---------------------------------------------------------------------------
# write_pipeline_events
# ---------------------------------------------------------------------------

class TestWritePipelineEvents:
    def test_single_event_written(self, tmp_path):
        db_path = _make_db(tmp_path)
        write_pipeline_events(db_path, [{
            "event_type": "ingest_ok",
            "source_name": "sales_2026-03.csv",
            "severity": "info",
            "detail": "",
        }])
        df = _read_events(db_path)
        assert len(df) == 1
        assert df.iloc[0]["event_type"] == "ingest_ok"
        assert df.iloc[0]["source_name"] == "sales_2026-03.csv"
        assert df.iloc[0]["severity"] == "info"

    def test_multiple_events_written(self, tmp_path):
        db_path = _make_db(tmp_path)
        write_pipeline_events(db_path, [
            {"event_type": "ingest_ok",     "source_name": "a.csv", "severity": "info",  "detail": ""},
            {"event_type": "ingest_failed", "source_name": "b.csv", "severity": "error", "detail": "bad file"},
        ])
        df = _read_events(db_path)
        assert len(df) == 2
        assert set(df["event_type"]) == {"ingest_ok", "ingest_failed"}

    def test_empty_list_writes_nothing(self, tmp_path):
        db_path = _make_db(tmp_path)
        write_pipeline_events(db_path, [])
        assert _count_events(db_path) == 0

    def test_null_source_name_allowed(self, tmp_path):
        db_path = _make_db(tmp_path)
        write_pipeline_events(db_path, [{
            "event_type": "report_error",
            "source_name": None,
            "severity": "error",
            "detail": "source table missing",
        }])
        df = _read_events(db_path)
        assert len(df) == 1
        assert df.iloc[0]["source_name"] is None or pd.isna(df.iloc[0]["source_name"])

    def test_detail_truncated_to_1000_chars(self, tmp_path):
        db_path = _make_db(tmp_path)
        long_detail = "x" * 2000
        write_pipeline_events(db_path, [{
            "event_type": "ingest_failed",
            "source_name": "f.csv",
            "severity": "error",
            "detail": long_detail,
        }])
        df = _read_events(db_path)
        assert len(df.iloc[0]["detail"]) == 1000

    def test_bad_db_path_does_not_raise(self):
        # Fire-and-forget — bad path must never propagate an exception
        write_pipeline_events("/nonexistent/pipeline.db", [{
            "event_type": "ingest_ok",
            "source_name": "x.csv",
            "severity": "info",
            "detail": "",
        }])

    def test_occurred_at_is_set(self, tmp_path):
        db_path = _make_db(tmp_path)
        before = datetime.now(timezone.utc)
        write_pipeline_events(db_path, [{
            "event_type": "validation_passed",
            "source_name": "sales_validation",
            "severity": "info",
            "detail": "",
        }])
        after = datetime.now(timezone.utc)
        df = _read_events(db_path)
        ts = pd.to_datetime(df.iloc[0]["occurred_at"], utc=True)
        assert before <= ts <= after


# ---------------------------------------------------------------------------
# vp export log — via Click test runner
# ---------------------------------------------------------------------------

class TestExportLog:
    @pytest.fixture()
    def db_with_events(self, tmp_path):
        db_path = _make_db(tmp_path)
        now = datetime.now(timezone.utc)
        write_pipeline_events(db_path, [
            {"event_type": "ingest_ok",          "source_name": "a.csv",             "severity": "info",  "detail": ""},
            {"event_type": "ingest_failed",       "source_name": "b.csv",             "severity": "error", "detail": "bad"},
            {"event_type": "validation_passed",   "source_name": "sales_validation",  "severity": "info",  "detail": ""},
            {"event_type": "validation_failed",   "source_name": "inventory_val",     "severity": "warn",  "detail": ""},
        ])
        return db_path, tmp_path

    def test_exports_all_events(self, db_with_events):
        from click.testing import CliRunner
        from proto_pipe.cli.commands.export import export_log

        db_path, tmp_path = db_with_events
        out_path = str(tmp_path / "events.csv")

        runner = CliRunner()
        result = runner.invoke(export_log, [
            "--pipeline-db", db_path,
            "--output", out_path,
        ])

        assert result.exit_code == 0, result.output
        df = pd.read_csv(out_path)
        assert len(df) == 4

    def test_severity_filter_errors_only(self, db_with_events):
        from click.testing import CliRunner
        from proto_pipe.cli.commands.export import export_log

        db_path, tmp_path = db_with_events
        out_path = str(tmp_path / "errors.csv")

        runner = CliRunner()
        result = runner.invoke(export_log, [
            "--pipeline-db", db_path,
            "--output", out_path,
            "--severity", "error",
        ])

        assert result.exit_code == 0, result.output
        df = pd.read_csv(out_path)
        assert len(df) == 1
        assert df.iloc[0]["severity"] == "error"

    def test_since_filter(self, tmp_path):
        from click.testing import CliRunner
        from proto_pipe.cli.commands.export import export_log

        db_path = _make_db(tmp_path)
        # Write one old event and one recent event
        conn = duckdb.connect(db_path)
        old_ts = datetime(2026, 1, 1, tzinfo=timezone.utc)
        new_ts = datetime(2026, 4, 1, tzinfo=timezone.utc)
        conn.execute(
            "INSERT INTO pipeline_events VALUES (?, ?, ?, ?, ?)",
            ["ingest_ok", "old.csv", "info", "", old_ts],
        )
        conn.execute(
            "INSERT INTO pipeline_events VALUES (?, ?, ?, ?, ?)",
            ["ingest_ok", "new.csv", "info", "", new_ts],
        )
        conn.close()

        out_path = str(tmp_path / "since.csv")
        runner = CliRunner()
        result = runner.invoke(export_log, [
            "--pipeline-db", db_path,
            "--output", out_path,
            "--since", "2026-03-01",
        ])

        assert result.exit_code == 0, result.output
        df = pd.read_csv(out_path)
        assert len(df) == 1
        assert df.iloc[0]["source_name"] == "new.csv"

    def test_clear_deletes_exported_rows(self, db_with_events):
        from click.testing import CliRunner
        from proto_pipe.cli.commands.export import export_log

        db_path, tmp_path = db_with_events
        out_path = str(tmp_path / "cleared.csv")

        runner = CliRunner()
        result = runner.invoke(export_log, [
            "--pipeline-db", db_path,
            "--output", out_path,
            "--severity", "error",
            "--clear",
        ])

        assert result.exit_code == 0, result.output
        # Only the error row is deleted; others remain
        assert _count_events(db_path) == 3

    def test_clear_without_filter_deletes_all(self, db_with_events):
        from click.testing import CliRunner
        from proto_pipe.cli.commands.export import export_log

        db_path, tmp_path = db_with_events
        out_path = str(tmp_path / "all_cleared.csv")

        runner = CliRunner()
        result = runner.invoke(export_log, [
            "--pipeline-db", db_path,
            "--output", out_path,
            "--clear",
        ])

        assert result.exit_code == 0, result.output
        assert _count_events(db_path) == 0

    def test_invalid_since_format_returns_error(self, db_with_events):
        from click.testing import CliRunner
        from proto_pipe.cli.commands.export import export_log

        db_path, tmp_path = db_with_events
        runner = CliRunner()
        result = runner.invoke(export_log, [
            "--pipeline-db", db_path,
            "--output", str(tmp_path / "x.csv"),
            "--since", "not-a-date",
        ])

        assert result.exit_code == 0  # exits cleanly, not crashes
        assert "[error]" in result.output

    def test_no_matching_events_prints_info(self, db_with_events):
        from click.testing import CliRunner
        from proto_pipe.cli.commands.export import export_log

        db_path, tmp_path = db_with_events
        runner = CliRunner()
        result = runner.invoke(export_log, [
            "--pipeline-db", db_path,
            "--output", str(tmp_path / "x.csv"),
            "--since", "2030-01-01",  # future date — no events
        ])

        assert result.exit_code == 0
        assert "No events matched" in result.output
