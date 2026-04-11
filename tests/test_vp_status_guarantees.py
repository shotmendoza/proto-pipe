"""Behavioral guarantee tests for vp status.

Guarantees:
- vp status (bare) shows all stages (Sources, Reports, Deliverables)
- vp status source shows all sources without requiring a name
- vp status source <n> drills into one source with detail
- vp status report shows all reports without requiring a name
- Prescriptive next-step messages present when action needed
"""
from datetime import datetime, timezone

import duckdb
import pytest
from click.testing import CliRunner
from unittest.mock import patch


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture()
def status_db(tmp_path, populated_db) -> str:
    """Pipeline DB with ingest_state and validation_pass populated."""
    from proto_pipe.io.db import init_all_pipeline_tables

    now = datetime.now(timezone.utc)
    with duckdb.connect(populated_db) as conn:
        init_all_pipeline_tables(conn)
        # Ingest history
        conn.execute("""
            INSERT INTO ingest_state (id, filename, table_name, status, rows, ingested_at)
            VALUES
                ('is1', 'sales_q1.csv', 'sales', 'ok', 3, ?)
        """, [now])
        # Validation history
        conn.execute("""
            INSERT INTO validation_pass
                (pk_value, table_name, report_name, row_hash, check_set_hash, status, validated_at)
            VALUES
                ('ORD-001', 'sales', 'sales_report', 'h1', 'cs1', 'passed', ?),
                ('ORD-002', 'sales', 'sales_report', 'h2', 'cs1', 'passed', ?)
        """, [now, now])
    return populated_db


@pytest.fixture()
def src_cfg_path(tmp_path) -> "Path":
    from proto_pipe.io.config import SourceConfig
    path = tmp_path / "sources_config.yaml"
    config = SourceConfig(path)
    config.add({
        "name": "sales",
        "target_table": "sales",
        "primary_key": "order_id",
        "patterns": ["sales*.csv"],
    })
    return path


def _cfg(db_path, src_cfg_path):
    return lambda key, override=None: {
        "pipeline_db": db_path,
        "sources_config": str(src_cfg_path),
        "deliverables_config": str(src_cfg_path.parent / "deliverables_config.yaml"),
        "output_dir": str(src_cfg_path.parent / "output"),
        "log_dir": str(src_cfg_path.parent / "logs"),
    }.get(key, override or key)


# ---------------------------------------------------------------------------
# vp status (bare) — all stages
# ---------------------------------------------------------------------------

class TestVpStatusBare:
    def test_shows_all_stages(self, status_db, src_cfg_path):
        from proto_pipe.cli.status import status_cmd

        runner = CliRunner()
        with patch(
            "proto_pipe.cli.status.config_path_or_override",
            side_effect=_cfg(status_db, src_cfg_path),
        ):
            result = runner.invoke(status_cmd, [])

        assert result.exit_code == 0, result.output
        assert "Sources" in result.output or "source" in result.output.lower()
        assert "Reports" in result.output or "report" in result.output.lower()
        assert "Deliverables" in result.output or "deliverable" in result.output.lower()

    def test_prescriptive_next_steps_when_no_deliverables(self, status_db, src_cfg_path):
        from proto_pipe.cli.status import status_cmd

        runner = CliRunner()
        with patch(
            "proto_pipe.cli.status.config_path_or_override",
            side_effect=_cfg(status_db, src_cfg_path),
        ):
            result = runner.invoke(status_cmd, [])

        assert result.exit_code == 0
        assert "Next steps" in result.output or "vp new deliverable" in result.output


# ---------------------------------------------------------------------------
# vp status source — all sources, no name required
# ---------------------------------------------------------------------------

class TestVpStatusSource:
    def test_shows_all_sources_without_name(self, status_db, src_cfg_path):
        from proto_pipe.cli.status import status_cmd

        runner = CliRunner()
        with patch(
            "proto_pipe.cli.status.config_path_or_override",
            side_effect=_cfg(status_db, src_cfg_path),
        ):
            result = runner.invoke(status_cmd, ["source"])

        assert result.exit_code == 0, result.output
        assert "sales" in result.output

    def test_drilldown_shows_detail(self, status_db, src_cfg_path):
        from proto_pipe.cli.status import status_cmd

        runner = CliRunner()
        with patch(
            "proto_pipe.cli.status.config_path_or_override",
            side_effect=_cfg(status_db, src_cfg_path),
        ):
            result = runner.invoke(status_cmd, ["source", "sales"])

        assert result.exit_code == 0, result.output
        assert "Source: sales" in result.output
        # Should show row count or ingest history
        assert "Rows" in result.output or "rows" in result.output


# ---------------------------------------------------------------------------
# vp status report — all reports, no name required
# ---------------------------------------------------------------------------

class TestVpStatusReport:
    def test_shows_all_reports_without_name(self, status_db, src_cfg_path):
        from proto_pipe.cli.status import status_cmd

        runner = CliRunner()
        with patch(
            "proto_pipe.cli.status.config_path_or_override",
            side_effect=_cfg(status_db, src_cfg_path),
        ):
            result = runner.invoke(status_cmd, ["report"])

        assert result.exit_code == 0, result.output
        assert "sales_report" in result.output

    def test_drilldown_shows_detail(self, status_db, src_cfg_path):
        from proto_pipe.cli.status import status_cmd

        runner = CliRunner()
        with patch(
            "proto_pipe.cli.status.config_path_or_override",
            side_effect=_cfg(status_db, src_cfg_path),
        ):
            result = runner.invoke(status_cmd, ["report", "sales_report"])

        assert result.exit_code == 0, result.output
        assert "Report: sales_report" in result.output


class TestVpStatusSourceMissingTable:
    """When a source table doesn't exist (ingest failed before creating it),
    vp status source <name> should show 'table not found' plus the most
    recent failure message from ingest_state.
    """

    @pytest.fixture()
    def failed_ingest_db(self, tmp_path):
        """DB with ingest_state failure but no source table."""
        from proto_pipe.io.db import init_all_pipeline_tables

        db_path = str(tmp_path / "pipeline.db")
        now = datetime.now(timezone.utc)
        with duckdb.connect(db_path) as conn:
            init_all_pipeline_tables(conn)
            # File failed — table 'foobar' was never created
            conn.execute(
                """
                         INSERT INTO ingest_state
                             (id, filename, table_name, status, rows, message, ingested_at)
                         VALUES
                             ('ff1', 'foobar_2026.csv', 'foobar', 'failed', NULL,
                              'DuckDB write failed: Conversion Error', ?)
                         """,
                [now],
            )
        return db_path

    def test_missing_table_shows_table_not_found(self, failed_ingest_db, src_cfg_path):
        from proto_pipe.cli.status import status_cmd

        runner = CliRunner()
        with patch(
            "proto_pipe.cli.status.config_path_or_override",
            side_effect=_cfg(failed_ingest_db, src_cfg_path),
        ):
            result = runner.invoke(status_cmd, ["source", "foobar"])

        assert result.exit_code == 0, result.output
        assert "table not found" in result.output.lower()

    def test_missing_table_shows_failure_message(self, failed_ingest_db, src_cfg_path):
        from proto_pipe.cli.status import status_cmd

        runner = CliRunner()
        with patch(
            "proto_pipe.cli.status.config_path_or_override",
            side_effect=_cfg(failed_ingest_db, src_cfg_path),
        ):
            result = runner.invoke(status_cmd, ["source", "foobar"])

        assert result.exit_code == 0, result.output
        assert "Conversion Error" in result.output

    def test_missing_table_shows_fix_command(self, failed_ingest_db, src_cfg_path):
        from proto_pipe.cli.status import status_cmd

        runner = CliRunner()
        with patch(
            "proto_pipe.cli.status.config_path_or_override",
            side_effect=_cfg(failed_ingest_db, src_cfg_path),
        ):
            result = runner.invoke(status_cmd, ["source", "foobar"])

        assert result.exit_code == 0, result.output
        assert "vp ingest" in result.output

    def test_existing_table_no_failure_message(self, status_db, src_cfg_path):
        """When the table exists, no failure message is shown."""
        from proto_pipe.cli.status import status_cmd

        runner = CliRunner()
        with patch(
            "proto_pipe.cli.status.config_path_or_override",
            side_effect=_cfg(status_db, src_cfg_path),
        ):
            result = runner.invoke(status_cmd, ["source", "sales"])

        assert result.exit_code == 0, result.output
        assert "table not found" not in result.output.lower()
