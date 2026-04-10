"""Behavioral guarantee tests for vp deliver.

Guarantees:
- vp deliver <n> produces output file
- vp deliver with unknown name shows error with available names
- Stale-data warning when source newer than last validate (deferred — stub only)
"""
import duckdb
import pytest
from click.testing import CliRunner
from pathlib import Path
from unittest.mock import patch

from proto_pipe.io.config import SourceConfig, ReportConfig, DeliverableConfig


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture()
def deliver_env(tmp_path, populated_db):
    """Full environment for deliver tests: DB, configs, output dir."""
    from proto_pipe.io.db import init_all_pipeline_tables

    with duckdb.connect(populated_db) as conn:
        init_all_pipeline_tables(conn)

    # Source config
    src_path = tmp_path / "sources_config.yaml"
    SourceConfig(src_path).add({
        "name": "sales",
        "target_table": "sales",
        "primary_key": "order_id",
        "patterns": ["sales*.csv"],
    })

    # Report config
    rep_path = tmp_path / "reports_config.yaml"
    rep_cfg = ReportConfig(rep_path)
    rep_cfg.add({
        "name": "sales_report",
        "source": {"table": "sales", "primary_key": "order_id"},
        "target_table": "sales_report",
        "checks": [],
    })

    # Deliverable config with sql_file that just selects from sales
    sql_dir = tmp_path / "sql"
    sql_dir.mkdir()
    sql_file = sql_dir / "sales_out.sql"
    sql_file.write_text("SELECT order_id, customer_id, price FROM sales")

    del_path = tmp_path / "deliverables_config.yaml"
    output_dir = tmp_path / "output"
    output_dir.mkdir()
    del_cfg = DeliverableConfig(del_path)
    del_cfg.add({
        "name": "monthly_pack",
        "format": "csv",
        "filename_template": "monthly_pack_{date}.csv",
        "output_dir": str(output_dir),
        "reports": [{"name": "sales_report", "sql_file": str(sql_file)}],
    })

    def cfg(key, override=None):
        return {
            "pipeline_db": populated_db,
            "sources_config": str(src_path),
            "reports_config": str(rep_path),
            "deliverables_config": str(del_path),
            "output_dir": str(output_dir),
            "sql_dir": str(sql_dir),
        }.get(key, override or key)

    return {
        "db": populated_db,
        "cfg": cfg,
        "output_dir": output_dir,
    }


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestVpDeliverGuarantees:
    def test_produces_output_file(self, deliver_env):
        from proto_pipe.cli.reports import deliver

        runner = CliRunner()
        with patch(
            "proto_pipe.cli.reports.config_path_or_override",
            side_effect=deliver_env["cfg"],
        ):
            result = runner.invoke(deliver, ["monthly_pack"])

        assert result.exit_code == 0, result.output
        # Check output file was created
        output_files = list(deliver_env["output_dir"].glob("*"))
        assert len(output_files) >= 1, (
            f"Expected at least one output file, got: {output_files}\n"
            f"Output: {result.output}"
        )

    def test_unknown_name_shows_error(self, deliver_env):
        from proto_pipe.cli.reports import deliver

        runner = CliRunner()
        with patch(
            "proto_pipe.cli.reports.config_path_or_override",
            side_effect=deliver_env["cfg"],
        ):
            result = runner.invoke(deliver, ["nonexistent_pack"])

        assert result.exit_code == 0  # Click returns 0, error in output
        assert "[error]" in result.output
        assert "monthly_pack" in result.output, (
            "Error should list available deliverable names"
        )

    def test_stale_data_warning(self):
        """Stub — stale-data warning is deferred (CLAUDE.md Active Deferred Work).

        When implemented: warn when source has rows newer than the report's
        last validate timestamp.
        """
        pytest.skip("Deferred: vp deliver stale-data warning not yet implemented")
