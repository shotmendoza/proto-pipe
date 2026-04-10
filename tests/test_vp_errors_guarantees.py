"""Behavioral guarantee tests for vp errors.

Guarantees:
- vp errors (bare) shows summary counts from both source_block and validation_block
- vp errors source shows rows grouped by cause with prescriptive fix commands
- vp errors source <n> filters to one table
- vp errors source clear removes rows from source_block
- vp errors source clear does not modify the source table
- vp errors source clear with no rows shows message
- vp errors source clear without --yes prompts for confirmation
- vp errors report clear removes rows from validation_block
- Prescriptive fix commands appear in output
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
def errors_db(tmp_path, populated_db) -> str:
    """Pipeline DB with pipeline tables and sample flags in both stages."""
    from proto_pipe.io.db import init_all_pipeline_tables

    now = datetime.now(timezone.utc)
    with duckdb.connect(populated_db) as conn:
        init_all_pipeline_tables(conn)
        # Source errors
        conn.execute("""
            INSERT INTO source_block (id, table_name, check_name, pk_value, reason, flagged_at)
            VALUES
                ('sb1', 'sales', 'type_conflict',      'ORD-001', 'bad int',    ?),
                ('sb2', 'sales', 'type_conflict',      'ORD-002', 'bad int',    ?),
                ('sb3', 'sales', 'duplicate_conflict',  'ORD-003', 'dup values', ?)
        """, [now, now, now])
        # Validation errors
        conn.execute("""
            INSERT INTO validation_block
                (id, table_name, report_name, check_name, pk_value, reason, flagged_at)
            VALUES
                ('vb1', 'sales', 'sales_report', 'null_check',  'ORD-001', 'null price', ?),
                ('vb2', 'sales', 'sales_report', 'range_check', 'ORD-002', 'out of range', ?)
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


def _cfg(errors_db, src_cfg_path):
    return lambda key, override=None: {
        "pipeline_db": errors_db,
        "sources_config": str(src_cfg_path),
        "output_dir": str(src_cfg_path.parent / "output"),
        "incoming_dir": str(src_cfg_path.parent / "incoming"),
    }.get(key, override or key)


# ---------------------------------------------------------------------------
# vp errors (bare) — summary counts
# ---------------------------------------------------------------------------

class TestVpErrorsSummary:
    def test_bare_shows_counts_both_stages(self, errors_db, src_cfg_path):
        from proto_pipe.cli.errors import errors_cmd

        runner = CliRunner()
        with patch(
            "proto_pipe.cli.errors.config_path_or_override",
            side_effect=_cfg(errors_db, src_cfg_path),
        ):
            result = runner.invoke(errors_cmd, [])

        assert result.exit_code == 0, result.output
        assert "3" in result.output, "Should show 3 source errors"
        assert "2" in result.output, "Should show 2 validation errors"
        assert "Source" in result.output
        assert "Report" in result.output

    def test_bare_all_clear_when_empty(self, tmp_path, populated_db, src_cfg_path):
        from proto_pipe.cli.errors import errors_cmd
        from proto_pipe.io.db import init_all_pipeline_tables

        with duckdb.connect(populated_db) as conn:
            init_all_pipeline_tables(conn)

        runner = CliRunner()
        with patch(
            "proto_pipe.cli.errors.config_path_or_override",
            side_effect=_cfg(populated_db, src_cfg_path),
        ):
            result = runner.invoke(errors_cmd, [])

        assert result.exit_code == 0
        assert "all clear" in result.output.lower()


# ---------------------------------------------------------------------------
# vp errors source — grouped by cause + prescriptive
# ---------------------------------------------------------------------------

class TestVpErrorsSource:
    def test_shows_grouped_by_cause(self, errors_db, src_cfg_path):
        from proto_pipe.cli.errors import errors_cmd

        runner = CliRunner()
        with patch(
            "proto_pipe.cli.errors.config_path_or_override",
            side_effect=_cfg(errors_db, src_cfg_path),
        ):
            result = runner.invoke(errors_cmd, ["source"])

        assert result.exit_code == 0, result.output
        assert "type_conflict" in result.output
        assert "duplicate_conflict" in result.output

    def test_prescriptive_fix_commands_in_output(self, errors_db, src_cfg_path):
        from proto_pipe.cli.errors import errors_cmd

        runner = CliRunner()
        with patch(
            "proto_pipe.cli.errors.config_path_or_override",
            side_effect=_cfg(errors_db, src_cfg_path),
        ):
            result = runner.invoke(errors_cmd, ["source"])

        assert "vp errors source export" in result.output
        assert "vp errors source retry" in result.output

    def test_name_filters_to_one_table(self, errors_db, src_cfg_path):
        from proto_pipe.cli.errors import errors_cmd

        runner = CliRunner()
        with patch(
            "proto_pipe.cli.errors.config_path_or_override",
            side_effect=_cfg(errors_db, src_cfg_path),
        ):
            result = runner.invoke(errors_cmd, ["source", "sales"])

        assert result.exit_code == 0, result.output
        assert "sales" in result.output

    def test_name_no_errors_shows_all_clear(self, errors_db, src_cfg_path):
        from proto_pipe.cli.errors import errors_cmd

        runner = CliRunner()
        with patch(
            "proto_pipe.cli.errors.config_path_or_override",
            side_effect=_cfg(errors_db, src_cfg_path),
        ):
            result = runner.invoke(errors_cmd, ["source", "nonexistent"])

        assert "all clear" in result.output.lower() or "no source errors" in result.output.lower()


# ---------------------------------------------------------------------------
# vp errors source clear — migrated from TestFlaggedClear
# ---------------------------------------------------------------------------

class TestVpErrorsSourceClear:
    def test_clear_removes_rows(self, errors_db, src_cfg_path):
        from proto_pipe.cli.errors import source_clear

        runner = CliRunner()
        with patch(
            "proto_pipe.cli.errors.config_path_or_override",
            side_effect=_cfg(errors_db, src_cfg_path),
        ):
            result = runner.invoke(source_clear, ["sales", "--yes"])

        assert result.exit_code == 0, result.output

        with duckdb.connect(errors_db) as conn:
            count = conn.execute(
                "SELECT count(*) FROM source_block WHERE table_name = 'sales'"
            ).fetchone()[0]
        assert count == 0

    def test_clear_empty_shows_message(self, tmp_path, populated_db, src_cfg_path):
        from proto_pipe.cli.errors import source_clear
        from proto_pipe.io.db import init_all_pipeline_tables

        with duckdb.connect(populated_db) as conn:
            init_all_pipeline_tables(conn)

        runner = CliRunner()
        with patch(
            "proto_pipe.cli.errors.config_path_or_override",
            side_effect=_cfg(populated_db, src_cfg_path),
        ):
            result = runner.invoke(source_clear, ["sales", "--yes"])

        assert result.exit_code == 0
        assert "no source errors" in result.output.lower()

    def test_clear_source_table_unchanged(self, errors_db, src_cfg_path):
        """vp errors source clear drops flags without modifying the source table."""
        from proto_pipe.cli.errors import source_clear

        with duckdb.connect(errors_db) as conn:
            before_count = conn.execute("SELECT count(*) FROM sales").fetchone()[0]

        runner = CliRunner()
        with patch(
            "proto_pipe.cli.errors.config_path_or_override",
            side_effect=_cfg(errors_db, src_cfg_path),
        ):
            runner.invoke(source_clear, ["sales", "--yes"])

        with duckdb.connect(errors_db) as conn:
            after_count = conn.execute("SELECT count(*) FROM sales").fetchone()[0]

        assert after_count == before_count, (
            "vp errors source clear must not modify the source table"
        )

    def test_clear_without_yes_prompts(self, errors_db, src_cfg_path):
        from proto_pipe.cli.errors import source_clear

        runner = CliRunner()
        with patch(
            "proto_pipe.cli.errors.config_path_or_override",
            side_effect=_cfg(errors_db, src_cfg_path),
        ):
            # Send 'n' to the prompt
            result = runner.invoke(source_clear, ["sales"], input="n\n")

        # Rows should NOT be cleared
        with duckdb.connect(errors_db) as conn:
            count = conn.execute(
                "SELECT count(*) FROM source_block WHERE table_name = 'sales'"
            ).fetchone()[0]
        assert count == 3


# ---------------------------------------------------------------------------
# vp errors report clear
# ---------------------------------------------------------------------------

class TestVpErrorsReportClear:
    def test_clear_removes_validation_rows(self, errors_db, src_cfg_path):
        from proto_pipe.cli.errors import report_clear

        runner = CliRunner()
        with patch(
            "proto_pipe.cli.errors.config_path_or_override",
            side_effect=_cfg(errors_db, src_cfg_path),
        ):
            result = runner.invoke(report_clear, ["sales_report", "--yes"])

        assert result.exit_code == 0, result.output

        with duckdb.connect(errors_db) as conn:
            count = conn.execute(
                "SELECT count(*) FROM validation_block WHERE report_name = 'sales_report'"
            ).fetchone()[0]
        assert count == 0


# ---------------------------------------------------------------------------
# vp errors report — grouped by cause
# ---------------------------------------------------------------------------

class TestVpErrorsReport:
    def test_shows_grouped_by_cause(self, errors_db, src_cfg_path):
        from proto_pipe.cli.errors import errors_cmd

        runner = CliRunner()
        with patch(
            "proto_pipe.cli.errors.config_path_or_override",
            side_effect=_cfg(errors_db, src_cfg_path),
        ):
            result = runner.invoke(errors_cmd, ["report"])

        assert result.exit_code == 0, result.output
        assert "null_check" in result.output
        assert "range_check" in result.output

    def test_prescriptive_fix_commands(self, errors_db, src_cfg_path):
        from proto_pipe.cli.errors import errors_cmd

        runner = CliRunner()
        with patch(
            "proto_pipe.cli.errors.config_path_or_override",
            side_effect=_cfg(errors_db, src_cfg_path),
        ):
            result = runner.invoke(errors_cmd, ["report"])

        assert "vp errors report export" in result.output
