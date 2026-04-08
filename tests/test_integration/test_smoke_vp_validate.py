"""CLI smoke test for `vp validate` — rule 13 compliance.

Patching strategy per test_cli_commands.py convention:
  - config_path_or_override → command module namespace (module-level import)
  - load_config             → command module namespace (module-level import)
  - load_custom_checks      → proto_pipe.checks.helpers (lazy import inside fn)
  - register_from_config    → proto_pipe.io.registry (lazy import inside fn)
  - run_all_reports         → proto_pipe.reports.runner (lazy import inside fn)
  - write_pipeline_events   → proto_pipe.io.db (lazy import inside fn)
  - pipeline DB             → real temp DB initialized with pipeline tables
"""
from __future__ import annotations

import duckdb
import pytest
from click.testing import CliRunner
from unittest.mock import patch

from proto_pipe.cli.commands.validation import validate
from proto_pipe.io.db import init_all_pipeline_tables


def _cfg(pipeline_db, watermark_db, reports_config):
    mapping = {
        "pipeline_db": pipeline_db,
        "watermark_db": watermark_db,
        "reports_config": reports_config,
    }
    return lambda key, override=None: mapping.get(key, override or key)


def _init_db(pipeline_db):
    with duckdb.connect(pipeline_db) as conn:
        init_all_pipeline_tables(conn)


def _completed(name="sales_report"):
    return [{"report": name, "status": "completed", "results": {}}]


def _skipped(name="sales_report"):
    return [{"report": name, "status": "skipped"}]


_EMPTY_CONFIG = {"templates": {}, "reports": []}


class TestVpValidateSmoke:

    def test_validate_exits_cleanly_with_no_reports(self, tmp_path, pipeline_db, watermark_db):
        """vp validate with zero registered reports exits cleanly."""
        _init_db(pipeline_db)
        rep_cfg = str(tmp_path / "reports_config.yaml")
        runner = CliRunner()
        with (
            patch("proto_pipe.cli.commands.validation.config_path_or_override", side_effect=_cfg(pipeline_db, watermark_db, rep_cfg)),
            patch("proto_pipe.cli.commands.validation.load_config", return_value=_EMPTY_CONFIG),
            patch("proto_pipe.checks.helpers.load_custom_checks"),
            patch("proto_pipe.io.registry.register_from_config"),
            patch("proto_pipe.reports.runner.run_all_reports", return_value=[]),
            patch("proto_pipe.io.db.write_pipeline_events"),
        ):
            result = runner.invoke(validate)
        assert result.exit_code == 0, result.output

    def test_validate_completed_report_shows_status(self, tmp_path, pipeline_db, watermark_db):
        """Completed report name appears in output."""
        _init_db(pipeline_db)
        rep_cfg = str(tmp_path / "reports_config.yaml")
        runner = CliRunner()
        with (
            patch("proto_pipe.cli.commands.validation.config_path_or_override", side_effect=_cfg(pipeline_db, watermark_db, rep_cfg)),
            patch("proto_pipe.cli.commands.validation.load_config", return_value=_EMPTY_CONFIG),
            patch("proto_pipe.checks.helpers.load_custom_checks"),
            patch("proto_pipe.io.registry.register_from_config"),
            patch("proto_pipe.reports.runner.run_all_reports", return_value=_completed("sales_report")),
            patch("proto_pipe.io.db.write_pipeline_events"),
        ):
            result = runner.invoke(validate)
        assert result.exit_code == 0, result.output
        assert "sales_report" in result.output

    def test_validate_skipped_report_shows_no_pending(self, tmp_path, pipeline_db, watermark_db):
        """Skipped report shows 'No pending records' message."""
        _init_db(pipeline_db)
        rep_cfg = str(tmp_path / "reports_config.yaml")
        runner = CliRunner()
        with (
            patch("proto_pipe.cli.commands.validation.config_path_or_override", side_effect=_cfg(pipeline_db, watermark_db, rep_cfg)),
            patch("proto_pipe.cli.commands.validation.load_config", return_value=_EMPTY_CONFIG),
            patch("proto_pipe.checks.helpers.load_custom_checks"),
            patch("proto_pipe.io.registry.register_from_config"),
            patch("proto_pipe.reports.runner.run_all_reports", return_value=_skipped()),
            patch("proto_pipe.io.db.write_pipeline_events"),
        ):
            result = runner.invoke(validate)
        assert result.exit_code == 0, result.output
        assert "No pending records" in result.output

    def test_validate_full_flag_passes_to_run_all_reports(self, tmp_path, pipeline_db, watermark_db):
        """vp validate --full passes full_revalidation=True to run_all_reports."""
        _init_db(pipeline_db)
        rep_cfg = str(tmp_path / "reports_config.yaml")
        captured = {}

        def _capture(*args, **kwargs):
            captured.update(kwargs)
            return []

        runner = CliRunner()
        with (
            patch("proto_pipe.cli.commands.validation.config_path_or_override", side_effect=_cfg(pipeline_db, watermark_db, rep_cfg)),
            patch("proto_pipe.cli.commands.validation.load_config", return_value=_EMPTY_CONFIG),
            patch("proto_pipe.checks.helpers.load_custom_checks"),
            patch("proto_pipe.io.registry.register_from_config"),
            patch("proto_pipe.reports.runner.run_all_reports", side_effect=_capture),
            patch("proto_pipe.io.db.write_pipeline_events"),
        ):
            result = runner.invoke(validate, ["--full"])
        assert result.exit_code == 0, result.output
        assert captured.get("full_revalidation") is True, (
            "--full must pass full_revalidation=True to run_all_reports"
        )

    def test_validate_table_filter_warns_no_matching_reports(self, tmp_path, pipeline_db, watermark_db):
        """vp validate --table with no matching reports shows warn."""
        _init_db(pipeline_db)
        rep_cfg = str(tmp_path / "reports_config.yaml")
        runner = CliRunner()
        with (
            patch("proto_pipe.cli.commands.validation.config_path_or_override", side_effect=_cfg(pipeline_db, watermark_db, rep_cfg)),
            patch("proto_pipe.cli.commands.validation.load_config", return_value=_EMPTY_CONFIG),
            patch("proto_pipe.checks.helpers.load_custom_checks"),
            patch("proto_pipe.io.registry.register_from_config"),
            patch("proto_pipe.reports.runner.run_all_reports", return_value=[]),
            patch("proto_pipe.io.db.write_pipeline_events"),
        ):
            result = runner.invoke(validate, ["--table", "nonexistent"])
        assert result.exit_code == 0, result.output
        assert "warn" in result.output.lower() or "no reports" in result.output.lower()

    def test_validate_no_failures_shows_success_message(self, tmp_path, pipeline_db, watermark_db):
        """Zero failures in validation_block produces success indicator."""
        _init_db(pipeline_db)
        rep_cfg = str(tmp_path / "reports_config.yaml")
        runner = CliRunner()
        with (
            patch("proto_pipe.cli.commands.validation.config_path_or_override", side_effect=_cfg(pipeline_db, watermark_db, rep_cfg)),
            patch("proto_pipe.cli.commands.validation.load_config", return_value=_EMPTY_CONFIG),
            patch("proto_pipe.checks.helpers.load_custom_checks"),
            patch("proto_pipe.io.registry.register_from_config"),
            patch("proto_pipe.reports.runner.run_all_reports", return_value=_completed()),
            patch("proto_pipe.io.db.write_pipeline_events"),
        ):
            result = runner.invoke(validate)
        assert result.exit_code == 0, result.output
        assert "No validation failures" in result.output or "✓" in result.output
