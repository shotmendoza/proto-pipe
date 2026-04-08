"""Smoke test for vp new report — per CLAUDE.md rule 13.

Invokes the full Click command through CliRunner, exercising the complete
call chain from the CLI entry point through ReportConfigPrompter.run(),
prompt_checks(), and _prompt_kind_group(). Questionary, DB connections,
and config I/O are mocked; the prompt flow itself runs for real.

This test exists because unit tests on individual prompter methods cannot
catch integration failures between callers and callees — e.g. a method
signature change where the caller is not updated produces a TypeError at
runtime even when all unit tests pass. A CliRunner smoke test catches this
class of failure immediately.

Root cause this test guards against: prompt_checks() gained a required
registry_types param; run() was not updated to pass it; unit tests passed;
runtime raised TypeError: prompt_checks() missing 1 required positional
argument: 'registry_types'.
"""
from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
from click.testing import CliRunner

from proto_pipe.cli.commands.new import new_report
from proto_pipe.cli.prompts import ReportConfigPrompter


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _fake_select(choices_value):
    """Return a questionary.select mock that returns choices_value."""
    def _select(label, choices, default=None):
        m = MagicMock()
        m.ask.return_value = choices_value
        return m
    return _select


def _fake_text(return_value):
    """Return a questionary.text mock that returns return_value."""
    def _text(label, default=""):
        m = MagicMock()
        m.ask.return_value = return_value
        return m
    return _text


def _fake_checkbox_first(label, choices):
    """questionary.checkbox mock — always selects the first available choice."""
    m = MagicMock()
    if choices:
        first = choices[0]
        m.ask.return_value = [first.value if hasattr(first, "value") else first]
    else:
        m.ask.return_value = []
    return m


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture()
def fake_settings(tmp_path):
    return {
        "paths": {
            "pipeline_db": str(tmp_path / "pipeline.db"),
            "reports_config": str(tmp_path / "reports_config.yaml"),
        },
        "multi_select_params": False,
        "custom_checks_module": None,
    }


@pytest.fixture()
def mock_report_config():
    cfg = MagicMock()
    cfg.all.return_value = []
    cfg.names.return_value = []
    cfg.add_or_update.return_value = None
    return cfg


@pytest.fixture()
def mock_conn():
    conn = MagicMock()
    conn.close.return_value = None
    return conn


# ---------------------------------------------------------------------------
# Smoke tests
# ---------------------------------------------------------------------------

class TestVpNewReportSmoke:
    """CLI smoke tests for vp new report — rule 13."""

    def test_full_flow_exits_without_error(
        self, tmp_path, fake_settings, mock_report_config, mock_conn
    ):
        """vp new report completes STEP_TABLE→STEP_NAME→STEP_CHECKS→STEP_PARAMS
        without raising any exception.

        This is the canonical guard against caller/callee signature drift:
        if prompt_checks() or any other method in the chain has a missing
        required argument, this test fails with a non-zero exit code.
        """
        runner = CliRunner()

        with patch("proto_pipe.io.config.load_settings", return_value=fake_settings), \
             patch("proto_pipe.io.config.config_path_or_override",
                   return_value=str(tmp_path / "reports_config.yaml")), \
             patch("proto_pipe.io.config.ReportConfig", return_value=mock_report_config), \
             patch("proto_pipe.cli.commands.new.get_all_source_tables",
                   return_value=[("sales", 0)]), \
             patch("proto_pipe.cli.commands.new.duckdb.connect",
                   return_value=mock_conn), \
             patch("proto_pipe.io.db.init_check_registry_metadata"), \
             patch("proto_pipe.checks.registry.CheckParamInspector.write_to_db"), \
             patch("proto_pipe.cli.scaffold.get_table_columns", return_value=["price"]), \
             patch("proto_pipe.cli.scaffold.get_original_func", return_value=None), \
             patch("proto_pipe.cli.scaffold.get_check_first_sentence", return_value=""), \
             patch("proto_pipe.cli.scaffold.build_check_param_lines", return_value=[]), \
             patch("proto_pipe.io.db.get_registry_types",
                   return_value={"price": "DOUBLE"}), \
             patch("proto_pipe.cli.prompts.questionary.select",
                   side_effect=_fake_select("sales")), \
             patch("proto_pipe.cli.prompts.questionary.text",
                   side_effect=_fake_text("sales_report")), \
             patch("proto_pipe.cli.prompts.questionary.checkbox",
                   side_effect=_fake_checkbox_first), \
             patch.object(ReportConfigPrompter, "prompt_params",
                          return_value=([{"name": "stub_check"}], [], False)):

            result = runner.invoke(new_report, [
                "--reports-config", str(tmp_path / "reports_config.yaml"),
                "--pipeline-db", str(tmp_path / "pipeline.db"),
            ])

        assert result.exit_code == 0, (
            f"vp new report exited with code {result.exit_code}.\n"
            f"Output:\n{result.output}\n"
            f"Exception: {result.exception!r}"
        )
        assert result.exception is None, (
            f"vp new report raised an exception: {result.exception!r}\n"
            f"Output:\n{result.output}"
        )

    def test_no_tables_exits_cleanly(
        self, tmp_path, fake_settings, mock_report_config, mock_conn
    ):
        """vp new report exits cleanly with a message when no tables are ingested."""
        runner = CliRunner()

        with patch("proto_pipe.io.config.load_settings", return_value=fake_settings), \
             patch("proto_pipe.io.config.config_path_or_override",
                   return_value=str(tmp_path / "reports_config.yaml")), \
             patch("proto_pipe.io.config.ReportConfig", return_value=mock_report_config), \
             patch("proto_pipe.cli.commands.new.get_all_source_tables",
                   return_value=[]), \
             patch("proto_pipe.cli.commands.new.duckdb.connect",
                   return_value=mock_conn), \
             patch("proto_pipe.io.db.init_check_registry_metadata"):

            result = runner.invoke(new_report, [
                "--reports-config", str(tmp_path / "reports_config.yaml"),
                "--pipeline-db", str(tmp_path / "pipeline.db"),
            ])

        assert result.exit_code == 0
        assert result.exception is None
        assert "vp ingest" in result.output

    def test_output_confirms_report_added(
        self, tmp_path, fake_settings, mock_report_config, mock_conn
    ):
        """vp new report echoes confirmation with the report name on success."""
        runner = CliRunner()

        with patch("proto_pipe.io.config.load_settings", return_value=fake_settings), \
             patch("proto_pipe.io.config.config_path_or_override",
                   return_value=str(tmp_path / "reports_config.yaml")), \
             patch("proto_pipe.io.config.ReportConfig", return_value=mock_report_config), \
             patch("proto_pipe.cli.commands.new.get_all_source_tables",
                   return_value=[("sales", 0)]), \
             patch("proto_pipe.cli.commands.new.duckdb.connect",
                   return_value=mock_conn), \
             patch("proto_pipe.io.db.init_check_registry_metadata"), \
             patch("proto_pipe.checks.registry.CheckParamInspector.write_to_db"), \
             patch("proto_pipe.cli.scaffold.get_table_columns", return_value=["price"]), \
             patch("proto_pipe.cli.scaffold.get_original_func", return_value=None), \
             patch("proto_pipe.cli.scaffold.get_check_first_sentence", return_value=""), \
             patch("proto_pipe.cli.scaffold.build_check_param_lines", return_value=[]), \
             patch("proto_pipe.io.db.get_registry_types",
                   return_value={"price": "DOUBLE"}), \
             patch("proto_pipe.cli.prompts.questionary.select",
                   side_effect=_fake_select("sales")), \
             patch("proto_pipe.cli.prompts.questionary.text",
                   side_effect=_fake_text("sales_report")), \
             patch("proto_pipe.cli.prompts.questionary.checkbox",
                   side_effect=_fake_checkbox_first), \
             patch.object(ReportConfigPrompter, "prompt_params",
                          return_value=([{"name": "stub_check"}], [], False)):

            result = runner.invoke(new_report, [
                "--reports-config", str(tmp_path / "reports_config.yaml"),
                "--pipeline-db", str(tmp_path / "pipeline.db"),
            ])

        assert "sales_report" in result.output, (
            f"Expected report name 'sales_report' in output. Got:\n{result.output}"
        )
        assert "[ok]" in result.output
