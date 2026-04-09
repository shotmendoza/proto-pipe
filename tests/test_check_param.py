"""Tests for check_params_history write path — rule 14.

Verifies that _fill_params writes column param selections to
check_params_history via record_param_history, so get_param_suggestions
can surface them as cross-source suggestions for the same check+param.

"""
from __future__ import annotations

import contextlib
from unittest.mock import MagicMock, call, patch

import pandas as pd
import pytest

from proto_pipe.checks.registry import CheckRegistry
from proto_pipe.cli.prompts import ReportConfigPrompter


# ---------------------------------------------------------------------------
# Helpers (reused from test_report_prompter pattern)
# ---------------------------------------------------------------------------

def _make_col_check():
    def f(col: str) -> "pd.Series[bool]":
        return pd.Series([True])
    return f


def _make_prompter_single(registry):
    return ReportConfigPrompter(
        check_registry=registry,
        p_db="/fake/path.db",
        multi_select=False,
    )


def _make_prompter_multi(registry):
    return ReportConfigPrompter(
        check_registry=registry,
        p_db="/fake/path.db",
        multi_select=True,
    )


@contextlib.contextmanager
def _fill_params_stubs(table_cols, registry_types_result):
    with patch("proto_pipe.cli.scaffold.get_table_columns", return_value=table_cols), \
         patch("proto_pipe.cli.scaffold.get_param_suggestions", return_value=[]), \
         patch("proto_pipe.io.db.get_registry_types", return_value=registry_types_result), \
         patch("proto_pipe.cli.prompts.click.echo"):
        yield


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestCheckParamsHistoryWritePath:
    """Rule 14: column param selections must be written to check_params_history."""

    @pytest.fixture()
    def registry(self):
        reg = CheckRegistry()
        reg.register("col_check", _make_col_check(), kind="check")
        return reg

    def test_column_param_written_to_history_on_single_select(self, registry):
        """Single-select column param: record_param_history called with the
        selected column name so it surfaces as a cross-source suggestion."""
        prompter = _make_prompter_single(registry)
        conn = MagicMock()

        mock_select = MagicMock()
        mock_select.return_value.ask.return_value = "endt"

        mock_record = MagicMock()

        with _fill_params_stubs(["endt", "price"], {"endt": "DATE", "price": "DOUBLE"}), \
             patch("proto_pipe.cli.prompts.questionary.select", mock_select), \
             patch("proto_pipe.cli.scaffold.record_param_history", mock_record):

            prompter.prompt_params(
                selected_checks=["col_check"],
                table="van",
                conn=conn,
                report_name="van_report",
            )

        # record_param_history must have been called with the column name
        # as the param value — not just with filled_params (which excludes cols)
        col_history_calls = [
            c for c in mock_record.call_args_list
            if c.args[4].get("col") == "endt"
        ]
        assert col_history_calls, (
            "Expected record_param_history called with {'col': 'endt'} "
            f"for column param. All calls: {mock_record.call_args_list}"
        )

    def test_multiselect_writes_one_entry_per_column(self, registry):
        """Multi-select: one record_param_history call per selected column,
        not one call with the full list — because _similar_columns matches
        individual column name strings."""
        prompter = _make_prompter_multi(registry)
        conn = MagicMock()

        mock_cb = MagicMock()
        mock_cb.return_value.ask.return_value = ["endt", "expiry_date"]

        mock_record = MagicMock()

        with _fill_params_stubs(
            ["endt", "expiry_date", "price"],
            {"endt": "DATE", "expiry_date": "DATE", "price": "DOUBLE"},
        ), \
             patch("proto_pipe.cli.prompts.questionary.checkbox", mock_cb), \
             patch("proto_pipe.cli.scaffold.record_param_history", mock_record):

            prompter.prompt_params(
                selected_checks=["col_check"],
                table="van",
                conn=conn,
                report_name="van_report",
            )

        col_history_calls = [
            c for c in mock_record.call_args_list
            if "col" in c.args[4]
        ]
        recorded_cols = {c.args[4]["col"] for c in col_history_calls}

        assert "endt" in recorded_cols, (
            "Expected 'endt' written to history individually"
        )
        assert "expiry_date" in recorded_cols, (
            "Expected 'expiry_date' written to history individually"
        )
        assert len(col_history_calls) == 2, (
            f"Expected 2 history entries (one per column), got {len(col_history_calls)}"
        )

    def test_history_not_written_for_scalar_params(self, registry):
        """Scalar params must not appear in check_params_history via the
        column param write path — they go through filled_params only."""
        from proto_pipe.checks.registry import CheckRegistry

        def check_with_scalar(col: pd.Series, threshold: float) -> "pd.Series[bool]":
            return pd.Series([True])

        reg = CheckRegistry()
        reg.register("scalar_check", check_with_scalar, kind="check")
        prompter = _make_prompter_single(reg)
        conn = MagicMock()

        mock_select = MagicMock()
        mock_select.return_value.ask.return_value = "endt"
        mock_text = MagicMock()
        mock_text.return_value.ask.return_value = "0.5"

        mock_record = MagicMock()

        with _fill_params_stubs(["endt", "price"], {"endt": "DATE", "price": "DOUBLE"}), \
             patch("proto_pipe.cli.prompts.questionary.select", mock_select), \
             patch("proto_pipe.cli.prompts.questionary.text", mock_text), \
             patch("proto_pipe.cli.scaffold.record_param_history", mock_record):

            prompter.prompt_params(
                selected_checks=["scalar_check"],
                table="van",
                conn=conn,
                report_name="van_report",
            )

        # The column param write should record col → endt
        col_writes = [c for c in mock_record.call_args_list if c.args[4].get("col") == "endt"]
        assert col_writes, "Column param must be written to history"

        # threshold must NOT appear in any column-param history call
        threshold_col_writes = [
            c for c in mock_record.call_args_list
            if "threshold" in c.args[4] and c.args[4]["threshold"] == "endt"
        ]
        assert not threshold_col_writes, (
            "Scalar param 'threshold' must not appear in column param history path"
        )

    def test_column_param_history_uses_check_name_and_param_name(self, registry):
        """History entries are keyed by check name and param name so
        get_param_suggestions can find them for the right check+param combo."""
        prompter = _make_prompter_single(registry)
        conn = MagicMock()

        mock_select = MagicMock()
        mock_select.return_value.ask.return_value = "endt"

        mock_record = MagicMock()

        with _fill_params_stubs(["endt"], {"endt": "DATE"}), \
             patch("proto_pipe.cli.prompts.questionary.select", mock_select), \
             patch("proto_pipe.cli.scaffold.record_param_history", mock_record):

            prompter.prompt_params(
                selected_checks=["col_check"],
                table="van",
                conn=conn,
                report_name="van_report",
            )

        col_calls = [c for c in mock_record.call_args_list if c.args[4].get("col") == "endt"]
        assert col_calls, "Expected at least one history call for the column param"

        # Verify the call includes check_name="col_check" and report_name
        first = col_calls[0]
        assert first.args[1] == "col_check", (
            f"Expected check_name='col_check', got {first.args[1]!r}"
        )
