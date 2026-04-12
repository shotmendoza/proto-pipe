"""Tests for _fill_params fixes — Issues 1-4.

Issue 1: check_params_history DDL missing → table never created.
Issue 2: No escape hatch in checkbox path for column-backed scalars.
Issue 3: Prompt allows multi-select but is_expandable() blocks expansion.
Issue 4: ESC during param config cancels entire wizard instead of skipping function.

Unit tests for prompts.py and db.py.
"""
from __future__ import annotations

import contextlib
from unittest.mock import MagicMock, patch, call

import pandas as pd
import pytest

from proto_pipe.checks.registry import CheckRegistry, CheckParamInspector
from proto_pipe.cli.prompts import ReportConfigPrompter, _DIRECT_ENTRY


# ---------------------------------------------------------------------------
# Helpers  (shared with test_col_history_preselect.py pattern)
# ---------------------------------------------------------------------------


def _make_prompter(registry, multi_select=True):
    return ReportConfigPrompter(
        check_registry=registry,
        p_db="/fake/path.db",
        multi_select=multi_select,
    )


@contextlib.contextmanager
def _fill_params_stubs(table_cols, registry_types_result, col_history=None):
    """Stubs for DB-touching helpers."""
    with patch("proto_pipe.io.db.get_table_columns", return_value=table_cols), \
         patch("proto_pipe.io.db.get_param_suggestions", return_value=[]), \
         patch("proto_pipe.io.db.get_column_param_history",
               return_value=col_history or []), \
         patch("proto_pipe.io.db.record_param_history"), \
         patch("proto_pipe.io.db.get_registry_types", return_value=registry_types_result), \
         patch("proto_pipe.cli.prompts.click.echo"):
        yield


# ===========================================================================
# Issue 1 — check_params_history DDL
# ===========================================================================


class TestInitCheckParamsHistory:
    """init_all_pipeline_tables creates check_params_history."""

    def test_table_created_by_init_all(self, tmp_path):
        """check_params_history table exists after init_all_pipeline_tables."""
        import duckdb
        from proto_pipe.io.db import init_all_pipeline_tables

        db_path = str(tmp_path / "test.db")
        conn = duckdb.connect(db_path)
        init_all_pipeline_tables(conn)

        tables = [
            row[0]
            for row in conn.execute(
                "SELECT table_name FROM information_schema.tables"
            ).fetchall()
        ]
        assert "check_params_history" in tables

    def test_record_and_read_roundtrip(self, tmp_path):
        """record_param_history writes, get_column_param_history reads back."""
        import duckdb
        from proto_pipe.io.db import (
            init_all_pipeline_tables,
            record_param_history,
            get_column_param_history,
        )

        db_path = str(tmp_path / "test.db")
        conn = duckdb.connect(db_path)
        init_all_pipeline_tables(conn)

        record_param_history(
            conn, "my_check", "my_report", "my_table", {"col": "premium"}
        )

        result = get_column_param_history(conn, "my_check", "col")
        assert result == ["premium"]


# ===========================================================================
# Issue 2 — escape hatch in checkbox path
# ===========================================================================


class TestCheckboxEscapeHatch:
    """Column-backed scalar params show _DIRECT_ENTRY in the checkbox
    when eligible=True and the param is not a pd.Series param."""

    @pytest.fixture()
    def registry_expandable_transform(self):
        """Transform with pd.Series return (expandable) and a scalar param."""
        def my_transform(col: str, threshold: int = 10) -> "pd.Series":
            pass
        reg = CheckRegistry()
        reg.register("my_transform", my_transform, kind="transform")
        return reg

    def test_direct_entry_in_checkbox_for_scalar_param(
        self, registry_expandable_transform
    ):
        """When eligible=True, column-backed scalar param shows _DIRECT_ENTRY."""
        prompter = _make_prompter(registry_expandable_transform)
        conn = MagicMock()

        checkbox_calls = []

        def capture_checkbox(label, choices):
            checkbox_calls.append((label, choices))
            m = MagicMock()
            # Return the direct entry option for the scalar param
            if "threshold" in label:
                m.ask.return_value = [_DIRECT_ENTRY]
            else:
                m.ask.return_value = ["col_a"]
            return m

        def capture_text(label, default=""):
            m = MagicMock()
            m.ask.return_value = "42"
            return m

        def capture_select(label, choices, default=None):
            m = MagicMock()
            m.ask.return_value = (
                choices[0].value if hasattr(choices[0], "value") else choices[0]
            )
            return m

        with _fill_params_stubs(
            ["col_a", "col_b"],
            {"col_a": "VARCHAR", "col_b": "DOUBLE"},
        ), patch(
            "proto_pipe.cli.prompts.questionary.checkbox",
            side_effect=capture_checkbox,
        ), patch(
            "proto_pipe.cli.prompts.questionary.text",
            side_effect=capture_text,
        ), patch(
            "proto_pipe.cli.prompts.questionary.select",
            side_effect=capture_select,
        ):
            entries, alias, go_back = prompter._fill_params(
                selected_checks=["my_transform"],
                table="sales",
                conn=conn,
                report_name="test_report",
            )

        assert not go_back

        # Find the checkbox call for threshold
        threshold_calls = [
            (label, choices)
            for label, choices in checkbox_calls
            if "threshold" in label
        ]
        assert threshold_calls, "threshold param should have a checkbox call"
        _, threshold_choices = threshold_calls[0]
        choice_values = [c.value for c in threshold_choices]
        assert _DIRECT_ENTRY in choice_values, (
            "Checkbox for scalar param must include _DIRECT_ENTRY"
        )

        # Verify the value ended up in filled_params, not alias_map
        entry = next(e for e in entries if e["name"] == "my_transform")
        assert entry["params"]["threshold"] == 42
        assert not any(
            e["param"] == "threshold" for e in alias
        ), "Direct entry should not appear in alias_map"


# ===========================================================================
# Issue 3 — eligible gated on is_expandable
# ===========================================================================


class TestEligibleGatedOnExpandable:
    """Functions returning non-Series/DataFrame (e.g. str) get single-select
    even when they are transforms with column params."""

    @pytest.fixture()
    def registry_str_return_transform(self):
        """Transform returning str — not expandable."""
        def my_transform(a: str, b: int, c: bool = False) -> str:
            pass
        reg = CheckRegistry()
        reg.register("my_transform", my_transform, kind="transform")
        return reg

    def test_str_return_transform_gets_single_select(
        self, registry_str_return_transform
    ):
        """A -> str transform must use questionary.select, not checkbox."""
        prompter = _make_prompter(registry_str_return_transform)
        conn = MagicMock()

        select_calls = []
        checkbox_calls = []

        def capture_select(label, choices, default=None):
            select_calls.append(label)
            m = MagicMock()
            m.ask.return_value = (
                choices[0].value if hasattr(choices[0], "value") else choices[0]
            )
            return m

        def capture_checkbox(label, choices):
            checkbox_calls.append(label)
            m = MagicMock()
            m.ask.return_value = [choices[0].value]
            return m

        def capture_text(label, default=""):
            m = MagicMock()
            m.ask.return_value = "42"
            return m

        with _fill_params_stubs(
            ["col_a", "col_b"],
            {"col_a": "VARCHAR", "col_b": "BIGINT"},
        ), patch(
            "proto_pipe.cli.prompts.questionary.select",
            side_effect=capture_select,
        ), patch(
            "proto_pipe.cli.prompts.questionary.checkbox",
            side_effect=capture_checkbox,
        ), patch(
            "proto_pipe.cli.prompts.questionary.text",
            side_effect=capture_text,
        ):
            prompter._fill_params(
                selected_checks=["my_transform"],
                table="sales",
                conn=conn,
                report_name="test_report",
            )

        # Column-picker params (a, b, c) should all hit select, not checkbox
        assert len(select_calls) > 0, "Should use select for non-expandable transform"
        assert len(checkbox_calls) == 0, (
            "Checkbox must not be used for non-expandable transform"
        )

    def test_is_expandable_false_for_str_return(self):
        """CheckParamInspector.is_expandable() is False for -> str."""
        def my_func(a: str) -> str:
            pass
        inspector = CheckParamInspector(my_func)
        assert not inspector.is_expandable()

    def test_is_expandable_true_for_series_return(self):
        """CheckParamInspector.is_expandable() is True for -> pd.Series."""
        def my_func(a: str) -> "pd.Series":
            pass
        inspector = CheckParamInspector(my_func)
        assert inspector.is_expandable()

    def test_str_return_transform_has_escape_hatch(
        self, registry_str_return_transform
    ):
        """Non-expandable transform with scalar params shows _DIRECT_ENTRY
        in single-select for each column-backed scalar param."""
        prompter = _make_prompter(registry_str_return_transform)
        conn = MagicMock()

        select_choice_values = {}

        def capture_select(label, choices, default=None):
            select_choice_values[label] = [
                c.value if hasattr(c, "value") else c for c in choices
            ]
            m = MagicMock()
            m.ask.return_value = (
                choices[0].value if hasattr(choices[0], "value") else choices[0]
            )
            return m

        def capture_text(label, default=""):
            m = MagicMock()
            m.ask.return_value = "test"
            return m

        with _fill_params_stubs(
            ["col_a", "col_b"],
            {"col_a": "VARCHAR", "col_b": "BIGINT"},
        ), patch(
            "proto_pipe.cli.prompts.questionary.select",
            side_effect=capture_select,
        ), patch(
            "proto_pipe.cli.prompts.questionary.text",
            side_effect=capture_text,
        ):
            prompter._fill_params(
                selected_checks=["my_transform"],
                table="sales",
                conn=conn,
                report_name="test_report",
            )

        # b and c are column-backed scalars — both should have _DIRECT_ENTRY
        for label in select_choice_values:
            if "b:" in label or "c:" in label:
                assert _DIRECT_ENTRY in select_choice_values[label], (
                    f"{label} must include _DIRECT_ENTRY for scalar params"
                )


# ===========================================================================
# Issue 4 — ESC skips function, not entire wizard
# ===========================================================================


class TestEscSkipsFunction:
    """ESC during param configuration drops the current function and
    continues with the next, rather than aborting the entire wizard."""

    @pytest.fixture()
    def two_check_registry(self):
        """Two checks: check_a has a param, check_b has a param."""
        def check_a(col: str) -> "pd.Series[bool]":
            pass
        def check_b(col: str) -> "pd.Series[bool]":
            pass
        reg = CheckRegistry()
        reg.register("check_a", check_a, kind="check")
        reg.register("check_b", check_b, kind="check")
        return reg

    def test_esc_on_first_function_skips_it_keeps_second(
        self, two_check_registry
    ):
        """User ESCs on check_a's param, confirms skip → check_b still runs."""
        prompter = _make_prompter(two_check_registry, multi_select=False)
        conn = MagicMock()

        select_call = [0]

        def capture_select(label, choices, default=None):
            select_call[0] += 1
            m = MagicMock()
            if select_call[0] == 1:
                # First call = check_a's param → ESC (None)
                m.ask.return_value = None
            else:
                # Second call = check_b's param → pick first col
                m.ask.return_value = (
                    choices[0].value
                    if hasattr(choices[0], "value")
                    else choices[0]
                )
            return m

        def capture_confirm(label, default=True):
            m = MagicMock()
            m.ask.return_value = True  # Yes, skip this function
            return m

        with _fill_params_stubs(
            ["col_a", "col_b"],
            {"col_a": "VARCHAR", "col_b": "VARCHAR"},
        ), patch(
            "proto_pipe.cli.prompts.questionary.select",
            side_effect=capture_select,
        ), patch(
            "proto_pipe.cli.prompts.questionary.confirm",
            side_effect=capture_confirm,
        ):
            entries, alias, go_back = prompter._fill_params(
                selected_checks=["check_a", "check_b"],
                table="sales",
                conn=conn,
                report_name="test_report",
            )

        assert not go_back, "Should not abort wizard when user chose to skip"
        names = [e["name"] for e in entries]
        assert "check_a" not in names, "Skipped function must not be in entries"
        assert "check_b" in names, "Non-skipped function must be in entries"

    def test_esc_then_abort_returns_go_back(self, two_check_registry):
        """User ESCs on check_a's param, declines skip → go_back=True."""
        prompter = _make_prompter(two_check_registry, multi_select=False)
        conn = MagicMock()

        def capture_select(label, choices, default=None):
            m = MagicMock()
            m.ask.return_value = None  # ESC
            return m

        def capture_confirm(label, default=True):
            m = MagicMock()
            m.ask.return_value = False  # No, abort entire wizard
            return m

        with _fill_params_stubs(
            ["col_a"],
            {"col_a": "VARCHAR"},
        ), patch(
            "proto_pipe.cli.prompts.questionary.select",
            side_effect=capture_select,
        ), patch(
            "proto_pipe.cli.prompts.questionary.confirm",
            side_effect=capture_confirm,
        ):
            entries, alias, go_back = prompter._fill_params(
                selected_checks=["check_a", "check_b"],
                table="sales",
                conn=conn,
                report_name="test_report",
            )

        assert go_back, "Should return go_back=True when user declines skip"
        assert entries == []

    def test_skip_rolls_back_alias_entries(self, two_check_registry):
        """When a function is skipped, its alias_map entries are removed."""
        # Use multi-select registry where alias entries get added before ESC
        def transform_a(col: str) -> "pd.Series":
            pass
        def check_b(col: str) -> "pd.Series[bool]":
            pass
        reg = CheckRegistry()
        reg.register("transform_a", transform_a, kind="transform")
        reg.register("check_b", check_b, kind="check")

        prompter = _make_prompter(reg, multi_select=True)
        conn = MagicMock()

        checkbox_call = [0]

        def capture_checkbox(label, choices):
            checkbox_call[0] += 1
            m = MagicMock()
            if checkbox_call[0] == 1:
                # transform_a col param → select a column
                m.ask.return_value = ["col_a"]
            else:
                # check_b col param → select a column
                m.ask.return_value = ["col_b"]
            return m

        select_call = [0]

        def capture_select(label, choices, default=None):
            select_call[0] += 1
            m = MagicMock()
            # This is the _output prompt for transform_a → ESC
            m.ask.return_value = None
            return m

        def capture_confirm(label, default=True):
            m = MagicMock()
            m.ask.return_value = True  # Yes, skip transform_a
            return m

        with _fill_params_stubs(
            ["col_a", "col_b"],
            {"col_a": "VARCHAR", "col_b": "VARCHAR"},
        ), patch(
            "proto_pipe.cli.prompts.questionary.checkbox",
            side_effect=capture_checkbox,
        ), patch(
            "proto_pipe.cli.prompts.questionary.select",
            side_effect=capture_select,
        ), patch(
            "proto_pipe.cli.prompts.questionary.confirm",
            side_effect=capture_confirm,
        ):
            entries, alias, go_back = prompter._fill_params(
                selected_checks=["transform_a", "check_b"],
                table="sales",
                conn=conn,
                report_name="test_report",
            )

        assert not go_back
        # transform_a's alias entries (col_a) must be rolled back
        transform_a_aliases = [e for e in alias if e.get("column") == "col_a"]
        assert not transform_a_aliases, (
            "Skipped function's alias entries must be rolled back"
        )
        # check_b's alias entries (col_b) must survive
        check_b_aliases = [e for e in alias if e.get("column") == "col_b"]
        assert check_b_aliases, "Non-skipped function's alias entries must survive"
