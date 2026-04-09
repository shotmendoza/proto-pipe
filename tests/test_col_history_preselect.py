"""Tests for cross-session column param pre-selection via check_params_history.

Behavioral guarantees (CLAUDE.md):
- Historical column selections for a check+param are pre-checked (checkbox)
  or pre-defaulted (select) when the same check+param is configured again.
- Within-session alias_cols are pre-checked/pre-defaulted.
- Historical values not present in the current source are not pre-selected
  (can't select a column that doesn't exist here).
- Exact param name == column name (auto_match) is still pre-selected as a
  fallback when no history or within-session choices exist.
- get_column_param_history returns raw values without similarity filtering.
"""
from __future__ import annotations

import contextlib
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from proto_pipe.checks.registry import CheckRegistry
from proto_pipe.cli.prompts import ReportConfigPrompter


# ---------------------------------------------------------------------------
# Helpers
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
def _fill_params_stubs(table_cols, registry_types_result, col_history=None):
    """Stubs for DB-touching helpers. col_history controls what
    get_column_param_history returns (default: empty list)."""
    with patch("proto_pipe.io.db.get_table_columns", return_value=table_cols), \
         patch("proto_pipe.io.db.get_param_suggestions", return_value=[]), \
         patch("proto_pipe.io.db.get_column_param_history",
               return_value=col_history or []), \
         patch("proto_pipe.io.db.record_param_history"), \
         patch("proto_pipe.io.db.get_registry_types", return_value=registry_types_result), \
         patch("proto_pipe.cli.prompts.click.echo"):
        yield


# ---------------------------------------------------------------------------
# get_column_param_history unit tests
# ---------------------------------------------------------------------------

class TestGetColumnParamHistory:

    def test_returns_raw_values_ordered_by_recency(self):
        """Raw historical values returned without similarity filtering."""
        from proto_pipe.io.db import get_column_param_history

        conn = MagicMock()
        conn.execute.return_value.fetchall.return_value = [
            ("expiry_date",), ("endt",), ("end_date",)
        ]

        result = get_column_param_history(conn, "null_check", "col")

        assert result == ["expiry_date", "endt", "end_date"]
        # Verify it queries by check_name + param_name
        call_args = conn.execute.call_args
        assert "null_check" in call_args.args[1]
        assert "col" in call_args.args[1]

    def test_returns_empty_on_db_error(self):
        """Falls back to empty list when check_params_history doesn't exist."""
        from proto_pipe.io.db import get_column_param_history

        conn = MagicMock()
        conn.execute.side_effect = Exception("Table not found")

        result = get_column_param_history(conn, "null_check", "col")

        assert result == []

    def test_filters_none_values(self):
        """None or empty param_value rows are excluded."""
        from proto_pipe.io.db import get_column_param_history

        conn = MagicMock()
        conn.execute.return_value.fetchall.return_value = [
            ("endt",), (None,), ("",), ("expiry_date",)
        ]

        result = get_column_param_history(conn, "null_check", "col")

        assert result == ["endt", "expiry_date"]


# ---------------------------------------------------------------------------
# Pre-selection from historical matches
# ---------------------------------------------------------------------------

class TestHistoricalPreselect:

    @pytest.fixture()
    def registry(self):
        reg = CheckRegistry()
        reg.register("null_check", _make_col_check(), kind="check")
        return reg

    def test_historical_col_in_source_is_predefault_for_select(self, registry):
        """When history has 'endt' and source has 'endt', select default='endt'."""
        prompter = _make_prompter_single(registry)
        conn = MagicMock()
        captured_default = []

        def capture_select(label, choices, default=None):
            captured_default.append(default)
            m = MagicMock()
            m.ask.return_value = default or choices[0].value
            return m

        with _fill_params_stubs(
            ["endt", "price"],
            {"endt": "DATE", "price": "DOUBLE"},
            col_history=["endt"],
        ), patch("proto_pipe.cli.prompts.questionary.select", side_effect=capture_select):

            prompter.prompt_params(
                selected_checks=["null_check"],
                table="van",
                conn=conn,
                report_name="van_report",
            )

        assert captured_default == ["endt"], (
            f"Expected default='endt' from history. Got: {captured_default}"
        )

    def test_historical_col_not_in_source_is_not_predefaulted(self, registry):
        """When history has 'endt' but source doesn't, no pre-selection fires."""
        prompter = _make_prompter_single(registry)
        conn = MagicMock()
        captured_default = []

        def capture_select(label, choices, default=None):
            captured_default.append(default)
            m = MagicMock()
            m.ask.return_value = choices[0].value
            return m

        with _fill_params_stubs(
            ["expiry_date", "price"],
            {"expiry_date": "DATE", "price": "DOUBLE"},
            col_history=["endt"],  # not in this source's columns
        ), patch("proto_pipe.cli.prompts.questionary.select", side_effect=capture_select):

            prompter.prompt_params(
                selected_checks=["null_check"],
                table="foo",
                conn=conn,
                report_name="foo_report",
            )

        # endt is not available, so default should be None (falls through to auto_match)
        assert captured_default == [None], (
            f"Expected no pre-selection when history col not in source. Got: {captured_default}"
        )

    def test_historical_col_in_source_is_prechecked_for_checkbox(self, registry):
        """When history has 'endt' and source has 'endt', checkbox pre-checks it."""
        prompter = _make_prompter_multi(registry)
        conn = MagicMock()
        captured_choices = []

        def capture_checkbox(label, choices):
            captured_choices.extend(choices)
            m = MagicMock()
            m.ask.return_value = [c.value for c in choices if c.checked]
            return m

        with _fill_params_stubs(
            ["endt", "price"],
            {"endt": "DATE", "price": "DOUBLE"},
            col_history=["endt"],
        ), patch("proto_pipe.cli.prompts.questionary.checkbox", side_effect=capture_checkbox):

            prompter.prompt_params(
                selected_checks=["null_check"],
                table="van",
                conn=conn,
                report_name="van_report",
            )

        endt_choice = next((c for c in captured_choices if c.value == "endt"), None)
        assert endt_choice is not None
        assert endt_choice.checked is True, (
            "Historical column 'endt' must be pre-checked in checkbox prompt"
        )

    def test_multiple_historical_cols_all_prechecked_if_in_source(self, registry):
        """All historical columns that exist in the current source are pre-checked."""
        prompter = _make_prompter_multi(registry)
        conn = MagicMock()
        captured_choices = []

        def capture_checkbox(label, choices):
            captured_choices.extend(choices)
            m = MagicMock()
            m.ask.return_value = [c.value for c in choices if c.checked]
            return m

        with _fill_params_stubs(
            ["endt", "expiry_date", "price"],
            {"endt": "DATE", "expiry_date": "DATE", "price": "DOUBLE"},
            col_history=["endt", "expiry_date", "stale_col"],  # stale_col not in source
        ), patch("proto_pipe.cli.prompts.questionary.checkbox", side_effect=capture_checkbox):

            prompter.prompt_params(
                selected_checks=["null_check"],
                table="van",
                conn=conn,
                report_name="van_report",
            )

        by_value = {c.value: c for c in captured_choices}
        assert by_value["endt"].checked is True
        assert by_value["expiry_date"].checked is True
        assert by_value["price"].checked is False  # not in history

    def test_history_takes_priority_over_auto_match_as_default(self, registry):
        """Historical match is used as default before auto_match (exact name)."""
        prompter = _make_prompter_single(registry)
        conn = MagicMock()
        captured_default = []

        def capture_select(label, choices, default=None):
            captured_default.append(default)
            m = MagicMock()
            m.ask.return_value = default or choices[0].value
            return m

        # Source has both "col" (auto_match candidate) and "endt" (history match)
        with _fill_params_stubs(
            ["col", "endt", "price"],
            {"col": "VARCHAR", "endt": "DATE", "price": "DOUBLE"},
            col_history=["endt"],
        ), patch("proto_pipe.cli.prompts.questionary.select", side_effect=capture_select):

            prompter.prompt_params(
                selected_checks=["null_check"],
                table="van",
                conn=conn,
                report_name="van_report",
            )

        # History ('endt') should be default, not auto_match ('col')
        assert captured_default == ["endt"], (
            f"Expected history match 'endt' as default over auto_match 'col'. "
            f"Got: {captured_default}"
        )


# ---------------------------------------------------------------------------
# Within-session alias_cols pre-selection
# ---------------------------------------------------------------------------

class TestWithinSessionPreselect:

    @pytest.fixture()
    def registry_two_checks(self):
        """Registry with two checks that share the same 'col' param name."""
        def check_a(col: str) -> "pd.Series[bool]":
            return pd.Series([True])
        def check_b(col: str) -> "pd.Series[bool]":
            return pd.Series([True])

        reg = CheckRegistry()
        reg.register("check_a", check_a, kind="check")
        reg.register("check_b", check_b, kind="check")
        return reg

    def test_within_session_selection_prechecked_for_subsequent_check(
        self, registry_two_checks
    ):
        """When check_a maps col→endt this session, check_b's col prompt
        pre-checks endt because it's in alias_param_to_cols."""
        prompter = _make_prompter_multi(registry_two_checks)
        conn = MagicMock()

        call_count = 0
        captured_checks: list[list] = []

        def capture_checkbox(label, choices):
            nonlocal call_count
            call_count += 1
            captured_checks.append(list(choices))
            m = MagicMock()
            # First call (check_a): select endt
            # Second call (check_b): return whatever is pre-checked
            if call_count == 1:
                m.ask.return_value = ["endt"]
            else:
                m.ask.return_value = [c.value for c in choices if c.checked]
            return m

        with _fill_params_stubs(
            ["endt", "price"],
            {"endt": "DATE", "price": "DOUBLE"},
            col_history=[],
        ), patch("proto_pipe.cli.prompts.questionary.checkbox", side_effect=capture_checkbox):

            _, alias_map, _ = prompter.prompt_params(
                selected_checks=["check_a", "check_b"],
                table="van",
                conn=conn,
                report_name="van_report",
            )

        # check_b's choices should have endt pre-checked
        assert len(captured_checks) == 2, "Expected two checkbox prompts"
        check_b_choices = {c.value: c for c in captured_checks[1]}
        assert check_b_choices["endt"].checked is True, (
            "Within-session alias (endt) must be pre-checked for check_b"
        )


# ---------------------------------------------------------------------------
# _output prompt — param-mirror pre-selection (rule 9)
# ---------------------------------------------------------------------------

class TestOutputPromptParamMirror:
    """Behavioral guarantees for the two-step _output param-mirror prompt.

    Root cause of previous test failures: eligible=False for transforms (not
    bool-Series return) meant the checkbox never fired, alias entries were never
    added during the call, and the _output prompt never triggered.
    Fix: eligible is now also True for transforms with column-backed params.
    Tests must trigger the checkbox by having the mock return multiple columns,
    creating the n_runs > 1 scenario that fires the _output mirror prompt.
    """

    @pytest.fixture()
    def registry_transform(self):
        """Two-param transform. Used by test 1 which needs both multi-col (bar)
        and single-col (baz) params to verify exclusion logic."""
        def my_transform(bar: float, baz: float) -> "pd.Series":
            pass
        reg = CheckRegistry()
        reg.register("my_transform", my_transform, kind="transform")
        return reg

    @pytest.fixture()
    def registry_single_transform(self):
        """Single-param transform. Used by tests 2 and 3 so the only checkbox
        call before the _output prompt is the bar param selection — no second
        param checkbox that would be mistaken for the output step 2."""
        def my_transform(bar: float) -> "pd.Series":
            pass
        reg = CheckRegistry()
        reg.register("my_transform", my_transform, kind="transform")
        return reg

    def test_output_prompt_step1_shows_multi_col_params_only(
        self, registry_transform
    ):
        """Step 1 choices include only params with >1 column, plus manual escape.

        Single-entry (broadcast) params must not appear as mirror choices.
        bar → [col_a, col_b] (multi), baz → [floor_a] (broadcast, excluded).
        Checkbox mock: bar returns 2 cols, baz returns 1 col → n_runs=2 → _output fires.
        """
        prompter = _make_prompter_multi(registry_transform)
        conn = MagicMock()

        captured_step1_choices = []
        checkbox_call = [0]

        def capture_select(label, choices, default=None):
            # Only fires for _output step 1 (col params use checkbox with eligible=True)
            captured_step1_choices.extend(choices)
            m = MagicMock()
            m.ask.return_value = choices[-1].value  # pick manual so step 2 doesn't interfere
            return m

        def capture_checkbox(label, choices):
            checkbox_call[0] += 1
            m = MagicMock()
            if checkbox_call[0] == 1:
                m.ask.return_value = ["col_a", "col_b"]  # bar: 2 cols → multi
            elif checkbox_call[0] == 2:
                m.ask.return_value = ["floor_a"]          # baz: 1 col → broadcast
            else:
                m.ask.return_value = []                   # _output step 2: manual → empty
            return m

        with _fill_params_stubs(
            ["col_a", "col_b", "floor_a", "result"],
            {"col_a": "DOUBLE", "col_b": "DOUBLE", "floor_a": "DOUBLE", "result": "DOUBLE"},
        ), patch("proto_pipe.cli.prompts.questionary.select", side_effect=capture_select), \
           patch("proto_pipe.cli.prompts.questionary.checkbox", side_effect=capture_checkbox):

            prompter._fill_params(
                selected_checks=["my_transform"],
                table="sales",
                conn=conn,
                report_name="test_report",
            )

        step1_values = [c.value for c in captured_step1_choices]
        assert "bar" in step1_values, "Multi-column param 'bar' must appear in step 1"
        assert "baz" not in step1_values, (
            "Single-entry broadcast param 'baz' must NOT appear in step 1"
        )
        assert any("\u270e" in v for v in step1_values), (
            "Manual escape option must always be present in step 1"
        )

    def test_output_prompt_param_chosen_writes_directly(
        self, registry_single_transform
    ):
        """When user picks a param in step 1, its columns are written directly as
        _output entries — no step 2 checkbox fires. Eliminates Enter-key bleed-through
        where the Enter that closed step 1 auto-submits step 2's pre-checked items.
        """
        prompter = _make_prompter_multi(registry_single_transform)
        conn = MagicMock()

        checkbox_call = [0]
        step2_fired = [False]

        def capture_select(label, choices, default=None):
            m = MagicMock()
            m.ask.return_value = "bar"
            return m

        def capture_checkbox(label, choices):
            checkbox_call[0] += 1
            m = MagicMock()
            if checkbox_call[0] == 1:
                m.ask.return_value = ["col_a", "col_b"]
            else:
                step2_fired[0] = True
                m.ask.return_value = []
            return m

        with _fill_params_stubs(
            ["col_a", "col_b", "result"],
            {"col_a": "DOUBLE", "col_b": "DOUBLE", "result": "DOUBLE"},
        ), patch("proto_pipe.cli.prompts.questionary.select", side_effect=capture_select), \
           patch("proto_pipe.cli.prompts.questionary.checkbox", side_effect=capture_checkbox):
            _, alias_entries, _ = prompter._fill_params(
                selected_checks=["my_transform"],
                table="sales",
                conn=conn,
                report_name="test_report",
            )

        assert not step2_fired[0], (
            "Step 2 checkbox must NOT fire when a param is chosen — direct write only"
        )
        output_cols = [e["column"] for e in alias_entries if e["param"] == "_output"]
        assert set(output_cols) == {"col_a", "col_b"}, (
            "bar's columns written directly as _output entries"
        )

    def test_output_prompt_manual_shows_free_checkbox(self, registry_single_transform):
        """Manual escape shows a free checkbox with no pre-checks. Result is stored."""
        prompter = _make_prompter_multi(registry_single_transform)
        conn = MagicMock()

        captured_output_choices = []
        checkbox_call = [0]

        def capture_select(label, choices, default=None):
            m = MagicMock()
            m.ask.return_value = choices[-1].value  # manual
            return m

        def capture_checkbox(label, choices):
            checkbox_call[0] += 1
            m = MagicMock()
            if checkbox_call[0] == 1:
                m.ask.return_value = ["col_a", "col_b"]
            else:
                captured_output_choices.extend(choices)
                m.ask.return_value = ["result"]
            return m

        with _fill_params_stubs(
            ["col_a", "col_b", "result"],
            {"col_a": "DOUBLE", "col_b": "DOUBLE", "result": "DOUBLE"},
        ), patch("proto_pipe.cli.prompts.questionary.select", side_effect=capture_select), \
           patch("proto_pipe.cli.prompts.questionary.checkbox", side_effect=capture_checkbox):
            _, alias_entries, _ = prompter._fill_params(
                selected_checks=["my_transform"],
                table="sales",
                conn=conn,
                report_name="test_report",
            )

        assert captured_output_choices, "Manual path must show output checkbox"
        assert all(not getattr(c, "checked", False) for c in captured_output_choices), (
            "No columns pre-checked in manual mode"
        )
        output_cols = [e["column"] for e in alias_entries if e["param"] == "_output"]
        assert output_cols == ["result"]


# ---------------------------------------------------------------------------
# _output scoping — check field written per transform
# ---------------------------------------------------------------------------

class TestOutputScopingCheckField:
    """_output alias_map entries carry "check": check_name so the runner
    can scope write-back to the correct transform.

    Guarantees:
    - n_runs==1 (single-select): _output entry has "check" == check_name
    - n_runs>1 (mirror): all _output entries have "check" == check_name
    - manual select: all _output entries have "check" == check_name
    - Two transforms in the same _fill_params call write distinct "check" values
    """

    @pytest.fixture()
    def single_transform_registry(self):
        def transform_a(bar: float) -> "pd.Series":
            pass
        reg = CheckRegistry()
        reg.register("transform_a", transform_a, kind="transform")
        return reg

    @pytest.fixture()
    def two_transform_registry(self):
        def transform_a(bar: float) -> "pd.Series":
            pass
        def transform_b(baz: float) -> "pd.Series":
            pass
        reg = CheckRegistry()
        reg.register("transform_a", transform_a, kind="transform")
        reg.register("transform_b", transform_b, kind="transform")
        return reg

    def test_single_run_output_carries_check_name(self, single_transform_registry):
        """n_runs==1: _output entry has 'check' == 'transform_a'."""
        prompter = _make_prompter_multi(single_transform_registry)
        conn = MagicMock()

        checkbox_call = [0]

        def capture_checkbox(label, choices):
            checkbox_call[0] += 1
            m = MagicMock()
            m.ask.return_value = ["col_a"]  # single col → n_runs=1
            return m

        def capture_select(label, choices, default=None):
            m = MagicMock()
            m.ask.return_value = choices[0].value if hasattr(choices[0], 'value') else choices[0]
            return m

        with _fill_params_stubs(
            ["col_a", "col_b"],
            {"col_a": "DOUBLE", "col_b": "DOUBLE"},
        ), patch("proto_pipe.cli.prompts.questionary.checkbox", side_effect=capture_checkbox), \
           patch("proto_pipe.cli.prompts.questionary.select", side_effect=capture_select):

            _, alias_entries, _ = prompter._fill_params(
                selected_checks=["transform_a"],
                table="sales",
                conn=conn,
                report_name="test_report",
            )

        output_entries = [e for e in alias_entries if e["param"] == "_output"]
        assert output_entries, "Expected at least one _output entry"
        for e in output_entries:
            assert e.get("check") == "transform_a", (
                f"_output entry must have check='transform_a', got {e}"
            )

    def test_multi_run_mirror_output_carries_check_name(self, single_transform_registry):
        """n_runs>1 mirror path: all _output entries have 'check' == check_name."""
        prompter = _make_prompter_multi(single_transform_registry)
        conn = MagicMock()

        checkbox_call = [0]

        def capture_checkbox(label, choices):
            checkbox_call[0] += 1
            m = MagicMock()
            m.ask.return_value = ["col_a", "col_b"]  # two cols → n_runs=2
            return m

        def capture_select(label, choices, default=None):
            # Step 1: pick first param as mirror
            m = MagicMock()
            m.ask.return_value = choices[0].value if hasattr(choices[0], 'value') else "bar"
            return m

        with _fill_params_stubs(
            ["col_a", "col_b"],
            {"col_a": "DOUBLE", "col_b": "DOUBLE"},
        ), patch("proto_pipe.cli.prompts.questionary.checkbox", side_effect=capture_checkbox), \
           patch("proto_pipe.cli.prompts.questionary.select", side_effect=capture_select):

            _, alias_entries, _ = prompter._fill_params(
                selected_checks=["transform_a"],
                table="sales",
                conn=conn,
                report_name="test_report",
            )

        output_entries = [e for e in alias_entries if e["param"] == "_output"]
        assert output_entries, "Expected _output entries for n_runs>1 mirror"
        for e in output_entries:
            assert e.get("check") == "transform_a", (
                f"_output entry must have check='transform_a', got {e}"
            )

    def test_two_transforms_have_distinct_check_fields(self, two_transform_registry):
        """Two transforms in the same _fill_params call write distinct check values."""
        prompter = _make_prompter_multi(two_transform_registry)
        conn = MagicMock()

        checkbox_call = [0]

        def capture_checkbox(label, choices):
            checkbox_call[0] += 1
            m = MagicMock()
            # Each transform gets one col → n_runs=1 each → _output fires twice
            m.ask.return_value = ["col_a"] if checkbox_call[0] % 2 == 1 else ["col_b"]
            return m

        def capture_select(label, choices, default=None):
            m = MagicMock()
            m.ask.return_value = choices[0].value if hasattr(choices[0], 'value') else choices[0]
            return m

        with _fill_params_stubs(
            ["col_a", "col_b"],
            {"col_a": "DOUBLE", "col_b": "DOUBLE"},
        ), patch("proto_pipe.cli.prompts.questionary.checkbox", side_effect=capture_checkbox), \
           patch("proto_pipe.cli.prompts.questionary.select", side_effect=capture_select):

            _, alias_entries, _ = prompter._fill_params(
                selected_checks=["transform_a", "transform_b"],
                table="sales",
                conn=conn,
                report_name="test_report",
            )

        output_entries = [e for e in alias_entries if e["param"] == "_output"]
        assert len(output_entries) == 2, (
            f"Expected 2 _output entries (one per transform), got {len(output_entries)}"
        )
        check_values = {e.get("check") for e in output_entries}
        assert check_values == {"transform_a", "transform_b"}, (
            f"Each transform must have a distinct check field: {check_values}"
        )
