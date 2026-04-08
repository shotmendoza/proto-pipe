"""Tests for ReportConfigPrompter.prompt_checks() behavioral guarantees.

Guarantees under test (CLAUDE.md — vp new report split function selection):
  - Transforms and checks are never shown in the same prompt.
  - If a kind has no registered functions, its prompt is skipped.
  - Column list is printed once before the first selection prompt.
  - If registry_types is empty, a [warn] is printed and prompts continue.
  - ESC on either prompt returns go_back=True.
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

def _make_check():
    def f(col: str) -> "pd.Series[bool]":
        return pd.Series([True])
    return f


def _make_transform():
    def f(col: str) -> "pd.DataFrame":
        return pd.DataFrame()
    return f


def _make_prompter(registry):
    return ReportConfigPrompter(
        check_registry=registry,
        p_db="/fake/path.db",
        multi_select=True,
    )




def _make_prompter_single(registry: CheckRegistry) -> ReportConfigPrompter:
    """Prompter with multi_select=False — forces questionary.select for column params."""
    return ReportConfigPrompter(
        check_registry=registry,
        p_db="/fake/path.db",
        multi_select=False,
    )
@contextlib.contextmanager
def _scaffold_stubs():
    with patch("proto_pipe.cli.scaffold.get_original_func", return_value=None), \
         patch("proto_pipe.cli.scaffold.get_check_first_sentence", return_value=""), \
         patch("proto_pipe.cli.scaffold.build_check_param_lines", return_value=[]):
        yield


def _checkbox_returning(value):
    m = MagicMock()
    m.return_value.ask.return_value = value
    return m


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture()
def registry_both():
    reg = CheckRegistry()
    reg.register("my_check", _make_check(), kind="check")
    reg.register("my_transform", _make_transform(), kind="transform")
    return reg


@pytest.fixture()
def registry_checks_only():
    reg = CheckRegistry()
    reg.register("my_check", _make_check(), kind="check")
    return reg


@pytest.fixture()
def registry_transforms_only():
    reg = CheckRegistry()
    reg.register("my_transform", _make_transform(), kind="transform")
    return reg


# ---------------------------------------------------------------------------
# Kind split
# ---------------------------------------------------------------------------

class TestKindSplit:

    def test_two_prompts_when_both_kinds_registered(self, registry_both):
        """Transforms and checks each get their own separate checkbox prompt."""
        prompter = _make_prompter(registry_both)
        mock_cb = _checkbox_returning([])

        with _scaffold_stubs(), \
             patch("proto_pipe.cli.prompts.questionary.checkbox", mock_cb), \
             patch("proto_pipe.cli.prompts.click.echo"):

            selected, go_back = prompter.prompt_checks(
                table_cols=["col"], alias_map=[],
                registry_types={"col": "VARCHAR"},
            )

        assert mock_cb.call_count == 2, "Expected two separate checkbox calls — one per kind"
        assert go_back is False

    def test_transforms_prompt_comes_before_checks(self, registry_both):
        """Transforms prompt is shown first, checks prompt second."""
        prompter = _make_prompter(registry_both)
        labels = []

        def capture(label, choices):
            labels.append(label)
            m = MagicMock()
            m.ask.return_value = []
            return m

        with _scaffold_stubs(), \
             patch("proto_pipe.cli.prompts.questionary.checkbox", side_effect=capture), \
             patch("proto_pipe.cli.prompts.click.echo"):

            prompter.prompt_checks(
                table_cols=["col"], alias_map=[],
                registry_types={"col": "VARCHAR"},
            )

        assert len(labels) == 2
        assert "transform" in labels[0].lower()
        assert "check" in labels[1].lower() or "validation" in labels[1].lower()

    def test_each_prompt_contains_only_correct_kind(self, registry_both):
        """Choices in each prompt contain only functions of the matching kind."""
        prompter = _make_prompter(registry_both)
        captured = []

        def capture(label, choices):
            captured.append(list(choices))
            m = MagicMock()
            m.ask.return_value = []
            return m

        with _scaffold_stubs(), \
             patch("proto_pipe.cli.prompts.questionary.checkbox", side_effect=capture), \
             patch("proto_pipe.cli.prompts.click.echo"):

            prompter.prompt_checks(
                table_cols=["col"], alias_map=[],
                registry_types={"col": "VARCHAR"},
            )

        assert len(captured) == 2
        transform_values = {c.value for c in captured[0]}
        check_values = {c.value for c in captured[1]}
        assert transform_values == {"my_transform"}
        assert check_values == {"my_check"}
        assert transform_values.isdisjoint(check_values)

    def test_transforms_prompt_skipped_when_none_registered(self, registry_checks_only):
        """When no transforms registered, only the checks prompt is shown."""
        prompter = _make_prompter(registry_checks_only)
        mock_cb = _checkbox_returning([])

        with _scaffold_stubs(), \
             patch("proto_pipe.cli.prompts.questionary.checkbox", mock_cb), \
             patch("proto_pipe.cli.prompts.click.echo"):

            selected, go_back = prompter.prompt_checks(
                table_cols=["col"], alias_map=[],
                registry_types={"col": "VARCHAR"},
            )

        assert mock_cb.call_count == 1, "Transforms prompt must be skipped when none registered"
        assert go_back is False

    def test_checks_prompt_skipped_when_none_registered(self, registry_transforms_only):
        """When no checks registered, only the transforms prompt is shown."""
        prompter = _make_prompter(registry_transforms_only)
        mock_cb = _checkbox_returning([])

        with _scaffold_stubs(), \
             patch("proto_pipe.cli.prompts.questionary.checkbox", mock_cb), \
             patch("proto_pipe.cli.prompts.click.echo"):

            prompter.prompt_checks(
                table_cols=["col"], alias_map=[],
                registry_types={"col": "VARCHAR"},
            )

        assert mock_cb.call_count == 1, "Checks prompt must be skipped when none registered"

    def test_selections_from_both_prompts_merged(self, registry_both):
        """Selections from both prompts are merged into a single returned list."""
        prompter = _make_prompter(registry_both)
        n = 0

        def alternate(label, choices):
            nonlocal n
            n += 1
            m = MagicMock()
            m.ask.return_value = ["my_transform"] if n == 1 else ["my_check"]
            return m

        with _scaffold_stubs(), \
             patch("proto_pipe.cli.prompts.questionary.checkbox", side_effect=alternate), \
             patch("proto_pipe.cli.prompts.click.echo"):

            selected, go_back = prompter.prompt_checks(
                table_cols=["col"], alias_map=[],
                registry_types={"col": "VARCHAR"},
            )

        assert set(selected) == {"my_transform", "my_check"}
        assert go_back is False


# ---------------------------------------------------------------------------
# Column display
# ---------------------------------------------------------------------------

class TestColumnDisplay:

    def test_columns_printed_before_first_checkbox(self, registry_checks_only):
        """Column list appears in output before the first checkbox prompt."""
        prompter = _make_prompter(registry_checks_only)
        call_order = []

        def record_echo(msg="", **kw):
            call_order.append(("echo", str(msg)))

        def record_cb(label, choices):
            call_order.append(("checkbox", label))
            m = MagicMock()
            m.ask.return_value = []
            return m

        with _scaffold_stubs(), \
             patch("proto_pipe.cli.prompts.questionary.checkbox", side_effect=record_cb), \
             patch("proto_pipe.cli.prompts.click.echo", side_effect=record_echo):

            prompter.prompt_checks(
                table_cols=["price"], alias_map=[],
                registry_types={"price": "DOUBLE"},
            )

        first_cb = next(i for i, (k, _) in enumerate(call_order) if k == "checkbox")
        col_echo = next(
            (i for i, (k, t) in enumerate(call_order) if k == "echo" and "price" in t),
            None,
        )
        assert col_echo is not None, "Column 'price' was never echoed"
        assert col_echo < first_cb, "Column display must appear before first checkbox"

    def test_columns_printed_exactly_once(self, registry_both):
        """Column list is printed once even when both kinds have prompts."""
        prompter = _make_prompter(registry_both)
        echoed = []

        with _scaffold_stubs(), \
             patch("proto_pipe.cli.prompts.questionary.checkbox", _checkbox_returning([])), \
             patch("proto_pipe.cli.prompts.click.echo",
                   side_effect=lambda m="", **k: echoed.append(str(m))):

            prompter.prompt_checks(
                table_cols=["col"], alias_map=[],
                registry_types={"col": "VARCHAR"},
            )

        mentions = [m for m in echoed if "col" in m and "VARCHAR" in m]
        assert len(mentions) == 1, f"Expected exactly one col/VARCHAR echo, got {mentions}"

    def test_warn_when_registry_types_empty(self, registry_checks_only):
        """When registry_types is empty a [warn] is printed."""
        prompter = _make_prompter(registry_checks_only)
        echoed = []

        with _scaffold_stubs(), \
             patch("proto_pipe.cli.prompts.questionary.checkbox", _checkbox_returning([])), \
             patch("proto_pipe.cli.prompts.click.echo",
                   side_effect=lambda m="", **k: echoed.append(str(m))):

            selected, go_back = prompter.prompt_checks(
                table_cols=[], alias_map=[], registry_types={},
            )

        assert any("[warn]" in m for m in echoed), f"Expected [warn], got: {echoed}"

    def test_prompts_continue_after_warn(self, registry_checks_only):
        """Prompts continue normally after the [warn] — not aborted."""
        prompter = _make_prompter(registry_checks_only)
        mock_cb = _checkbox_returning([])

        with _scaffold_stubs(), \
             patch("proto_pipe.cli.prompts.questionary.checkbox", mock_cb), \
             patch("proto_pipe.cli.prompts.click.echo"):

            selected, go_back = prompter.prompt_checks(
                table_cols=[], alias_map=[], registry_types={},
            )

        assert mock_cb.call_count >= 1, "Checkbox must still appear after [warn]"
        assert go_back is False


# ---------------------------------------------------------------------------
# ESC behaviour
# ---------------------------------------------------------------------------

class TestEscBehaviour:

    def test_esc_on_transforms_returns_go_back(self, registry_both):
        """ESC (None from questionary) on transforms prompt → go_back=True."""
        prompter = _make_prompter(registry_both)

        with _scaffold_stubs(), \
             patch("proto_pipe.cli.prompts.questionary.checkbox", _checkbox_returning(None)), \
             patch("proto_pipe.cli.prompts.click.echo"):

            selected, go_back = prompter.prompt_checks(
                table_cols=["col"], alias_map=[],
                registry_types={"col": "VARCHAR"},
            )

        assert go_back is True
        assert selected is None

    def test_esc_on_checks_returns_go_back(self, registry_both):
        """ESC on the checks prompt (second call) also returns go_back=True."""
        prompter = _make_prompter(registry_both)
        n = 0

        def esc_on_second(label, choices):
            nonlocal n
            n += 1
            m = MagicMock()
            m.ask.return_value = [] if n == 1 else None
            return m

        with _scaffold_stubs(), \
             patch("proto_pipe.cli.prompts.questionary.checkbox", side_effect=esc_on_second), \
             patch("proto_pipe.cli.prompts.click.echo"):

            selected, go_back = prompter.prompt_checks(
                table_cols=["col"], alias_map=[],
                registry_types={"col": "VARCHAR"},
            )

        assert go_back is True
        assert selected is None


# ---------------------------------------------------------------------------
# _make_col_choices
# ---------------------------------------------------------------------------

class TestMakeColChoices:
    """Behavioral guarantees for the _make_col_choices module-level helper."""

    def test_title_includes_type(self):
        """Choice title is 'column_name (TYPE)' when column is in registry_types."""
        from proto_pipe.cli.prompts import _make_col_choices
        choices = _make_col_choices(["price"], {"price": "DOUBLE"})
        assert len(choices) == 1
        assert choices[0].title == "price (DOUBLE)"

    def test_title_is_column_name_when_type_unknown(self):
        """Choice title is just column name when column not in registry_types."""
        from proto_pipe.cli.prompts import _make_col_choices
        choices = _make_col_choices(["mystery_col"], {})
        assert choices[0].title == "mystery_col"

    def test_value_is_column_name_not_title(self):
        """Choice value is the raw column name, not the formatted title string."""
        from proto_pipe.cli.prompts import _make_col_choices
        choices = _make_col_choices(["price"], {"price": "DOUBLE"})
        assert choices[0].value == "price"
        assert choices[0].value != "price (DOUBLE)"

    def test_precheck_marks_column_as_checked(self):
        """Columns in precheck have checked=True."""
        from proto_pipe.cli.prompts import _make_col_choices
        choices = _make_col_choices(
            ["price", "quantity"], {"price": "DOUBLE", "quantity": "BIGINT"},
            precheck={"price"},
        )
        by_value = {c.value: c for c in choices}
        assert by_value["price"].checked is True
        assert by_value["quantity"].checked is False

    def test_no_precheck_all_unchecked(self):
        """With no precheck, all choices have checked=False."""
        from proto_pipe.cli.prompts import _make_col_choices
        choices = _make_col_choices(["a", "b"], {"a": "VARCHAR", "b": "BIGINT"})
        assert all(not c.checked for c in choices)

    def test_order_preserved(self):
        """Output order matches input column order."""
        from proto_pipe.cli.prompts import _make_col_choices
        cols = ["c", "a", "b"]
        choices = _make_col_choices(cols, {})
        assert [c.value for c in choices] == cols


# ---------------------------------------------------------------------------
# Helpers shared by _fill_params tests
# ---------------------------------------------------------------------------

def _make_col_param_check():
    """Check with one str-annotated column param — alias_map eligible."""
    def f(col: str) -> "pd.Series[bool]":
        return pd.Series([True])
    return f


def _make_scalar_param_check():
    """Check with a column param AND a scalar float param."""
    def f(col: str, min_val: float) -> "pd.Series[bool]":
        return pd.Series([True])
    return f


@contextlib.contextmanager
def _fill_params_stubs(table_cols, registry_types_result):
    """Stubs for the DB-touching helpers called inside _fill_params."""
    with patch("proto_pipe.cli.scaffold.get_table_columns", return_value=table_cols), \
         patch("proto_pipe.cli.scaffold.get_param_suggestions", return_value=[]), \
         patch("proto_pipe.cli.scaffold.get_column_param_history", return_value=[]), \
         patch("proto_pipe.cli.scaffold.record_param_history"), \
         patch("proto_pipe.io.db.get_registry_types", return_value=registry_types_result), \
         patch("proto_pipe.cli.prompts.click.echo"):
        yield


# ---------------------------------------------------------------------------
# ReportConfigPrompter._fill_params() — alias_map guarantees
# ---------------------------------------------------------------------------

class TestFillParamsAliasMap:

    @pytest.fixture()
    def registry_col(self):
        """Registry with a single column-param check."""
        reg = CheckRegistry()
        reg.register("col_check", _make_col_param_check(), kind="check")
        return reg

    @pytest.fixture()
    def registry_scalar(self):
        """Registry with a check that has both a column param and a scalar param."""
        reg = CheckRegistry()
        reg.register("scalar_check", _make_scalar_param_check(), kind="check")
        return reg

    @pytest.fixture()
    def registry_multiselect(self):
        """Registry with a multiselect-eligible check (multi_select=True prompter)."""
        reg = CheckRegistry()
        reg.register("ms_check", _make_col_param_check(), kind="check")
        return reg

    def test_column_param_produces_alias_map_entry(self, registry_col):
        """Every column param on every selected function gets an alias_map entry."""
        prompter = _make_prompter_single(registry_col)
        conn = MagicMock()

        mock_select = MagicMock()
        mock_select.return_value.ask.return_value = "price"

        with _fill_params_stubs(["price", "quantity"], {"price": "DOUBLE", "quantity": "BIGINT"}), \
             patch("proto_pipe.cli.prompts.questionary.select", mock_select):

            _, alias_map, go_back = prompter.prompt_params(
                selected_checks=["col_check"],
                table="sales",
                conn=conn,
                report_name="test_report",
            )

        assert go_back is False
        assert any(e["param"] == "col" and e["column"] == "price" for e in alias_map), (
            f"Expected alias_map entry for param='col', column='price'. Got: {alias_map}"
        )

    def test_auto_populate_on_exact_name_match(self, registry_col):
        """When param name exactly matches a column, it is passed as the select default."""
        prompter = _make_prompter_single(registry_col)
        conn = MagicMock()

        captured_default = []

        def capture_select(label, choices, default=None):
            captured_default.append(default)
            m = MagicMock()
            m.ask.return_value = default or choices[0].value
            return m

        # table_cols contains "col" — exact match for param name "col"
        with _fill_params_stubs(["col", "price"], {"col": "VARCHAR", "price": "DOUBLE"}), \
             patch("proto_pipe.cli.prompts.questionary.select", side_effect=capture_select):

            prompter.prompt_params(
                selected_checks=["col_check"],
                table="sales",
                conn=conn,
                report_name="test_report",
            )

        assert captured_default, "select was never called"
        assert captured_default[0] == "col", (
            f"Expected default='col' (exact match), got default={captured_default[0]!r}"
        )

    def test_no_auto_populate_when_no_exact_match(self, registry_col):
        """When param name does not match any column, default is None — no fuzzy matching."""
        prompter = _make_prompter_single(registry_col)
        conn = MagicMock()

        captured_default = []

        def capture_select(label, choices, default=None):
            captured_default.append(default)
            m = MagicMock()
            m.ask.return_value = choices[0].value
            return m

        # table_cols does NOT contain "col" — no exact match
        with _fill_params_stubs(["price", "quantity"], {"price": "DOUBLE", "quantity": "BIGINT"}), \
             patch("proto_pipe.cli.prompts.questionary.select", side_effect=capture_select):

            prompter.prompt_params(
                selected_checks=["col_check"],
                table="sales",
                conn=conn,
                report_name="test_report",
            )

        assert captured_default, "select was never called"
        assert captured_default[0] is None, (
            f"Expected no auto-populate (default=None) when no exact match. "
            f"Got default={captured_default[0]!r}"
        )

    def test_choices_carry_type_from_registry(self, registry_col):
        """Column choices passed to questionary.select include type from column_type_registry."""
        prompter = _make_prompter_single(registry_col)
        conn = MagicMock()

        captured_choices = []

        def capture_select(label, choices, default=None):
            captured_choices.extend(choices)
            m = MagicMock()
            m.ask.return_value = choices[0].value
            return m

        with _fill_params_stubs(["price"], {"price": "DOUBLE"}), \
             patch("proto_pipe.cli.prompts.questionary.select", side_effect=capture_select):

            prompter.prompt_params(
                selected_checks=["col_check"],
                table="sales",
                conn=conn,
                report_name="test_report",
            )

        assert captured_choices, "select was never called"
        price_choice = next((c for c in captured_choices if c.value == "price"), None)
        assert price_choice is not None
        assert "DOUBLE" in price_choice.title, (
            f"Expected type 'DOUBLE' in choice title, got: {price_choice.title!r}"
        )

    def test_scalar_param_excluded_from_alias_map(self, registry_scalar):
        """int/float params go to alias_map when column chosen, filled_params when escape chosen.

        Under the unified column-or-escape routing model, int/float params get the
        same column picker + escape hatch as str/unannotated params (when no pd.Series
        params are present). When the user picks the escape hatch, the value goes to
        filled_params as a broadcast constant — not alias_map.
        This test verifies the escape hatch path: col → alias_map, min_val → filled_params.
        """
        prompter = _make_prompter_single(registry_scalar)
        conn = MagicMock()

        # Differentiated mock: col gets a column, min_val gets the escape hatch.
        # Sentinel is the LAST choice in the list (prompts.py positions it at end).
        call_count = [0]

        def capture_select(label, choices, default=None):
            call_count[0] += 1
            m = MagicMock()
            if call_count[0] == 1:
                # First call = col param → pick first column
                m.ask.return_value = choices[0].value
            else:
                # Second call = min_val param → pick escape hatch (last choice)
                m.ask.return_value = choices[-1].value
            return m

        mock_text = MagicMock()
        mock_text.return_value.ask.return_value = "10.0"

        with _fill_params_stubs(["price", "quantity"], {"price": "DOUBLE"}), \
             patch("proto_pipe.cli.prompts.questionary.select", side_effect=capture_select), \
             patch("proto_pipe.cli.prompts.questionary.text", mock_text):

            check_entries, alias_map, go_back = prompter.prompt_params(
                selected_checks=["scalar_check"],
                table="sales",
                conn=conn,
                report_name="test_report",
            )

        # col → alias_map (column picked), min_val → filled_params (escape chosen)
        alias_params = {e["param"] for e in alias_map}
        assert "min_val" not in alias_params, (
            "min_val must not appear in alias_map when escape hatch was chosen"
        )
        assert "col" in alias_params, "Column param 'col' must appear in alias_map"

        # min_val should be in the check entry params as a broadcast constant
        entry = next((e for e in check_entries if e["name"] == "scalar_check"), None)
        assert entry is not None
        assert "min_val" in entry.get("params", {}), (
            "min_val must appear in check entry params when escape hatch was chosen"
        )

    def test_multiselect_produces_one_entry_per_column(self, registry_multiselect):
        """Multi-select eligible params produce one alias_map entry per selected column."""
        prompter = ReportConfigPrompter(
            check_registry=registry_multiselect,
            p_db="/fake/path.db",
            multi_select=True,
        )
        conn = MagicMock()

        mock_cb = MagicMock()
        mock_cb.return_value.ask.return_value = ["price", "quantity"]

        with _fill_params_stubs(["price", "quantity"], {"price": "DOUBLE", "quantity": "BIGINT"}), \
             patch("proto_pipe.cli.prompts.questionary.checkbox", mock_cb):

            _, alias_map, go_back = prompter.prompt_params(
                selected_checks=["ms_check"],
                table="sales",
                conn=conn,
                report_name="test_report",
            )

        col_entries = [e for e in alias_map if e["param"] == "col"]
        assert len(col_entries) == 2, (
            f"Expected 2 alias_map entries (one per selected column), got {col_entries}"
        )
        columns = {e["column"] for e in col_entries}
        assert columns == {"price", "quantity"}

    def test_existing_alias_map_prefills_on_edit(self, registry_col):
        """Existing alias_map entries are preserved and the matched column is pre-selected."""
        prompter = _make_prompter_single(registry_col)
        conn = MagicMock()

        captured_default = []

        def capture_select(label, choices, default=None):
            captured_default.append(default)
            m = MagicMock()
            # User presses Enter — keeps the pre-filled value
            m.ask.return_value = default or choices[0].value
            return m

        existing = [{"param": "col", "column": "price"}]

        with _fill_params_stubs(["price", "quantity"], {"price": "DOUBLE", "quantity": "BIGINT"}), \
             patch("proto_pipe.cli.prompts.questionary.select", side_effect=capture_select):

            _, alias_map, go_back = prompter.prompt_params(
                selected_checks=["col_check"],
                table="sales",
                conn=conn,
                report_name="test_report",
                existing_alias_map=existing,
            )

        # Pre-existing entry preserved
        assert any(e["param"] == "col" and e["column"] == "price" for e in alias_map)
