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


@contextlib.contextmanager
def _scaffold_stubs():
    with patch("proto_pipe.cli.scaffold._get_original_func", return_value=None), \
         patch("proto_pipe.cli.scaffold._get_check_first_sentence", return_value=""), \
         patch("proto_pipe.cli.scaffold._build_check_param_lines", return_value=[]):
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
