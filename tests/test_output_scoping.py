"""Unit tests for _resolve_output_col scoping guarantee.

CLAUDE.md behavioral guarantees:
- _output entries carry "check": check_name scoping their write-back column
- _resolve_output_col filters _output entries by check_name
- Two transforms sharing an alias_map do not bleed into each other's output col
- Legacy entries without "check" field fall through (backward compat)
"""
from __future__ import annotations

import pytest
from proto_pipe.reports.transforms import _resolve_output_col


def _entry(param, column, check=None):
    e = {"param": param, "column": column}
    if check is not None:
        e["check"] = check
    return e


# ---------------------------------------------------------------------------
# Core scoping guarantee
# ---------------------------------------------------------------------------

class TestResolveOutputColScoping:

    def test_returns_correct_output_for_scoped_transform(self):
        """Returns the _output column belonging to the specified transform."""
        alias_map = [
            _entry("bar", "col_a", check="transform_a"),
            _entry("_output", "result_a", check="transform_a"),
            _entry("baz", "col_b", check="transform_b"),
            _entry("_output", "result_b", check="transform_b"),
        ]
        col_backed = {"bar": "col_a"}

        result = _resolve_output_col(alias_map, col_backed, check_name="transform_a")
        assert result == "result_a", (
            "Must return transform_a's output column, not transform_b's"
        )

    def test_does_not_bleed_into_sibling_transform_output(self):
        """transform_b's _output is not returned when resolving transform_a."""
        alias_map = [
            _entry("bar", "col_a", check="transform_a"),
            _entry("_output", "result_a", check="transform_a"),
            _entry("bar", "col_a", check="transform_b"),
            _entry("_output", "result_b", check="transform_b"),
        ]
        col_backed = {"bar": "col_a"}

        result_a = _resolve_output_col(alias_map, col_backed, check_name="transform_a")
        result_b = _resolve_output_col(alias_map, col_backed, check_name="transform_b")

        assert result_a == "result_a"
        assert result_b == "result_b"
        assert result_a != result_b, (
            "Two transforms with the same input column must resolve to different outputs"
        )

    def test_multi_run_mirrors_correct_output_per_run(self):
        """Multi-run transform: run index resolved within the scoped _output list."""
        alias_map = [
            _entry("bar", "col_a", check="transform_a"),
            _entry("bar", "col_b", check="transform_a"),
            _entry("_output", "out_a", check="transform_a"),
            _entry("_output", "out_b", check="transform_a"),
            # transform_b has its own _output that must not bleed
            _entry("bar", "col_a", check="transform_b"),
            _entry("_output", "out_wrong", check="transform_b"),
        ]

        result_run1 = _resolve_output_col(alias_map, {"bar": "col_a"}, check_name="transform_a")
        result_run2 = _resolve_output_col(alias_map, {"bar": "col_b"}, check_name="transform_a")

        assert result_run1 == "out_a"
        assert result_run2 == "out_b"

    def test_returns_none_when_no_output_for_check(self):
        """Returns None when no _output entry exists for the given check_name."""
        alias_map = [
            _entry("bar", "col_a", check="transform_a"),
            _entry("_output", "result_a", check="transform_a"),
        ]
        col_backed = {"bar": "col_a"}

        result = _resolve_output_col(alias_map, col_backed, check_name="transform_b")
        assert result is None, (
            "No _output scoped to transform_b — must return None"
        )

    def test_returns_none_for_empty_alias_map(self):
        """Empty alias_map always returns None."""
        result = _resolve_output_col([], {"bar": "col_a"}, check_name="transform_a")
        assert result is None

    def test_legacy_entries_without_check_field_fall_through(self):
        """Legacy _output entries without 'check' field still resolve correctly."""
        alias_map = [
            _entry("bar", "col_a"),           # no check field
            _entry("_output", "legacy_out"),   # no check field
        ]
        col_backed = {"bar": "col_a"}

        # With check_name — legacy entries fall through (no "check" key → included)
        result = _resolve_output_col(alias_map, col_backed, check_name="any_transform")
        assert result == "legacy_out", (
            "Legacy entries without 'check' field must still resolve"
        )

    def test_no_check_name_uses_all_output_entries(self):
        """When check_name is None, all _output entries are candidates (legacy path)."""
        alias_map = [
            _entry("bar", "col_a", check="transform_a"),
            _entry("_output", "result_a", check="transform_a"),
        ]
        col_backed = {"bar": "col_a"}

        result = _resolve_output_col(alias_map, col_backed, check_name=None)
        assert result == "result_a"
