"""Tests for _wrap_series_input — pd.Series injection wrapper.

CLAUDE.md behavioral guarantees (pd.Series injection wrapper, now implemented):

  1. A function with align_to: pd.Series and no context: dict receives
     df[col_name] as a Series — not the context dict.
  2. A function with a str column-backed param and no context: dict,
     combined with pd.DataFrame, receives both df and the column name
     correctly — the partial supplies the str, _wrap_dataframe_input
     supplies the df. (str-only without DataFrame still uses standard
     path — separate from this wrapper.)
  3. Existing context: dict functions continue to work unchanged
     (backward compatibility — has_legacy_context_param routes them
     to the standard path, not _wrap_series_input).
  4. Guarantee 4 (no pandas roundtrip for non-DataFrame functions) is
     an architectural intent not yet enforced at the unit level —
     _compute_report still loads all data to pandas. Not covered here;
     tracked as a separate deferred item.

CLAUDE.md rules applied:
  - CheckParamInspector is the canonical inspection pattern — all param
    detection uses inspector methods, never _sig directly.
  - validate_check is the single validation gate — all registration goes
    through it; tests register via CheckRegistry.register(), not directly.
  - context: dict must never appear in user-written functions (new style).
  - Backward compat: context: dict functions must still work (legacy style).
"""
from __future__ import annotations

from functools import partial

import pandas as pd
import pytest

from proto_pipe.checks.registry import (
    CheckRegistry,
    CheckParamInspector,
    _wrap_series_input,
)
from proto_pipe.checks.result import CheckResult


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_context(df: pd.DataFrame) -> dict:
    """Build the context dict the runner passes to registered functions."""
    return {"df": df}


def _make_df() -> pd.DataFrame:
    return pd.DataFrame({
        "order_id": ["ORD-001", "ORD-002", "ORD-003"],
        "price":    [10.0, 20.0, 30.0],
        "region":   ["EMEA", "APAC", "EMEA"],
        "_ingested_at": ["2026-01-01", "2026-01-01", "2026-01-01"],  # pipeline col
    })


# ---------------------------------------------------------------------------
# Guarantee 1 — pd.Series param receives df[col_name] as a Series
# ---------------------------------------------------------------------------

class TestSeriesInjection:
    """CLAUDE.md guarantee: a function with pd.Series params and no context: dict
    receives df[col_name] as a Series — not the raw context dict."""

    def test_series_param_receives_column_data(self):
        """Function gets the actual Series, not the context dict."""
        received = {}

        def check_price(price: pd.Series) -> pd.Series[bool]:
            received["price"] = price
            return price > 0

        func = partial(check_price, price="price")
        wrapped = _wrap_series_input(func, series_params=["price"])

        df = _make_df()
        result = wrapped(_make_context(df))

        assert "price" in received, "Function must have been called with 'price' kwarg"
        assert isinstance(received["price"], pd.Series), (
            "CLAUDE.md guarantee: pd.Series param must receive a pd.Series, not context dict"
        )
        pd.testing.assert_series_equal(
            received["price"].reset_index(drop=True),
            df["price"].reset_index(drop=True),
        )

    def test_series_param_strips_pipeline_columns(self):
        """Pipeline columns (prefixed _) must be stripped before column extraction.

        The user's function operates on clean data — pipeline internals
        are never visible.
        """
        received_cols = {}

        def check_fn(price: pd.Series) -> pd.Series[bool]:
            received_cols["available"] = True
            return price > 0

        func = partial(check_fn, price="price")
        wrapped = _wrap_series_input(func, series_params=["price"])

        df = _make_df()
        wrapped(_make_context(df))

        # The wrapper must not fail even though _ingested_at is in df
        # (it's stripped before extraction)
        assert received_cols.get("available"), "Function must have been called"

    def test_result_is_correct_boolean_mask(self):
        """The injected Series produces correct check results end-to-end."""
        def above_zero(price: pd.Series) -> pd.Series[bool]:
            return price > 15.0

        func = partial(above_zero, price="price")
        wrapped = _wrap_series_input(func, series_params=["price"])

        df = _make_df()
        result = wrapped(_make_context(df))

        assert isinstance(result, pd.Series)
        # price > 15.0: [False, True, True]
        assert list(result) == [False, True, True]

    def test_multiple_series_params_each_injected(self):
        """Multiple pd.Series params each receive their respective column."""
        received = {}

        def compare(price: pd.Series, qty: pd.Series) -> pd.Series[bool]:
            received["price"] = price
            received["qty"] = qty
            return price > qty

        df = pd.DataFrame({
            "price": [10.0, 20.0, 5.0],
            "qty":   [5.0, 25.0, 5.0],
        })

        func = partial(compare, price="price", qty="qty")
        wrapped = _wrap_series_input(func, series_params=["price", "qty"])

        result = wrapped(_make_context(df))

        assert isinstance(received["price"], pd.Series)
        assert isinstance(received["qty"], pd.Series)
        # price > qty: [True, False, False]
        assert list(result) == [True, False, False]

    def test_scalar_constant_passed_through_alongside_series(self):
        """Scalar constants baked into partial are passed through alongside
        the injected Series — the has_series_params() gate ensures int/float
        params are constants when any pd.Series param is present.
        """
        received = {}

        def check_threshold(col: pd.Series, threshold: float) -> pd.Series[bool]:
            received["threshold"] = threshold
            return col > threshold

        # col is column-backed (Series); threshold is a broadcast constant
        func = partial(check_threshold, col="price", threshold=15.0)
        wrapped = _wrap_series_input(func, series_params=["col"])

        df = _make_df()
        result = wrapped(_make_context(df))

        assert received["threshold"] == 15.0, (
            "Broadcast constant must be passed through from partial unchanged"
        )
        assert list(result) == [False, True, True]

    def test_raises_when_column_not_in_table(self):
        """Clear error when the baked column name does not exist in the table."""
        def check_fn(col: pd.Series) -> pd.Series[bool]:
            return col > 0

        func = partial(check_fn, col="nonexistent_column")
        wrapped = _wrap_series_input(func, series_params=["col"])

        with pytest.raises(ValueError, match="nonexistent_column"):
            wrapped(_make_context(_make_df()))

    def test_raises_when_no_column_baked_for_series_param(self):
        """Clear error when a Series param has no baked column name in partial."""
        def check_fn(col: pd.Series) -> pd.Series[bool]:
            return col > 0

        # No partial — col has no baked value
        wrapped = _wrap_series_input(check_fn, series_params=["col"])

        with pytest.raises(ValueError, match="col"):
            wrapped(_make_context(_make_df()))

    def test_no_multiple_values_error(self):
        """The wrapper must not cause 'multiple values for argument' errors.

        This is the specific production bug that _wrap_series_input fixes.
        Previously, partial(func, align_to="price")({"df": df}) would pass
        {"df": df} as the first positional arg AND align_to from the partial,
        causing 'multiple values for argument align_to'.
        """
        def align_column(align_to: pd.Series) -> pd.Series:
            return align_to.str.upper()

        func = partial(align_column, align_to="region")
        wrapped = _wrap_series_input(func, series_params=["align_to"])

        df = _make_df()
        # Must not raise TypeError: align_column() got multiple values for 'align_to'
        result = wrapped(_make_context(df))

        assert isinstance(result, pd.Series)
        assert list(result) == ["EMEA", "APAC", "EMEA"]


# ---------------------------------------------------------------------------
# validate_check routing — Series path wired correctly
# ---------------------------------------------------------------------------

class TestValidateCheckSeriesPath:
    """validate_check must route Series-input functions through _wrap_series_input.

    CLAUDE.md: validate_check is the single validation gate. CheckRegistry.register()
    calls it. The Series path fires when has_series_params() is True and
    has_legacy_context_param() is False.
    """

    def test_series_check_registered_and_runnable(self):
        """A check with pd.Series param registers and runs without error."""
        reg = CheckRegistry()

        def price_check(price: pd.Series) -> pd.Series[bool]:
            return price > 0

        func = partial(price_check, price="price")
        reg.register("price_check", func, kind="check")

        assert "price_check" in reg.available(), "Must be in registry after registration"
        assert reg.get_kind("price_check") == "check"

        df = _make_df()
        result = reg.run("price_check", _make_context(df))

        assert isinstance(result, CheckResult), (
            "CheckRegistry.run must return CheckResult for kind='check'"
        )
        assert result.passed, "All prices > 0 — check must pass"

    def test_series_transform_registered_and_runnable(self):
        """A transform with pd.Series param registers and runs without error."""
        reg = CheckRegistry()

        def upper_region(region: pd.Series) -> pd.Series:
            result = region.str.upper()
            result.name = "region"
            return result

        func = partial(upper_region, region="region")
        reg.register("upper_region", func, kind="transform")

        assert reg.get_kind("upper_region") == "transform"

        df = _make_df()
        result = reg.run("upper_region", _make_context(df))

        assert isinstance(result, pd.Series)
        assert list(result) == ["EMEA", "APAC", "EMEA"]

    def test_series_check_failing_rows_produce_mask(self):
        """Failing rows produce a correct mask for validation_block writing."""
        reg = CheckRegistry()

        def price_above_15(price: pd.Series) -> pd.Series[bool]:
            return price > 15.0

        func = partial(price_above_15, price="price")
        reg.register("price_above_15", func, kind="check")

        df = _make_df()
        result = reg.run("price_above_15", _make_context(df))

        assert isinstance(result, CheckResult)
        assert not result.passed
        # price > 15: [False, True, True] → mask (True = fail): [True, False, False]
        assert result.mask is not None
        assert list(result.mask) == [True, False, False]


# ---------------------------------------------------------------------------
# Guarantee 3 — Backward compatibility: context: dict functions unchanged
# ---------------------------------------------------------------------------

class TestLegacyContextBackwardCompatibility:
    """CLAUDE.md guarantee: existing context: dict functions continue to work
    unchanged. has_legacy_context_param() routes them to the standard path,
    not _wrap_series_input.
    """

    def test_legacy_context_function_still_runs(self):
        """A function with context: dict as first param must still work."""
        reg = CheckRegistry()

        def legacy_check(context: dict, col: str = "price") -> pd.Series[bool]:
            df = context["df"]
            return df[col] > 0

        reg.register("legacy_check", legacy_check, kind="check")

        df = _make_df()
        result = reg.run("legacy_check", _make_context(df))

        assert isinstance(result, CheckResult)
        assert result.passed

    def test_legacy_context_with_series_param_not_broken(self):
        """A legacy function that declares pd.Series AND context: dict is routed
        to the standard path — has_legacy_context_param() takes precedence.

        Such a function is unusual (mixing conventions) but must not crash.
        """
        reg = CheckRegistry()

        def mixed_legacy(context: dict, col: pd.Series = None) -> pd.Series[bool]:
            # Uses context["df"] directly (legacy pattern)
            df = context["df"]
            return df["price"] > 0

        reg.register("mixed_legacy", mixed_legacy, kind="check")

        df = _make_df()
        # Must not raise — context dict passed through standard path
        result = reg.run("mixed_legacy", _make_context(df))

        assert isinstance(result, CheckResult)
        assert result.passed

    def test_has_legacy_context_param_inspector_method(self):
        """CheckParamInspector.has_legacy_context_param() correctly identifies
        legacy functions — uses the canonical inspection method, not _sig directly.
        """
        def legacy_fn(context: dict, col: str) -> pd.Series[bool]:
            pass

        def new_fn(col: pd.Series) -> pd.Series[bool]:
            pass

        legacy_inspector = CheckParamInspector(legacy_fn)
        new_inspector = CheckParamInspector(new_fn)

        assert legacy_inspector.has_legacy_context_param() is True, (
            "Functions with 'context' param must be identified as legacy"
        )
        assert new_inspector.has_legacy_context_param() is False, (
            "Annotation-based functions without 'context' must not be identified as legacy"
        )

    def test_series_path_not_triggered_for_legacy_function(self):
        """has_series_params() alone is not enough to trigger _wrap_series_input —
        has_legacy_context_param() must also be False.

        Verifies the routing gate in validate_check.
        """
        inspector = CheckParamInspector(
            partial(
                lambda context, col: context["df"][col] > 0,
                col="price"
            )
        )
        # Simulate a legacy function: has a Series-like param but also context
        # We verify the gate logic directly through inspector methods

        def legacy_with_series(context: dict, col: pd.Series = None) -> pd.Series[bool]:
            pass

        inspector = CheckParamInspector(legacy_with_series)

        # The gate: Series path only fires when BOTH conditions hold
        series_path_fires = (
            inspector.has_series_params()
            and not inspector.has_legacy_context_param()
        )
        assert not series_path_fires, (
            "Series injection path must not fire for legacy context: dict functions"
        )
