"""Tests for _wrap_scalar_column_input — scalar column-backed injection wrapper.

CLAUDE.md behavioral guarantees (lines 722-725):

  1. foo(bar: str) where bar="price" (column-backed) receives df["price"]
     applied per row — not the context dict.
  2. foo(bar: str) where bar="EMEA" (constant, not a column name) receives
     the constant string — no per-row application.
  3. Legacy foo(context: dict, bar: str) functions continue to work unchanged.
  4. The wrapper correctly re-uses the same function across different reports
     (different df at call time).

CLAUDE.md rules applied:
  - CheckParamInspector is the canonical inspection pattern — inspector methods
    used throughout, never _sig directly.
  - validate_check is the single validation gate — all registration goes through
    CheckRegistry.register().
  - context: dict must never appear in user-written annotation-based functions.
  - Backward compat: legacy context: dict functions must still work.
  - Col-backed detection: isinstance(val, str) and val in clean_df.columns —
    made at call time so same wrapper works across different reports/tables.
"""
from __future__ import annotations

from functools import partial

import pandas as pd
import pytest

from proto_pipe.checks.registry import (
    CheckRegistry,
    CheckParamInspector,
    _wrap_scalar_column_input,
)
from proto_pipe.checks.result import CheckResult


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _ctx(df: pd.DataFrame) -> dict:
    return {"df": df}


def _make_sales_df() -> pd.DataFrame:
    return pd.DataFrame({
        "order_id": ["ORD-001", "ORD-002", "ORD-003"],
        "price":    [10.0, 20.0, 30.0],
        "region":   ["EMEA", "APAC", "EMEA"],
        "_ingested_at": ["2026-01-01"] * 3,  # pipeline col — must be stripped
    })


def _make_inventory_df() -> pd.DataFrame:
    return pd.DataFrame({
        "sku":         ["SKU-001", "SKU-002"],
        "stock_level": [100, 0],
        "region":      ["EMEA", "APAC"],
    })


# ---------------------------------------------------------------------------
# Guarantee 1 — column-backed str param receives per-row scalar values
# ---------------------------------------------------------------------------

class TestColumnBackedStrParam:
    """CLAUDE.md guarantee 1: foo(bar: str) where bar="price" (matches a column)
    receives df["price"] applied per row — not the context dict.

    Per annotation table: str/unannotated column-backed → scalar values from
    df[col_name] applied per row via pandas .apply().
    Col-backed detection: isinstance(val, str) and val in clean_df.columns.
    """

    def test_column_backed_str_receives_per_row_scalars(self):
        """Function receives individual row values, not the context dict."""
        received = []

        def check_price(price: str) -> bool:
            received.append(price)
            return float(price) > 0

        func = partial(check_price, price="price")
        wrapped = _wrap_scalar_column_input(func)

        df = _make_sales_df()
        result = wrapped(_ctx(df))

        assert len(received) == 3, "Function must be called once per row"
        # Values come from df["price"] — not context dict, not column name
        assert received == [10.0, 20.0, 30.0], (
            "CLAUDE.md guarantee 1: function must receive per-row scalar values "
            "from df['price'], not the context dict or column name string"
        )

    def test_column_backed_str_result_is_series(self):
        """Per-row apply assembles a Series result."""
        def above_fifteen(price: str) -> bool:
            return float(price) > 15.0

        func = partial(above_fifteen, price="price")
        wrapped = _wrap_scalar_column_input(func)

        df = _make_sales_df()
        result = wrapped(_ctx(df))

        assert isinstance(result, pd.Series), (
            "Per-row apply must assemble a pd.Series from scalar bool returns"
        )
        assert list(result) == [False, True, True], (
            "price > 15: [False, True, True]"
        )

    def test_column_backed_result_name_set_for_transform_writeback(self):
        """Single col-backed param sets result.name to the column name.

        CLAUDE.md: single col-backed param sets result.name for transform
        write-back routing in _apply_transforms_with_gate.
        """
        def upper_region(region: str) -> str:
            return region.upper()

        func = partial(upper_region, region="region")
        wrapped = _wrap_scalar_column_input(func)

        df = _make_sales_df()
        result = wrapped(_ctx(df))

        assert result.name == "region", (
            "result.name must be set to the col-backed column name "
            "so _apply_transforms_with_gate can route the write-back"
        )

    def test_pipeline_columns_stripped_before_col_backed_detection(self):
        """Pipeline _-prefixed columns are stripped before col-backed detection.

        A param baked as '_ingested_at' must not be treated as col-backed
        even though that column exists in df before stripping.
        """
        received = []

        def check_fn(col: str) -> bool:
            received.append(col)
            return True

        # Bake a non-existent column name — should be treated as constant
        func = partial(check_fn, col="nonexistent")
        wrapped = _wrap_scalar_column_input(func)

        df = _make_sales_df()
        result = wrapped(_ctx(df))

        # Called once with the constant string, not per row
        assert len(received) == 1
        assert received[0] == "nonexistent"

    def test_no_multiple_values_error_for_str_param_function(self):
        """The wrapper must not cause 'multiple values for argument' errors.

        This is the str-param equivalent of the pd.Series production bug —
        partial(func, bar="price")({"df": df}) would pass {"df": df} positionally
        AND bar="price" from the partial, causing collision.
        """
        def normalise(region: str) -> str:
            return region.lower()

        func = partial(normalise, region="region")
        wrapped = _wrap_scalar_column_input(func)

        df = _make_sales_df()
        # Must not raise TypeError: normalise() got multiple values for 'region'
        result = wrapped(_ctx(df))

        assert isinstance(result, pd.Series)
        assert list(result) == ["emea", "apac", "emea"]


# ---------------------------------------------------------------------------
# Guarantee 2 — constant str param (not a column) passes through unchanged
# ---------------------------------------------------------------------------

class TestConstantStrParam:
    """CLAUDE.md guarantee 2: foo(bar: str) where bar="EMEA" (not a column name)
    receives the constant string — no per-row application.

    Col-backed detection: isinstance(val, str) and val in clean_df.columns.
    "EMEA" is not a column name → treated as broadcast constant.
    """

    def test_constant_str_not_in_columns_passes_through(self):
        """Non-column string constant received as-is — not applied per row."""
        received = []

        def check_literal(label: str) -> bool:
            received.append(label)
            return label == "EMEA"

        # "EMEA" is a value, not a column name — must be treated as constant
        func = partial(check_literal, label="EMEA")
        wrapped = _wrap_scalar_column_input(func)

        df = _make_sales_df()
        result = wrapped(_ctx(df))

        assert len(received) == 1, (
            "CLAUDE.md guarantee 2: constant str must call function once, "
            "not per-row — 'EMEA' is not a column name so no .apply() fires"
        )
        assert received[0] == "EMEA"

    def test_float_constant_passes_through_unchanged(self):
        """Non-str baked values (float, int) are always constants — isinstance
        check ensures only strings are tested against column names.
        """
        received = []

        def check_threshold(threshold: float) -> bool:
            received.append(threshold)
            return threshold > 0

        func = partial(check_threshold, threshold=0.5)
        wrapped = _wrap_scalar_column_input(func)

        df = _make_sales_df()
        wrapped(_ctx(df))

        assert len(received) == 1
        assert received[0] == 0.5

    def test_mixed_col_backed_and_constant(self):
        """One col-backed str param and one constant float — col-backed gets
        per-row apply, constant broadcasts across all rows.
        """
        received_vals = []
        received_thresholds = []

        def check_price_threshold(price: str, threshold: float) -> bool:
            received_vals.append(price)
            received_thresholds.append(threshold)
            return float(price) > threshold

        # price="price" is col-backed; threshold=15.0 is constant
        func = partial(check_price_threshold, price="price", threshold=15.0)
        wrapped = _wrap_scalar_column_input(func)

        df = _make_sales_df()
        result = wrapped(_ctx(df))

        assert len(received_vals) == 3, "Col-backed param applied per row"
        assert all(t == 15.0 for t in received_thresholds), (
            "Constant threshold must broadcast unchanged across all rows"
        )
        assert list(result) == [False, True, True]


# ---------------------------------------------------------------------------
# Guarantee 3 — legacy context: dict functions unchanged
# ---------------------------------------------------------------------------

class TestLegacyBackwardCompatibility:
    """CLAUDE.md guarantee 3: legacy foo(context: dict, bar: str) functions
    continue to work unchanged.

    has_legacy_context_param() routes them to the standard path — they never
    reach _wrap_scalar_column_input.
    """

    def test_legacy_context_function_still_works(self):
        """Legacy function receives context dict and accesses df correctly."""
        reg = CheckRegistry()

        def legacy_check(context: dict, col: str = "price") -> pd.Series[bool]:
            df = context["df"]
            return df[col] > 0

        reg.register("legacy_check_scalar", legacy_check, kind="check")

        df = _make_sales_df()
        result = reg.run("legacy_check_scalar", _ctx(df))

        assert isinstance(result, CheckResult)
        assert result.passed

    def test_has_legacy_context_param_routes_away_from_scalar_wrapper(self):
        """has_legacy_context_param() correctly identifies legacy functions.

        CLAUDE.md canonical inspection pattern — use inspector method,
        never access _sig directly.
        """
        def legacy_fn(context: dict, col: str) -> pd.Series[bool]:
            pass

        def new_fn(col: str) -> bool:
            pass

        assert CheckParamInspector(legacy_fn).has_legacy_context_param() is True
        assert CheckParamInspector(new_fn).has_legacy_context_param() is False

    def test_scalar_path_not_triggered_for_legacy_function(self):
        """Verify gate: scalar wrapper fires only when has_legacy_context_param()
        is False. Legacy functions bypass it entirely.
        """
        def legacy_fn(context: dict, col: str) -> pd.Series[bool]:
            pass

        inspector = CheckParamInspector(legacy_fn)
        scalar_path_fires = not inspector.has_legacy_context_param()

        assert not scalar_path_fires, (
            "Scalar injection path must not fire for legacy context: dict functions"
        )


# ---------------------------------------------------------------------------
# Guarantee 4 — same function reuses across different reports (different df)
# ---------------------------------------------------------------------------

class TestReportReuse:
    """CLAUDE.md guarantee 4: the wrapper correctly re-uses the same function
    across different reports (different df at call time).

    Col-backed detection happens at call time — `val in clean_df.columns` uses
    whichever df the runner passes for the current report. Same registered
    wrapper, different data.
    """

    def test_same_wrapper_produces_correct_results_for_different_dfs(self):
        """Same registered function, called with two different DataFrames,
        produces results from the correct table each time.
        """
        def check_positive(col: str) -> bool:
            return float(col) > 0

        # Both reports bake "price" — but each report's df has different price values
        func = partial(check_positive, col="price")
        wrapped = _wrap_scalar_column_input(func)

        df_a = pd.DataFrame({"price": [10.0, 20.0]})
        df_b = pd.DataFrame({"price": [-5.0, 50.0]})

        result_a = wrapped(_ctx(df_a))
        result_b = wrapped(_ctx(df_b))

        assert list(result_a) == [True, True], "df_a: all prices positive"
        assert list(result_b) == [False, True], "df_b: first price negative"

    def test_col_backed_detection_uses_current_df_columns(self):
        """Same baked value ("region") is col-backed in sales df but not in
        a df without that column — detection is always against current df.
        """
        received_counts = []

        def check_fn(region: str) -> bool:
            received_counts.append(region)
            return True

        func = partial(check_fn, region="region")
        wrapped = _wrap_scalar_column_input(func)

        # df with "region" column → col-backed → per-row apply (3 calls)
        df_with_region = _make_sales_df()
        wrapped(_ctx(df_with_region))
        calls_with_column = len(received_counts)

        received_counts.clear()

        # df without "region" column → "region" is a constant → 1 call
        df_without_region = pd.DataFrame({"price": [1.0, 2.0, 3.0]})
        wrapped(_ctx(df_without_region))
        calls_without_column = len(received_counts)

        assert calls_with_column == 3, (
            "CLAUDE.md guarantee 4: col-backed detection uses current df — "
            "'region' column present means per-row apply fires (3 rows)"
        )
        assert calls_without_column == 1, (
            "'region' not in df.columns → treated as constant → called once"
        )

    def test_registered_check_runs_correctly_across_two_registries(self):
        """Same function registered in two independent CheckRegistry instances
        (simulating two separate reports) — each uses its own df.
        """
        def price_positive(price: str) -> bool:
            return float(price) > 0

        reg_a = CheckRegistry()
        reg_b = CheckRegistry()

        func_a = partial(price_positive, price="price")
        func_b = partial(price_positive, price="price")

        reg_a.register("price_check_a", func_a, kind="check")
        reg_b.register("price_check_b", func_b, kind="check")

        df_a = pd.DataFrame({"price": [10.0, 20.0]})
        df_b = pd.DataFrame({"price": [-1.0, 5.0]})

        result_a = reg_a.run("price_check_a", _ctx(df_a))
        result_b = reg_b.run("price_check_b", _ctx(df_b))

        assert isinstance(result_a, CheckResult) and result_a.passed
        assert isinstance(result_b, CheckResult) and not result_b.passed


# ---------------------------------------------------------------------------
# returns_scalar_bool — inspector method
# ---------------------------------------------------------------------------

class TestReturnsScalarBool:
    """CheckParamInspector.returns_scalar_bool() must correctly identify
    functions returning a scalar bool vs pd.Series[bool].

    CLAUDE.md canonical inspection pattern — new method added alongside
    returns_boolean_series() for the scalar check path in validate_check.
    """

    def test_bool_return_annotation_detected(self):
        def fn() -> bool:
            pass
        assert CheckParamInspector(fn).returns_scalar_bool() is True

    def test_series_bool_return_not_detected_as_scalar(self):
        def fn() -> pd.Series[bool]:
            pass
        assert CheckParamInspector(fn).returns_scalar_bool() is False

    def test_no_annotation_not_detected(self):
        def fn():
            pass
        assert CheckParamInspector(fn).returns_scalar_bool() is False

    def test_scalar_bool_check_accepted_by_validate_check(self):
        """validate_check must accept -> bool for kind='check' in scalar path.

        CLAUDE.md: scalar checks may return bool (per-row) or pd.Series[bool]
        (vectorised) — both are valid in the scalar injection path.
        """
        reg = CheckRegistry()

        def region_check(region: str) -> bool:
            return region in ("EMEA", "APAC")

        func = partial(region_check, region="region")
        reg.register("region_check", func, kind="check")

        assert "region_check" in reg.available(), (
            "-> bool return annotation must be accepted for scalar checks"
        )
        assert "region_check" not in reg._bad_checks, (
            "Scalar bool check must not land in _bad_checks"
        )
