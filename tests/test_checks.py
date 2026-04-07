"""Tests for proto_pipe.checks.built_in and the CheckRegistry.

Covers:
- check_nulls
- check_range
- check_schema
- check_duplicates
- CheckRegistry.register / run / available
- Invalid check name raises
"""

import pandas as pd
import pytest

from proto_pipe.checks.built_in import (
    check_nulls,
    check_range,
    check_schema,
    check_duplicates,
)
from proto_pipe.checks.result import CheckResult


# ---------------------------------------------------------------------------
# check_nulls — returns pd.Series[bool], True = row passes (no nulls)
# ---------------------------------------------------------------------------

class TestCheckNulls:
    def test_no_nulls_all_rows_pass(self, sales_df):
        result = check_nulls({"df": sales_df})
        assert isinstance(result, pd.Series)
        assert result.all()

    def test_detects_nulls_rows_fail(self, sales_df_with_nulls):
        result = check_nulls({"df": sales_df_with_nulls})
        assert not result.all()
        assert (~result).sum() > 0

    def test_null_rows_correctly_identified(self, sales_df_with_nulls):
        result = check_nulls({"df": sales_df_with_nulls})
        # row 0 has null customer_id, row 2 has null price
        assert not result.iloc[0]
        assert not result.iloc[2]
        assert result.iloc[1]  # row 1 is clean


# ---------------------------------------------------------------------------
# check_range — returns pd.Series[bool], True = row passes (in range)
# ---------------------------------------------------------------------------

class TestCheckRange:
    def test_all_in_range_all_pass(self, sales_df):
        result = check_range({"df": sales_df}, col="price", min_val=0, max_val=500)
        assert isinstance(result, pd.Series)
        assert result.all()

    def test_detects_out_of_range(self, sales_df_out_of_range):
        result = check_range(
            {"df": sales_df_out_of_range}, col="price", min_val=0, max_val=500
        )
        assert (~result).sum() == 1

    def test_boundary_values_are_inclusive(self, sales_df):
        # 99.99 is exactly on boundary — passes
        # 15.50 is below min_val=99.99 — fails
        result = check_range({"df": sales_df}, col="price", min_val=99.99, max_val=500)
        assert (~result).sum() == 1
        assert result.iloc[0]  # 99.99 passes

    def test_missing_column_raises(self, sales_df):
        with pytest.raises(ValueError, match="nonexistent"):
            check_range({"df": sales_df}, col="nonexistent", min_val=0, max_val=100)

    def test_violation_row_correctly_identified(self, sales_df_out_of_range):
        result = check_range(
            {"df": sales_df_out_of_range}, col="price", min_val=0, max_val=500
        )
        # Row at index 1 has price -5.00
        assert not result.iloc[1]


# ---------------------------------------------------------------------------
# check_schema — returns pd.Series[bool], True = schema matches
# ---------------------------------------------------------------------------

class TestCheckSchema:
    def test_exact_match_all_pass(self, sales_df):
        result = check_schema({"df": sales_df}, expected_cols=list(sales_df.columns))
        assert isinstance(result, pd.Series)
        assert result.all()

    def test_missing_columns_all_fail(self, sales_df):
        result = check_schema(
            {"df": sales_df},
            expected_cols=list(sales_df.columns) + ["nonexistent_col"],
        )
        assert not result.any()

    def test_extra_columns_all_fail(self, sales_df):
        df = sales_df.copy()
        df["extra"] = "x"
        result = check_schema({"df": df}, expected_cols=list(sales_df.columns))
        assert not result.any()

    def test_empty_expected_cols_fails(self, sales_df):
        result = check_schema({"df": sales_df}, expected_cols=[])
        assert not result.any()


# ---------------------------------------------------------------------------
# check_duplicates — returns pd.Series[bool], True = row is not a duplicate
# ---------------------------------------------------------------------------

class TestCheckDuplicates:
    def test_no_duplicates_all_pass(self, sales_df):
        result = check_duplicates({"df": sales_df})
        assert isinstance(result, pd.Series)
        assert result.all()

    def test_detects_duplicates(self, sales_df_with_duplicates):
        result = check_duplicates({"df": sales_df_with_duplicates})
        assert not result.all()
        assert (~result).sum() == 1

    def test_subset_scoping_catches_duplicate(self, sales_df_with_duplicates):
        result = check_duplicates(
            {"df": sales_df_with_duplicates}, subset=["order_id"]
        )
        assert not result.all()

    def test_subset_no_duplicates_all_pass(self, sales_df):
        result = check_duplicates({"df": sales_df}, subset=["order_id"])
        assert result.all()


# ---------------------------------------------------------------------------
# CheckRegistry — run returns CheckResult
# ---------------------------------------------------------------------------

class TestCheckRegistry:
    def test_register_and_run_returns_check_result(self, check_registry, sales_df):
        check_registry.register("null_check", check_nulls)
        result = check_registry.run("null_check", {"df": sales_df})
        assert isinstance(result, CheckResult)
        assert result.passed is True

    def test_available_lists_registered_checks(self, check_registry):
        check_registry.register("null_check", check_nulls)
        check_registry.register("dup_check", check_duplicates)
        assert "null_check" in check_registry.available()
        assert "dup_check" in check_registry.available()

    def test_unregistered_check_raises(self, check_registry, sales_df):
        with pytest.raises(ValueError, match="no_such_check"):
            check_registry.run("no_such_check", {"df": sales_df})

    def test_register_with_params_returns_check_result(self, check_registry, sales_df):
        from functools import partial
        fn = partial(check_range, col="price", min_val=0, max_val=500)
        check_registry.register("price_range", fn)
        result = check_registry.run("price_range", {"df": sales_df})
        assert isinstance(result, CheckResult)
        assert result.passed is True

    def test_overwrite_registration(self, check_registry, sales_df):
        check_registry.register("my_check", check_nulls)
        check_registry.register("my_check", check_duplicates)
        result = check_registry.run("my_check", {"df": sales_df})
        assert isinstance(result, CheckResult)
        assert result.passed is True

    def test_failed_check_has_mask(self, check_registry, sales_df_out_of_range):
        from functools import partial
        fn = partial(check_range, col="price", min_val=0, max_val=500)
        check_registry.register("price_range", fn)
        result = check_registry.run("price_range", {"df": sales_df_out_of_range})
        assert result.passed is False
        assert result.mask is not None
        assert result.mask.sum() == 1

# ---------------------------------------------------------------------------
# Spec behavioral guarantee tests
# ---------------------------------------------------------------------------

class TestCheckSeriesAlignment:
    """All built-in checks return pd.Series with the same index as the input.

    Spec guarantee:
      'kind=check → returns pd.Series[bool]. True = row passes.'
    The Series must be aligned to the input DataFrame index so that
    failing rows can be correctly identified by position.
    """

    def test_check_nulls_index_matches_input(self, sales_df_with_nulls):
        result = check_nulls({"df": sales_df_with_nulls})
        assert list(result.index) == list(sales_df_with_nulls.index)

    def test_check_range_index_matches_input(self, sales_df_out_of_range):
        result = check_range({"df": sales_df_out_of_range}, col="price", min_val=0, max_val=500)
        assert list(result.index) == list(sales_df_out_of_range.index)

    def test_check_duplicates_index_matches_input(self, sales_df_with_duplicates):
        result = check_duplicates({"df": sales_df_with_duplicates})
        assert list(result.index) == list(sales_df_with_duplicates.index)

    def test_check_schema_index_matches_input(self, sales_df):
        result = check_schema({"df": sales_df}, expected_cols=list(sales_df.columns))
        assert list(result.index) == list(sales_df.index)


class TestCheckSchemaBehavior:
    """check_schema is table-level — all rows get the same pass/fail result.

    Spec guarantee:
      'kind=check → returns pd.Series[bool].'
    For schema mismatches, the failure is table-wide (the schema either
    matches or it doesn't), so every row must get the same boolean value.
    """

    def test_schema_mismatch_is_uniform_all_fail(self, sales_df):
        result = check_schema(
            {"df": sales_df},
            expected_cols=list(sales_df.columns) + ["missing_col"],
        )
        assert len(result.unique()) == 1, (
            "Schema check must return a uniform Series — all rows pass or all fail"
        )
        assert not result.iloc[0]

    def test_schema_match_is_uniform_all_pass(self, sales_df):
        result = check_schema({"df": sales_df}, expected_cols=list(sales_df.columns))
        assert len(result.unique()) == 1
        assert result.iloc[0]


class TestCheckNullsEdgeCases:
    def test_empty_dataframe_returns_empty_series(self):
        """check_nulls with an empty DataFrame must return empty Series, not raise.

        Spec guarantee: checks return pd.Series[bool]. An empty DataFrame
        is a valid input — e.g. after watermark filtering produces no rows.
        """
        df = pd.DataFrame({"order_id": [], "price": []})
        result = check_nulls({"df": df})
        assert isinstance(result, pd.Series)
        assert len(result) == 0
