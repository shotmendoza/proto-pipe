
"""Tests for validation_pipeline.checks.built_in and the CheckRegistry.

Covers:
- check_nulls
- check_range
- check_schema
- check_duplicates
- CheckRegistry.register / run / available
- Invalid check name raises
"""

import pytest

from src.checks.built_in import (
    check_nulls,
    check_range,
    check_schema,
    check_duplicates,
)
from src.registry.base import CheckRegistry


# ---------------------------------------------------------------------------
# check_nulls
# ---------------------------------------------------------------------------

class TestCheckNulls:
    def test_no_nulls(self, sales_df):
        result = check_nulls({"df": sales_df})
        assert result["has_nulls"] is False
        assert all(v == 0 for v in result["null_counts"].values())

    def test_detects_nulls(self, sales_df_with_nulls):
        result = check_nulls({"df": sales_df_with_nulls})
        assert result["has_nulls"] is True
        assert result["null_counts"]["customer_id"] >= 1
        assert result["null_counts"]["price"] >= 1

    def test_returns_counts_per_column(self, sales_df_with_nulls):
        result = check_nulls({"df": sales_df_with_nulls})
        # Every column should be represented in the output
        assert set(result["null_counts"].keys()) == set(sales_df_with_nulls.columns)


# ---------------------------------------------------------------------------
# check_range
# ---------------------------------------------------------------------------

class TestCheckRange:
    def test_all_in_range(self, sales_df):
        result = check_range({"df": sales_df}, col="price", min_val=0, max_val=500)
        assert result["violations"] == 0
        assert result["violation_indices"] == []

    def test_detects_out_of_range(self, sales_df_out_of_range):
        result = check_range(
            {"df": sales_df_out_of_range}, col="price", min_val=0, max_val=500
        )
        assert result["violations"] == 1

    def test_boundary_values_are_inclusive(self, sales_df):
        # 99.99 is exactly on the boundary and should pass
        # 15.50 is below min_val=99.99 and should fail
        result = check_range({"df": sales_df}, col="price", min_val=99.99, max_val=500)
        assert result["violations"] == 1
        # confirm the boundary row itself is NOT in violations
        assert 0 not in result["violation_indices"]  # index 0 is the 99.99 row

    def test_missing_column_raises(self, sales_df):
        with pytest.raises(ValueError, match="nonexistent"):
            check_range({"df": sales_df}, col="nonexistent", min_val=0, max_val=100)

    def test_violation_indices_are_correct(self, sales_df_out_of_range):
        result = check_range(
            {"df": sales_df_out_of_range}, col="price", min_val=0, max_val=500
        )
        # Row at index 1 has price -5.00
        assert 1 in result["violation_indices"]


# ---------------------------------------------------------------------------
# check_schema
# ---------------------------------------------------------------------------

class TestCheckSchema:
    def test_exact_match(self, sales_df):
        result = check_schema({"df": sales_df}, expected_cols=list(sales_df.columns))
        assert result["matches"] is True
        assert result["missing_cols"] == []
        assert result["extra_cols"] == []

    def test_detects_missing_columns(self, sales_df):
        result = check_schema(
            {"df": sales_df},
            expected_cols=list(sales_df.columns) + ["nonexistent_col"],
        )
        assert result["matches"] is False
        assert "nonexistent_col" in result["missing_cols"]

    def test_detects_extra_columns(self, sales_df):
        df = sales_df.copy()
        df["extra"] = "x"
        result = check_schema({"df": df}, expected_cols=list(sales_df.columns))
        assert result["matches"] is False
        assert "extra" in result["extra_cols"]

    def test_empty_expected_cols(self, sales_df):
        result = check_schema({"df": sales_df}, expected_cols=[])
        assert result["matches"] is False
        assert set(result["extra_cols"]) == set(sales_df.columns)


# ---------------------------------------------------------------------------
# check_duplicates
# ---------------------------------------------------------------------------

class TestCheckDuplicates:
    def test_no_duplicates(self, sales_df):
        result = check_duplicates({"df": sales_df})
        assert result["has_duplicates"] is False
        assert result["duplicate_count"] == 0

    def test_detects_duplicates(self, sales_df_with_duplicates):
        result = check_duplicates({"df": sales_df_with_duplicates})
        assert result["has_duplicates"] is True
        assert result["duplicate_count"] == 1

    def test_subset_scoping(self, sales_df_with_duplicates):
        # Scoping to order_id should catch the duplicate
        result = check_duplicates(
            {"df": sales_df_with_duplicates}, subset=["order_id"]
        )
        assert result["has_duplicates"] is True
        assert result["subset"] == ["order_id"]

    def test_subset_no_duplicates(self, sales_df):
        # All order_ids are unique — no duplicates on this subset
        result = check_duplicates({"df": sales_df}, subset=["order_id"])
        assert result["has_duplicates"] is False


# ---------------------------------------------------------------------------
# CheckRegistry
# ---------------------------------------------------------------------------

class TestCheckRegistry:
    def test_register_and_run(self, check_registry, sales_df):
        check_registry.register("null_check", check_nulls)
        result = check_registry.run("null_check", {"df": sales_df})
        assert "has_nulls" in result

    def test_available_lists_registered_checks(self, check_registry):
        check_registry.register("null_check", check_nulls)
        check_registry.register("dup_check", check_duplicates)
        assert "null_check" in check_registry.available()
        assert "dup_check" in check_registry.available()

    def test_unregistered_check_raises(self, check_registry, sales_df):
        with pytest.raises(ValueError, match="no_such_check"):
            check_registry.run("no_such_check", {"df": sales_df})

    def test_register_with_params(self, check_registry, sales_df):
        from functools import partial
        fn = partial(check_range, col="price", min_val=0, max_val=500)
        check_registry.register("price_range", fn)
        result = check_registry.run("price_range", {"df": sales_df})
        assert result["violations"] == 0

    def test_overwrite_registration(self, check_registry, sales_df):
        check_registry.register("my_check", check_nulls)
        check_registry.register("my_check", check_duplicates)
        result = check_registry.run("my_check", {"df": sales_df})
        # Should run check_duplicates (the latest registration)
        assert "duplicate_count" in result
