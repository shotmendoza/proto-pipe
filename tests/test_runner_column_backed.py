"""Tests for column-backed scalar transform execution path in reports/runner.py.

Behavioral guarantees (CLAUDE.md):
  - Column-backed scalar params (int/float in alias_map, value is a df column name)
    → SQL column references in DuckDB UDF: func_udf(col_a, col_b)
  - _output alias_map entry → write-back column for the UDF result
  - Without _output: falls back to first column-backed param's column (legacy)
  - Broadcast constant params: baked into partial via closure, not SQL column refs
  - TRY_CAST gate still applies after UDF run (existing guarantee, not repeated here)
  - _resolve_output_col: correct run index matching when N expansions present
"""
from __future__ import annotations

import functools
from unittest.mock import MagicMock, patch, call

import duckdb
import pandas as pd
import pytest

from proto_pipe.reports.transforms import (
    _apply_scalar_transform_duckdb,
    _apply_transforms_with_gate,
    _resolve_output_col,
)
from proto_pipe.checks.registry import CheckRegistry


# ---------------------------------------------------------------------------
# _resolve_output_col — unit tests
# ---------------------------------------------------------------------------

class TestResolveOutputCol:
    """Behavioral guarantee: _resolve_output_col finds the correct write-back
    column by matching baked-in column values to alias_map run index."""

    def test_single_run_returns_output_col(self):
        """Single expansion → _output[0] returned."""
        alias_map = [
            {"param": "bar", "column": "price_col"},
            {"param": "_output", "column": "result_col"},
        ]
        result = _resolve_output_col(alias_map, {"bar": "price_col"})
        assert result == "result_col"

    def test_multi_run_first_index_correct(self):
        """bar=price_col is run 0 → _output[0] = result_a."""
        alias_map = [
            {"param": "bar", "column": "price_col"},
            {"param": "bar", "column": "qty_col"},
            {"param": "_output", "column": "result_a"},
            {"param": "_output", "column": "result_b"},
        ]
        result = _resolve_output_col(alias_map, {"bar": "price_col"})
        assert result == "result_a"

    def test_multi_run_second_index_correct(self):
        """bar=qty_col is run 1 → _output[1] = result_b."""
        alias_map = [
            {"param": "bar", "column": "price_col"},
            {"param": "bar", "column": "qty_col"},
            {"param": "_output", "column": "result_a"},
            {"param": "_output", "column": "result_b"},
        ]
        result = _resolve_output_col(alias_map, {"bar": "qty_col"})
        assert result == "result_b"

    def test_no_output_entries_returns_none(self):
        """When no _output in alias_map, returns None (caller uses fallback)."""
        alias_map = [{"param": "col", "column": "endt"}]
        result = _resolve_output_col(alias_map, {"col": "endt"})
        assert result is None

    def test_empty_alias_map_returns_none(self):
        """Empty alias_map returns None."""
        result = _resolve_output_col([], {"bar": "price_col"})
        assert result is None

    def test_unmatched_baked_col_returns_none(self):
        """Baked column not found in alias_map entries returns None."""
        alias_map = [
            {"param": "bar", "column": "price_col"},
            {"param": "_output", "column": "result_col"},
        ]
        result = _resolve_output_col(alias_map, {"bar": "unknown_col"})
        assert result is None


# ---------------------------------------------------------------------------
# _apply_scalar_transform_duckdb — integration tests
# ---------------------------------------------------------------------------

class TestApplyScalarTransformDuckdb:
    """Behavioral guarantee: column-backed scalar params become SQL column refs."""

    @pytest.fixture()
    def db_with_table(self, tmp_path):
        db_path = str(tmp_path / "test.db")
        conn = duckdb.connect(db_path)
        df = pd.DataFrame({
            "price_col": [10.0, 20.0, 30.0],
            "floor_col": [5.0, 5.0, 5.0],
            "result_col": [0.0, 0.0, 0.0],
            "id": [1, 2, 3],
        })
        conn.execute("CREATE TABLE sales AS SELECT * FROM df")
        return conn, "sales"

    def test_column_backed_float_updates_output_col(self, db_with_table):
        """float param backed by df column → UDF receives column values per row,
        result written to _output column."""
        conn, table = db_with_table

        def add_floor(price: float, floor: float) -> float:
            return price + floor

        # Simulate partial with column names as baked values
        func = functools.partial(add_floor, price="price_col", floor="floor_col")

        alias_map = [
            {"param": "price", "column": "price_col"},
            {"param": "floor", "column": "floor_col"},
            {"param": "_output", "column": "result_col"},
        ]

        df = conn.execute(f'SELECT * FROM "{table}"').df()
        conn.close()

        # New interface: no target_table or conn — returns modified DataFrame
        result_df = _apply_scalar_transform_duckdb(
            name="add_floor",
            func=func,
            df=df,
            registry_types={"result_col": "DOUBLE"},
            alias_map=alias_map,
        )

        assert list(result_df.sort_values("id")["result_col"]) == [15.0, 25.0, 35.0], (
            "UDF must receive per-row column values and write sum to result_col"
        )

    def test_no_output_fallback_writes_to_input_col(self, db_with_table):
        """Without _output in alias_map: result written back to first column-backed
        param's column (legacy single-column transform behaviour)."""
        conn, table = db_with_table

        def double(price: float) -> float:
            return price * 2

        func = functools.partial(double, price="price_col")

        alias_map = [{"param": "price", "column": "price_col"}]  # no _output

        df = conn.execute(f'SELECT * FROM "{table}"').df()
        conn.close()

        # New interface: no target_table or conn — returns modified DataFrame
        result_df = _apply_scalar_transform_duckdb(
            name="double",
            func=func,
            df=df,
            registry_types={"price_col": "DOUBLE"},
            alias_map=alias_map,
        )

        assert list(result_df.sort_values("id")["price_col"]) == [20.0, 40.0, 60.0], (
            "Without _output, result must write back to input column (price_col)"
        )

    def test_constant_params_not_treated_as_column_refs(self, db_with_table):
        """Broadcast constant int/float params (non-column values in partial)
        are NOT used as SQL column references — they stay as closure values."""
        conn, table = db_with_table

        def clamp(price: float, max_val: float) -> float:
            return min(price, max_val)

        # price is column-backed, max_val is a constant (not in df.columns)
        func = functools.partial(clamp, price="price_col", max_val=15.0)

        alias_map = [
            {"param": "price", "column": "price_col"},
            {"param": "_output", "column": "result_col"},
        ]

        df = conn.execute(f'SELECT * FROM "{table}"').df()
        conn.close()

        # New interface: no target_table or conn — returns modified DataFrame
        result_df = _apply_scalar_transform_duckdb(
            name="clamp",
            func=func,
            df=df,
            registry_types={"result_col": "DOUBLE"},
            alias_map=alias_map,
        )

        assert list(result_df.sort_values("id")["result_col"]) == [10.0, 15.0, 15.0], (
            "max_val=15.0 must be treated as a constant, not a column reference"
        )

    def test_raises_when_no_column_backed_params(self):
        """Raises ValueError when no column-backed params can be identified."""
        def no_col(x: float) -> float:
            return x

        # No column-backed params — x is a constant baked in (5.0 not in df.columns)
        func = functools.partial(no_col, x=5.0)
        df = pd.DataFrame({"a": [1.0]})

        # New interface: no target_table or conn required
        with pytest.raises(ValueError, match="no column-backed params"):
            _apply_scalar_transform_duckdb(
                name="no_col", func=func, df=df,
                registry_types={}, alias_map=[],
            )
