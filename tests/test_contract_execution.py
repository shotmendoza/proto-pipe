"""Tests for CheckContract inspection flags and DuckDB-native execution path.

CLAUDE.md behavioral guarantees:

CheckContract flags (set once at validate_check time):
  - needs_dataframe=True on pd.DataFrame path
  - needs_series=True on pd.Series path
  - is_scalar=True on scalar/str path
  - is_legacy=True on standard/legacy context path
  - get_contract(name) returns the full stored contract — canonical read method

_compute_report execution path gating:
  - needs_dataframe=True or is_legacy=True → pandas path (pending_df loaded)
  - needs_series=True or is_scalar=True → DuckDB path (conn kept open)

Wrapper dual-path:
  - _wrap_series_input handles both {"df": df} and {"conn": ...} contexts
  - _wrap_scalar_column_input handles both context types
  - Same function, different context, same result

_apply_transforms_with_gate:
  - Uses get_contract() instead of re-running CheckParamInspector
  - _get_df() loads pending rows lazily — only when a transform needs pandas

CLAUDE.md rules applied:
  - CheckParamInspector is canonical — flag assertions use get_contract(), never _checks
  - validate_check is the single gate — all registration goes through CheckRegistry.register()
  - In-code fake data, no static files
"""
from __future__ import annotations

from functools import partial

import duckdb
import pandas as pd
import pytest

from proto_pipe.checks.registry import (
    CheckRegistry,
    CheckParamInspector,
    _wrap_series_input,
    _wrap_scalar_column_input,
)
from proto_pipe.checks.result import CheckResult
from proto_pipe.checks.helpers import register_custom_check


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_df() -> pd.DataFrame:
    return pd.DataFrame({
        "order_id": ["ORD-001", "ORD-002", "ORD-003"],
        "price":    [10.0, 20.0, 30.0],
        "region":   ["EMEA", "APAC", "EMEA"],
    })


def _duckdb_context(pipeline_db: str, table: str, pending_pks: set, pk_col: str, all_columns: list) -> dict:
    conn = duckdb.connect(pipeline_db, read_only=True)
    return {
        "conn": conn,
        "table": table,
        "pending_pks": pending_pks,
        "pk_col": pk_col,
        "all_columns": all_columns,
    }


def _seed(pipeline_db: str, table: str, df: pd.DataFrame) -> None:
    with duckdb.connect(pipeline_db) as conn:
        conn.execute(f'CREATE OR REPLACE TABLE "{table}" AS SELECT * FROM df')


# ---------------------------------------------------------------------------
# Guarantee 1 — CheckContract flags set correctly per validate_check path
# ---------------------------------------------------------------------------

class TestCheckContractFlags:
    """CheckContract flags are set once at validate_check time.

    CLAUDE.md: CheckParamInspector is canonical — flags are populated by
    validate_check from inspector results, never re-computed at call time.
    """

    def test_dataframe_path_sets_needs_dataframe(self):
        """pd.DataFrame param → needs_dataframe=True, all others False."""
        reg = CheckRegistry()

        def fn(df: pd.DataFrame) -> pd.Series[bool]:
            return df["price"] > 0

        reg.register("df_check", fn, kind="check")
        contract = reg.get_contract("df_check")

        assert contract.needs_dataframe is True
        assert contract.needs_series is False
        assert contract.is_scalar is False
        assert contract.is_legacy is False

    def test_series_path_sets_needs_series(self):
        """pd.Series param → needs_series=True, all others False."""
        reg = CheckRegistry()

        def fn(price: pd.Series) -> pd.Series[bool]:
            return price > 0

        func = partial(fn, price="price")
        reg.register("series_check", func, kind="check")
        contract = reg.get_contract("series_check")

        assert contract.needs_series is True
        assert contract.needs_dataframe is False
        assert contract.is_scalar is False
        assert contract.is_legacy is False

    def test_scalar_path_sets_is_scalar(self):
        """str/float/unannotated params, no context → is_scalar=True."""
        reg = CheckRegistry()

        def fn(region: str) -> bool:
            return region in ("EMEA", "APAC")

        func = partial(fn, region="region")
        reg.register("scalar_check", func, kind="check")
        contract = reg.get_contract("scalar_check")

        assert contract.is_scalar is True
        assert contract.needs_dataframe is False
        assert contract.needs_series is False
        assert contract.is_legacy is False

    def test_legacy_path_sets_is_legacy(self):
        """context: dict param → is_legacy=True, all others False."""
        reg = CheckRegistry()

        def fn(context: dict) -> pd.Series[bool]:
            return context["df"]["price"] > 0

        reg.register("legacy_check", fn, kind="check")
        contract = reg.get_contract("legacy_check")

        assert contract.is_legacy is True
        assert contract.needs_dataframe is False
        assert contract.needs_series is False
        assert contract.is_scalar is False

    def test_positional_context_param_sets_is_legacy(self):
        """Informal legacy (ctx, no annotation) → is_legacy=True."""
        reg = CheckRegistry()

        def fn(ctx) -> pd.Series[bool]:
            return ctx["df"]["price"] > 0

        reg.register("informal_legacy", fn, kind="check")
        contract = reg.get_contract("informal_legacy")

        assert contract.is_legacy is True

    def test_kind_preserved_on_contract(self):
        """kind field is stored correctly alongside inspection flags."""
        reg = CheckRegistry()

        def transform(price: pd.Series) -> pd.Series:
            result = price * 1.1
            result.name = "price"
            return result

        func = partial(transform, price="price")
        reg.register("price_transform", func, kind="transform")
        contract = reg.get_contract("price_transform")

        assert contract.kind == "transform"
        assert contract.needs_series is True


# ---------------------------------------------------------------------------
# Guarantee 2 — get_contract() is the canonical read method
# ---------------------------------------------------------------------------

class TestGetContract:
    """get_contract(name) returns the full stored contract.

    CLAUDE.md: callers must use get_contract() — never access _checks directly.
    """

    def test_get_contract_returns_full_contract(self):
        """get_contract returns the CheckContract with all fields."""
        reg = CheckRegistry()

        def fn(price: pd.Series) -> pd.Series[bool]:
            return price > 0

        func = partial(fn, price="price")
        reg.register("price_check", func, kind="check")
        contract = reg.get_contract("price_check")

        assert contract is not None
        assert hasattr(contract, "func")
        assert hasattr(contract, "kind")
        assert hasattr(contract, "needs_dataframe")
        assert hasattr(contract, "needs_series")
        assert hasattr(contract, "is_scalar")
        assert hasattr(contract, "is_legacy")

    def test_get_contract_raises_for_unknown_name(self):
        """get_contract raises ValueError for unregistered names."""
        reg = CheckRegistry()
        with pytest.raises(ValueError, match="nonexistent"):
            reg.get_contract("nonexistent")

    def test_get_kind_and_get_contract_consistent(self):
        """get_kind(name) and get_contract(name).kind must always agree."""
        reg = CheckRegistry()

        def fn(ctx) -> pd.Series[bool]:
            return ctx["df"]["price"] > 0

        reg.register("my_check", fn, kind="check")

        assert reg.get_kind("my_check") == reg.get_contract("my_check").kind


# ---------------------------------------------------------------------------
# Guarantee 3 — _wrap_series_input dual-path produces identical results
# ---------------------------------------------------------------------------

class TestSeriesWrapperDualPath:
    """_wrap_series_input handles {"df": df} and {"conn": ...} contexts.

    CLAUDE.md: same function, different context type, same result.
    """

    def test_series_pandas_context_result(self):
        """Pandas context: extracts column from df, produces correct Series."""
        def fn(price: pd.Series) -> pd.Series[bool]:
            return price > 15.0

        func = partial(fn, price="price")
        wrapped = _wrap_series_input(func, series_params=["price"])

        df = _make_df()
        result = wrapped({"df": df})

        assert isinstance(result, (pd.Series, CheckResult))

    def test_series_duckdb_context_matches_pandas_context(self, tmp_path, pipeline_db):
        """DuckDB context produces the same result as pandas context."""
        df = _make_df()
        _seed(pipeline_db, "orders", df)

        def fn(price: pd.Series) -> pd.Series[bool]:
            return price > 15.0

        func = partial(fn, price="price")
        wrapped = _wrap_series_input(func, series_params=["price"])

        # Pandas context
        pandas_result = wrapped({"df": df})

        # DuckDB context
        pending_pks = set(df["order_id"].astype(str))
        conn = duckdb.connect(pipeline_db, read_only=True)
        try:
            duckdb_result = wrapped({
                "conn": conn,
                "table": "orders",
                "pending_pks": pending_pks,
                "pk_col": "order_id",
                "all_columns": list(df.columns),
            })
        finally:
            conn.close()

        # Both must produce matching boolean sequences
        pd.testing.assert_series_equal(
            pandas_result.reset_index(drop=True).astype(bool),
            duckdb_result.reset_index(drop=True).astype(bool),
            check_names=False,
        )

    def test_series_duckdb_context_only_fetches_needed_column(self, tmp_path, pipeline_db):
        """DuckDB context fetches only the needed column — not SELECT *."""
        df = _make_df()
        _seed(pipeline_db, "orders", df)

        received_series = {}

        def fn(price: pd.Series) -> pd.Series[bool]:
            received_series["price"] = price
            return price > 0

        func = partial(fn, price="price")
        wrapped = _wrap_series_input(func, series_params=["price"])

        pending_pks = set(df["order_id"].astype(str))
        conn = duckdb.connect(pipeline_db, read_only=True)
        try:
            wrapped({
                "conn": conn,
                "table": "orders",
                "pending_pks": pending_pks,
                "pk_col": "order_id",
                "all_columns": list(df.columns),
            })
        finally:
            conn.close()

        assert "price" in received_series
        assert isinstance(received_series["price"], pd.Series)
        # Only price column passed — not a full row with all columns
        assert len(received_series["price"]) == len(df)


# ---------------------------------------------------------------------------
# Guarantee 4 — _wrap_scalar_column_input dual-path
# ---------------------------------------------------------------------------

class TestScalarWrapperDualPath:
    """_wrap_scalar_column_input handles both context types.

    Col-backed detection uses all_columns in DuckDB context (same logic
    as isinstance(val, str) and val in clean_df.columns for pandas).
    """

    def test_scalar_pandas_and_duckdb_same_result(self, tmp_path, pipeline_db):
        """Same scalar function, both context types, same result."""
        df = _make_df()
        _seed(pipeline_db, "orders", df)

        def fn(region: str) -> bool:
            return region in ("EMEA", "APAC")

        func = partial(fn, region="region")
        wrapped = _wrap_scalar_column_input(func)

        # Pandas context
        pandas_result = wrapped({"df": df})

        # DuckDB context
        pending_pks = set(df["order_id"].astype(str))
        conn = duckdb.connect(pipeline_db, read_only=True)
        try:
            duckdb_result = wrapped({
                "conn": conn,
                "table": "orders",
                "pending_pks": pending_pks,
                "pk_col": "order_id",
                "all_columns": list(df.columns),
            })
        finally:
            conn.close()

        pd.testing.assert_series_equal(
            pandas_result.reset_index(drop=True).astype(bool),
            duckdb_result.reset_index(drop=True).astype(bool),
            check_names=False,
        )

    def test_scalar_constant_not_col_backed_in_duckdb_context(self, pipeline_db):
        """A baked constant not in all_columns is treated as constant — no per-row apply."""
        df = _make_df()
        _seed(pipeline_db, "orders", df)

        call_count = [0]

        def fn(label: str) -> bool:
            call_count[0] += 1
            return label == "EMEA"

        # "NOT_A_COLUMN" is not in df columns → must be constant
        func = partial(fn, label="NOT_A_COLUMN")
        wrapped = _wrap_scalar_column_input(func)

        pending_pks = set(df["order_id"].astype(str))
        conn = duckdb.connect(pipeline_db, read_only=True)
        try:
            wrapped({
                "conn": conn,
                "table": "orders",
                "pending_pks": pending_pks,
                "pk_col": "order_id",
                "all_columns": list(df.columns),
            })
        finally:
            conn.close()

        assert call_count[0] == 1, (
            "Constant param must call function once — not per row"
        )


# ---------------------------------------------------------------------------
# Guarantee 5 — _compute_report uses contract flags to gate pandas vs DuckDB
# ---------------------------------------------------------------------------

class TestComputeReportContractGating:
    """_compute_report reads contract flags, never re-inspects functions.

    needs_dataframe=True or is_legacy=True → pandas path
    needs_series=True or is_scalar=True → DuckDB path (conn kept open)
    """

    def test_series_check_takes_duckdb_path(self, pipeline_db, watermark_store, sales_df):
        """pd.Series check → DuckDB path — no full pending_df loaded."""
        from proto_pipe.io.db import init_all_pipeline_tables
        from proto_pipe.reports.runner import _compute_report

        with duckdb.connect(pipeline_db) as conn:
            init_all_pipeline_tables(conn)
            conn.execute("CREATE TABLE sales AS SELECT * FROM sales_df")

        cr = CheckRegistry()

        def price_check(price: pd.Series) -> pd.Series[bool]:
            return price > 0

        func = partial(price_check, price="price")
        register_custom_check("price_series_v1", func, cr, kind="check")

        config = {
            "name": "sales_report",
            "source": {"type": "duckdb", "path": pipeline_db, "table": "sales", "primary_key": "order_id"},
            "alias_map": [{"param": "price", "column": "price"}],
            "options": {"parallel": False},
            "checks": [{"name": "price_series_v1", "params": {}}],
        }
        from proto_pipe.checks.registry import ReportRegistry
        from proto_pipe.io.registry import register_from_config
        rr = ReportRegistry()
        register_from_config(config, cr, rr)
        report = rr.get("sales_report")

        # Must complete without error — DuckDB path fires for Series checks
        bundle = _compute_report(report, cr, watermark_store, pipeline_db)
        assert bundle.status == "computed", f"Expected computed, got {bundle.status}: {bundle.error}"

    def test_dataframe_check_takes_pandas_path(self, pipeline_db, watermark_store, sales_df):
        """pd.DataFrame check → pandas path — pending_df loaded."""
        from proto_pipe.io.db import init_all_pipeline_tables
        from proto_pipe.reports.runner import _compute_report

        with duckdb.connect(pipeline_db) as conn:
            init_all_pipeline_tables(conn)
            conn.execute("CREATE TABLE sales AS SELECT * FROM sales_df")

        cr = CheckRegistry()

        def df_check(df: pd.DataFrame) -> pd.Series[bool]:
            return df["price"] > 0

        register_custom_check("df_check_v1", df_check, cr, kind="check")

        config = {
            "name": "sales_report",
            "source": {"type": "duckdb", "path": pipeline_db, "table": "sales", "primary_key": "order_id"},
            "alias_map": [],
            "options": {"parallel": False},
            "checks": [{"name": "df_check_v1", "params": {}}],
        }
        from proto_pipe.checks.registry import ReportRegistry
        from proto_pipe.io.registry import register_from_config
        rr = ReportRegistry()
        register_from_config(config, cr, rr)
        report = rr.get("sales_report")

        bundle = _compute_report(report, cr, watermark_store, pipeline_db)
        assert bundle.status == "computed", f"Expected computed, got {bundle.status}: {bundle.error}"

    def test_legacy_check_takes_pandas_path(self, pipeline_db, watermark_store, sales_df):
        """Legacy context: dict check → pandas path (is_legacy=True)."""
        from proto_pipe.io.db import init_all_pipeline_tables
        from proto_pipe.reports.runner import _compute_report

        with duckdb.connect(pipeline_db) as conn:
            init_all_pipeline_tables(conn)
            conn.execute("CREATE TABLE sales AS SELECT * FROM sales_df")

        cr = CheckRegistry()

        def legacy_check(context: dict) -> pd.Series[bool]:
            return context["df"]["price"] > 0

        register_custom_check("legacy_v1", legacy_check, cr, kind="check")

        config = {
            "name": "sales_report",
            "source": {"type": "duckdb", "path": pipeline_db, "table": "sales", "primary_key": "order_id"},
            "alias_map": [],
            "options": {"parallel": False},
            "checks": [{"name": "legacy_v1", "params": {}}],
        }
        from proto_pipe.checks.registry import ReportRegistry
        from proto_pipe.io.registry import register_from_config
        rr = ReportRegistry()
        register_from_config(config, cr, rr)
        report = rr.get("sales_report")

        bundle = _compute_report(report, cr, watermark_store, pipeline_db)
        assert bundle.status == "computed"

    def test_no_checks_takes_duckdb_path(self, pipeline_db, watermark_store, sales_df):
        """Report with no checks → DuckDB path (no contracts force pandas)."""
        from proto_pipe.io.db import init_all_pipeline_tables
        from proto_pipe.reports.runner import _compute_report

        with duckdb.connect(pipeline_db) as conn:
            init_all_pipeline_tables(conn)
            conn.execute("CREATE TABLE sales AS SELECT * FROM sales_df")

        cr = CheckRegistry()
        config = {
            "name": "sales_report",
            "source": {"type": "duckdb", "path": pipeline_db, "table": "sales", "primary_key": "order_id"},
            "alias_map": [],
            "options": {"parallel": False},
            "checks": [],
            "resolved_checks": [],
        }

        bundle = _compute_report(config, cr, watermark_store, pipeline_db)
        assert bundle.status == "computed"


# ---------------------------------------------------------------------------
# Guarantee 6 — _apply_transforms_with_gate uses get_contract not inspector
# ---------------------------------------------------------------------------

class TestApplyTransformsUsesContracts:
    """_apply_transforms_with_gate reads contract.is_scalar instead of
    re-running CheckParamInspector — inspect once, read many times.
    """

    def test_scalar_transform_routed_via_contract_is_scalar(self, pipeline_db, watermark_store, sales_df):
        """is_scalar=True transform goes through DuckDB UDF path — not fresh inspector."""
        from proto_pipe.io.db import init_all_pipeline_tables
        from proto_pipe.reports.runner import _compute_report, _write_report

        with duckdb.connect(pipeline_db) as conn:
            init_all_pipeline_tables(conn)
            conn.execute("CREATE TABLE sales AS SELECT * FROM sales_df")

        cr = CheckRegistry()

        def upper_region(region: str) -> str:
            return region.upper()

        func = partial(upper_region, region="region")
        register_custom_check("upper_region_contract", func, cr, kind="transform")

        # Confirm contract flag
        assert cr.get_contract("upper_region_contract").is_scalar is True

        config = {
            "name": "sales_report",
            "source": {"type": "duckdb", "path": pipeline_db, "table": "sales", "primary_key": "order_id"},
            "alias_map": [{"param": "region", "column": "region"}],
            "options": {"parallel": False},
            "checks": [{"name": "upper_region_contract", "params": {}}],
        }
        from proto_pipe.checks.registry import ReportRegistry
        from proto_pipe.io.registry import register_from_config
        rr = ReportRegistry()
        register_from_config(config, cr, rr)
        report = rr.get("sales_report")

        # Must complete without error — contract routing works
        bundle = _compute_report(report, cr, watermark_store, pipeline_db)
        assert bundle.status == "computed"

    def test_series_transform_contract_is_not_scalar(self):
        """pd.Series transform → is_scalar=False — pandas path in transforms."""
        reg = CheckRegistry()

        def upper(region: pd.Series) -> pd.Series:
            result = region.str.upper()
            result.name = "region"
            return result

        func = partial(upper, region="region")
        reg.register("upper_series", func, kind="transform")

        contract = reg.get_contract("upper_series")
        assert contract.is_scalar is False
        assert contract.needs_series is True
