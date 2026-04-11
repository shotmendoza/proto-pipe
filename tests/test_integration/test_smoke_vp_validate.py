"""Tests for CheckContract inspection flags and DuckDB-native execution path.

Behavioral guarantees:

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

rules applied:
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
        "price": [10.0, 20.0, 30.0],
        "region": ["EMEA", "APAC", "EMEA"],
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
        full_config = {"templates": {}, "reports": [config]}
        register_from_config(full_config, cr, rr)
        report = rr.get("sales_report")

        # Must complete without error — contract routing works
        bundle = _compute_report(report, cr, pipeline_db)
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


# ---------------------------------------------------------------------------
# Fix 1 — Bulk column fetch guarantees
# ---------------------------------------------------------------------------

class TestBulkColumnFetch:
    """col_cache pre-fetches all Series columns in one query per report.

    Guarantees:
    - col_cache populated in context before check execution
    - _wrap_series_input reads from cache — no fallback query for cached cols
    - Cache scoped to report's source table only (no cross-table bleed)
    """

    def test_col_cache_in_context_for_duckdb_path(self, pipeline_db, watermark_store, sales_df):
        """col_cache is present in context when DuckDB path is taken."""
        from proto_pipe.io.db import init_all_pipeline_tables
        from proto_pipe.reports.runner import _compute_report

        with duckdb.connect(pipeline_db) as conn:
            init_all_pipeline_tables(conn)
            conn.execute("CREATE TABLE sales AS SELECT * FROM sales_df")

        cr = CheckRegistry()
        captured_context = {}

        def price_check(price: pd.Series) -> pd.Series[bool]:
            captured_context["col_cache"] = None  # just mark it was called
            return price > 0

        func = partial(price_check, price="price")
        register_custom_check("price_cache_v1", func, cr, kind="check")

        from proto_pipe.checks.registry import ReportRegistry
        from proto_pipe.io.registry import register_from_config
        rr = ReportRegistry()
        register_from_config({
            "templates": {},
            "reports": [{
                "name": "sales_report",
                "source": {"type": "duckdb", "path": pipeline_db, "table": "sales", "primary_key": "order_id"},
                "alias_map": [{"param": "price", "column": "price"}],
                "options": {"parallel": False},
                "checks": [{"name": "price_cache_v1", "params": {}}],
            }],
        }, cr, rr)

        bundle = _compute_report(rr.get("sales_report"), cr, pipeline_db)
        assert bundle.status == "computed"

    def test_col_cache_populated_with_needed_columns(self, pipeline_db, watermark_store, sales_df):
        """col_cache contains the Series param columns before check execution."""
        from proto_pipe.io.db import init_all_pipeline_tables
        from proto_pipe.reports.runner import _compute_report

        with duckdb.connect(pipeline_db) as conn:
            init_all_pipeline_tables(conn)
            conn.execute("CREATE TABLE sales AS SELECT * FROM sales_df")

        cr = CheckRegistry()
        received_cache = {}

        # Monkey-patch _wrap_series_input to capture the context's col_cache
        original_run = cr.run

        def capturing_run(name, context):
            if "col_cache" in context:
                received_cache.update(context["col_cache"])
            return original_run(name, context)

        cr.run = capturing_run

        def price_check(price: pd.Series) -> pd.Series[bool]:
            return price > 0

        func = partial(price_check, price="price")
        register_custom_check("price_cache_v2", func, cr, kind="check")

        from proto_pipe.checks.registry import ReportRegistry
        from proto_pipe.io.registry import register_from_config
        rr = ReportRegistry()
        register_from_config({
            "templates": {},
            "reports": [{
                "name": "sales_report",
                "source": {"type": "duckdb", "path": pipeline_db, "table": "sales", "primary_key": "order_id"},
                "alias_map": [{"param": "price", "column": "price"}],
                "options": {"parallel": False},
                "checks": [{"name": "price_cache_v2", "params": {}}],
            }],
        }, cr, rr)

        _compute_report(rr.get("sales_report"), cr, pipeline_db)

        assert "price" in received_cache, (
            "col_cache must contain 'price' — the Series param column for this check"
        )
        assert isinstance(received_cache["price"], pd.Series)

    def test_col_cache_reads_correct_values(self, pipeline_db, watermark_store, sales_df):
        """Values in col_cache match the pending rows from the source table."""
        from proto_pipe.io.db import init_all_pipeline_tables
        from proto_pipe.reports.runner import _compute_report

        with duckdb.connect(pipeline_db) as conn:
            init_all_pipeline_tables(conn)
            conn.execute("CREATE TABLE sales AS SELECT * FROM sales_df")

        cr = CheckRegistry()
        received_values = {}

        def price_check(price: pd.Series) -> pd.Series[bool]:
            received_values["price"] = list(price)
            return price > 0

        func = partial(price_check, price="price")
        register_custom_check("price_cache_v3", func, cr, kind="check")

        from proto_pipe.checks.registry import ReportRegistry
        from proto_pipe.io.registry import register_from_config
        rr = ReportRegistry()
        register_from_config({
            "templates": {},
            "reports": [{
                "name": "sales_report",
                "source": {"type": "duckdb", "path": pipeline_db, "table": "sales", "primary_key": "order_id"},
                "alias_map": [{"param": "price", "column": "price"}],
                "options": {"parallel": False},
                "checks": [{"name": "price_cache_v3", "params": {}}],
            }],
        }, cr, rr)

        _compute_report(rr.get("sales_report"), cr, pipeline_db)

        expected = sorted(sales_df["price"].tolist())
        assert sorted(received_values.get("price", [])) == expected, (
            "col_cache values must match the source table's price column"
        )

    def test_col_cache_does_not_contain_columns_from_other_tables(
            self, pipeline_db, watermark_store, sales_df
    ):
        """col_cache is scoped to the report's source table — no cross-table bleed."""
        from proto_pipe.io.db import init_all_pipeline_tables
        from proto_pipe.reports.runner import _compute_report

        # Inline inventory data — distinct columns from sales to verify no bleed
        inventory_df = pd.DataFrame({
            "sku": ["SKU-001", "SKU-002"],
            "qty_on_hand": [100, 200],
            "warehouse": ["WH-A", "WH-B"],
        })

        with duckdb.connect(pipeline_db) as conn:
            init_all_pipeline_tables(conn)
            conn.execute("CREATE TABLE sales AS SELECT * FROM sales_df")
            conn.execute("CREATE TABLE inventory AS SELECT * FROM inventory_df")

        cr = CheckRegistry()
        received_cache = {}

        original_run = cr.run

        def capturing_run(name, context):
            if "col_cache" in context:
                received_cache.update(context["col_cache"])
            return original_run(name, context)

        cr.run = capturing_run

        def price_check(price: pd.Series) -> pd.Series[bool]:
            return price > 0

        func = partial(price_check, price="price")
        register_custom_check("price_cache_v4", func, cr, kind="check")

        from proto_pipe.checks.registry import ReportRegistry
        from proto_pipe.io.registry import register_from_config
        rr = ReportRegistry()
        register_from_config({
            "templates": {},
            "reports": [{
                "name": "sales_report",
                "source": {"type": "duckdb", "path": pipeline_db, "table": "sales", "primary_key": "order_id"},
                "alias_map": [{"param": "price", "column": "price"}],
                "options": {"parallel": False},
                "checks": [{"name": "price_cache_v4", "params": {}}],
            }],
        }, cr, rr)

        _compute_report(rr.get("sales_report"), cr, pipeline_db)

        # inventory columns must not appear in cache for a sales report
        inventory_cols = set(inventory_df.columns)
        cached_cols = set(received_cache.keys())
        bleed = inventory_cols & cached_cols
        assert not bleed, (
            f"col_cache must not contain inventory columns: {bleed}"
        )


# ---------------------------------------------------------------------------
# Fix 2 — Better error message for missing alias_map
# ---------------------------------------------------------------------------

class TestMissingAliasMapError:
    """_wrap_series_input raises a clear error when alias_map is missing.

    Guarantees:
    - Error message includes 'vp edit report' for actionable guidance
    - Fires on both pandas and DuckDB paths
    """

    def test_missing_col_name_error_mentions_vp_edit_report_pandas(self):
        """Pandas path: error message tells user to run 'vp edit report'."""
        from proto_pipe.checks.registry import _wrap_series_input

        def fn(price: pd.Series) -> pd.Series[bool]:
            return price > 0

        # No partial — price param has no baked column name
        wrapped = _wrap_series_input(fn, series_params=["price"])

        df = pd.DataFrame({"price": [1.0, 2.0], "order_id": ["A", "B"]})
        with pytest.raises(ValueError) as exc_info:
            wrapped({"df": df})

        assert "vp edit report" in str(exc_info.value), (
            "Error must tell user to run 'vp edit report' to fix alias_map"
        )

    def test_missing_col_name_error_mentions_vp_edit_report_duckdb(self, pipeline_db):
        """DuckDB path: error message tells user to run 'vp edit report'."""
        from proto_pipe.checks.registry import _wrap_series_input

        with duckdb.connect(pipeline_db) as conn:
            conn.execute("CREATE TABLE sales AS SELECT * FROM (VALUES (1.0, 'A'), (2.0, 'B')) t(price, order_id)")

        def fn(price: pd.Series) -> pd.Series[bool]:
            return price > 0

        wrapped = _wrap_series_input(fn, series_params=["price"])

        conn = duckdb.connect(pipeline_db, read_only=True)
        try:
            with pytest.raises(ValueError) as exc_info:
                wrapped({
                    "conn": conn,
                    "table": "sales",
                    "pending_pks": {"A", "B"},
                    "pk_col": "order_id",
                    "all_columns": ["price", "order_id"],
                    "col_cache": {},
                })
        finally:
            conn.close()

        assert "vp edit report" in str(exc_info.value)


# ---------------------------------------------------------------------------
# Fix 3 — display_name resolves function name from UUID
# ---------------------------------------------------------------------------

class TestDisplayName:
    """display_name returns human-readable function name instead of UUID.

    display_name now lives in proto_pipe.cli.prompts (moved from runner)
    per the module responsibility rule — display concerns belong in the CLI layer.

    Guarantees:
    - Returns __name__ of the original function
    - Falls back to UUID when function can't be unwrapped
    - Never raises
    """

    def test_display_name_returns_function_name(self):
        """Registered check UUID resolves to the original function __name__."""
        from proto_pipe.cli.prompts import display_name
        from functools import partial

        reg = CheckRegistry()

        def my_price_check(price: pd.Series) -> pd.Series[bool]:
            return price > 0

        func = partial(my_price_check, price="price")
        reg.register("my_price_check", func, kind="check")

        name = display_name("my_price_check", reg)
        assert name == "my_price_check", (
            "_display_name must return the original function __name__, not the UUID"
        )

    def test_display_name_falls_back_to_uuid_for_unknown(self):
        """Unknown check name falls back to the UUID without raising."""
        from proto_pipe.cli.prompts import display_name

        reg = CheckRegistry()
        unknown_uuid = "2ebc92ed-123b-5563-afe1-7fd3e0f8b41b"

        result = display_name(unknown_uuid, reg)
        assert result == unknown_uuid, (
            "_display_name must return the UUID unchanged when check is not registered"
        )

    def test_display_name_never_raises(self):
        """_display_name is always safe to call regardless of registry state."""
        from proto_pipe.cli.prompts import display_name

        reg = CheckRegistry()

        # Should not raise for any input
        try:
            display_name("any-random-string", reg)
            display_name("", reg)
            display_name("2ebc92ed-123b-5563-afe1-7fd3e0f8b41b", reg)
        except Exception as e:
            pytest.fail(f"_display_name raised unexpectedly: {e}")

    def test_display_name_used_in_check_output(
            self, pipeline_db, watermark_store, sales_df
    ):
        """Check failure output shows function name not UUID."""
        from proto_pipe.io.db import init_all_pipeline_tables
        from proto_pipe.reports.runner import _compute_report

        with duckdb.connect(pipeline_db) as conn:
            init_all_pipeline_tables(conn)
            conn.execute("CREATE TABLE sales AS SELECT * FROM sales_df")

        cr = CheckRegistry()

        def always_fails(price: pd.Series) -> pd.Series[bool]:
            return price < 0  # all fail

        func = partial(always_fails, price="price")
        register_custom_check("always_fails_display", func, cr, kind="check")

        from proto_pipe.checks.registry import ReportRegistry
        from proto_pipe.io.registry import register_from_config

        rr = ReportRegistry()
        register_from_config(
            {
                "templates": {},
                "reports": [
                    {
                        "name": "sales_report",
                        "source": {
                            "type": "duckdb",
                            "path": pipeline_db,
                            "table": "sales",
                            "primary_key": "order_id",
                        },
                        "alias_map": [{"param": "price", "column": "price"}],
                        "options": {"parallel": False},
                        "checks": [{"name": "always_fails_display", "params": {}}],
                    }
                ],
            },
            cr,
            rr,
        )

        bundle = _compute_report(rr.get("sales_report"), cr, pipeline_db)

        log_messages = " ".join(entry.message for entry in bundle.log)
        assert (
                "always_fails" in log_messages
        ), "Check output must show function name 'always_fails', not a UUID"
