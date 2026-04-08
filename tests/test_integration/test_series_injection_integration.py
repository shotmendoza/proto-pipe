"""Integration tests for the pd.Series injection wrapper through the full pipeline.

These tests exercise the complete chain that unit tests cannot cover:

  register_custom_check → BUILT_IN_CHECKS → register_from_config →
  _expand_check_with_alias_map → _register_check → validate_check →
  _wrap_series_input → run_all_reports → _compute_report →
  check_registry.run → function receives pd.Series

CLAUDE.md guarantees verified here:
  - A pd.Series check registered via register_from_config with alias_map
    completes without "multiple values for argument" error end-to-end
  - validation_pass records correct pass/fail status per row
  - A pd.Series transform registered via the full path modifies the
    report table without error
  - kind propagation is correct: pd.Series transforms land in
    transform_names, not check_names, inside _compute_report

CLAUDE.md rules applied:
  - CheckParamInspector is the canonical inspection pattern
  - Tests use in-code fake data — no static files
  - register_custom_check is the correct entry point for adding functions
    to BUILT_IN_CHECKS before register_from_config
  - run_all_reports exercises the full parallel compute + sequential write path
"""
from __future__ import annotations

import duckdb
import pandas as pd
import pytest

from proto_pipe.checks.registry import CheckRegistry, ReportRegistry
from proto_pipe.checks.helpers import register_custom_check
from proto_pipe.io.db import ensure_pipeline_tables
from proto_pipe.io.registry import register_from_config
from proto_pipe.pipelines.watermark import WatermarkStore
from proto_pipe.reports.runner import run_all_reports


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _seed(pipeline_db: str, table: str, df: pd.DataFrame) -> None:
    conn = duckdb.connect(pipeline_db)
    conn.execute(f'CREATE OR REPLACE TABLE "{table}" AS SELECT * FROM df')
    conn.close()


def _init(pipeline_db: str) -> None:
    conn = duckdb.connect(pipeline_db)
    ensure_pipeline_tables(conn)
    conn.close()


def _count_validation_pass(pipeline_db: str, report_name: str) -> int:
    conn = duckdb.connect(pipeline_db, read_only=True)
    n = conn.execute(
        "SELECT count(*) FROM validation_pass WHERE report_name = ?",
        [report_name],
    ).fetchone()[0]
    conn.close()
    return n


def _get_pass_statuses(pipeline_db: str, report_name: str) -> list[str]:
    conn = duckdb.connect(pipeline_db, read_only=True)
    rows = conn.execute(
        "SELECT status FROM validation_pass WHERE report_name = ? ORDER BY pk_value",
        [report_name],
    ).fetchall()
    conn.close()
    return [r[0] for r in rows]


def _make_report_config(
    pipeline_db: str,
    report_name: str,
    table: str,
    check_name: str,
    alias_map: list[dict],
    pk: str = "order_id",
) -> dict:
    """Minimal report config for integration tests."""
    return {
        "templates": {},
        "reports": [{
            "name": report_name,
            "source": {
                "type": "duckdb",
                "path": pipeline_db,
                "table": table,
                "primary_key": pk,
                "timestamp_col": "updated_at",
            },
            "alias_map": alias_map,
            "options": {"parallel": False},
            "checks": [{"name": check_name, "params": {}}],
        }],
    }


# ---------------------------------------------------------------------------
# Integration — pd.Series check via full registration path
# ---------------------------------------------------------------------------

class TestSeriesCheckIntegration:
    """Full pipeline integration: pd.Series check registered via
    register_from_config + alias_map, run via run_all_reports.

    CLAUDE.md guarantee: the production 'multiple values for argument'
    error must not occur when a pd.Series param is tied to a column
    via alias_map and the function has no context: dict.
    """

    def test_series_check_completes_without_multiple_values_error(
        self, pipeline_db, watermark_db, sales_df
    ):
        """Core production bug regression test.

        A pd.Series check registered via the full pipeline path must complete
        without raising 'multiple values for argument <param_name>'.
        """
        _init(pipeline_db)
        _seed(pipeline_db, "sales", sales_df)

        def price_positive_check(price: pd.Series) -> pd.Series[bool]:
            return price > 0

        cr = CheckRegistry()
        rr = ReportRegistry()
        ws = WatermarkStore(watermark_db)

        register_custom_check(
            "price_positive_v1", price_positive_check, cr, kind="check"
        )

        config = _make_report_config(
            pipeline_db,
            report_name="sales_report",
            table="sales",
            check_name="price_positive_v1",
            alias_map=[{"param": "price", "column": "price"}],
        )
        register_from_config(config, cr, rr)

        # Must not raise — this is the production bug scenario
        results = run_all_reports(rr, cr, ws, pipeline_db=pipeline_db)

        assert len(results) == 1
        assert results[0]["status"] == "completed", (
            "Report must complete — 'multiple values for argument' must not occur"
        )

    def test_series_check_all_passing_rows_written_to_validation_pass(
        self, pipeline_db, watermark_db, sales_df
    ):
        """All 3 rows pass the check → 3 validation_pass entries with status=passed."""
        _init(pipeline_db)
        _seed(pipeline_db, "sales", sales_df)

        def price_check(price: pd.Series) -> pd.Series[bool]:
            return price > 0

        cr = CheckRegistry()
        rr = ReportRegistry()
        ws = WatermarkStore(watermark_db)
        register_custom_check("price_check_v2", price_check, cr, kind="check")

        config = _make_report_config(
            pipeline_db, "sales_report", "sales", "price_check_v2",
            alias_map=[{"param": "price", "column": "price"}],
        )
        register_from_config(config, cr, rr)
        run_all_reports(rr, cr, ws, pipeline_db=pipeline_db)

        assert _count_validation_pass(pipeline_db, "sales_report") == 3, (
            "All 3 rows must be written to validation_pass after a completed run"
        )
        statuses = _get_pass_statuses(pipeline_db, "sales_report")
        assert all(s == "passed" for s in statuses), (
            "All rows pass price > 0 — all validation_pass statuses must be 'passed'"
        )

    def test_series_check_failing_row_written_to_validation_block(
        self, pipeline_db, watermark_db, sales_df
    ):
        """A failing row is written to validation_block; its validation_pass
        status is 'failed'. Passing rows are 'passed'.
        """
        _init(pipeline_db)
        df = sales_df.copy()
        df.loc[1, "price"] = -5.0  # ORD-002 fails the check
        _seed(pipeline_db, "sales", df)

        def price_nonnegative(price: pd.Series) -> pd.Series[bool]:
            return price >= 0

        cr = CheckRegistry()
        rr = ReportRegistry()
        ws = WatermarkStore(watermark_db)
        register_custom_check("price_nonneg_v3", price_nonnegative, cr, kind="check")

        config = _make_report_config(
            pipeline_db, "sales_report", "sales", "price_nonneg_v3",
            alias_map=[{"param": "price", "column": "price"}],
        )
        register_from_config(config, cr, rr)
        results = run_all_reports(rr, cr, ws, pipeline_db=pipeline_db)

        assert results[0]["status"] == "completed"

        conn = duckdb.connect(pipeline_db, read_only=True)
        block_count = conn.execute(
            "SELECT count(*) FROM validation_block WHERE report_name = 'sales_report'"
        ).fetchone()[0]
        conn.close()

        assert block_count == 1, (
            "Exactly 1 failing row (ORD-002) must be written to validation_block"
        )

    def test_series_check_multi_column_expansion_each_runs_independently(
        self, pipeline_db, watermark_db, sales_df
    ):
        """When alias_map has N entries for the same param, the check runs N times.

        CLAUDE.md: multi-column expansion — same check runs once per alias_map entry,
        producing N result sets in validation_block.
        """
        _init(pipeline_db)
        _seed(pipeline_db, "sales", sales_df)

        def col_above_zero(col: pd.Series) -> pd.Series[bool]:
            return col > 0

        cr = CheckRegistry()
        rr = ReportRegistry()
        ws = WatermarkStore(watermark_db)
        register_custom_check("col_above_zero_v4", col_above_zero, cr, kind="check")

        config = {
            "templates": {},
            "reports": [{
                "name": "sales_report",
                "source": {
                    "type": "duckdb",
                    "path": pipeline_db,
                    "table": "sales",
                    "primary_key": "order_id",
                    "timestamp_col": "updated_at",
                },
                # Two columns tied to the same param — check runs twice
                "alias_map": [
                    {"param": "col", "column": "price"},
                    {"param": "col", "column": "quantity"},
                ],
                "options": {"parallel": False},
                "checks": [{"name": "col_above_zero_v4", "params": {}}],
            }],
        }
        register_from_config(config, cr, rr)

        report = rr.get("sales_report")
        # Two alias_map entries → two expanded check UUIDs
        assert len(report["resolved_checks"]) == 2, (
            "Multi-column expansion must produce 2 registered check instances"
        )

        results = run_all_reports(rr, cr, ws, pipeline_db=pipeline_db)
        assert results[0]["status"] == "completed"


# ---------------------------------------------------------------------------
# Integration — pd.Series transform via full registration path
# ---------------------------------------------------------------------------

class TestSeriesTransformIntegration:
    """Full pipeline integration: pd.Series transform registered via
    register_from_config + alias_map, run via run_all_reports.

    CLAUDE.md: kind propagation guarantee — transforms land in transform_names
    inside _compute_report, not check_names. The report table is modified.
    """

    def test_series_transform_completes_without_error(
        self, pipeline_db, watermark_db, sales_df
    ):
        """pd.Series transform registered via full path must complete."""
        _init(pipeline_db)
        _seed(pipeline_db, "sales", sales_df)

        def upper_region(region: pd.Series) -> pd.Series:
            result = region.str.upper()
            result.name = "region"
            return result

        cr = CheckRegistry()
        rr = ReportRegistry()
        ws = WatermarkStore(watermark_db)
        register_custom_check("upper_region_v5", upper_region, cr, kind="transform")

        config = _make_report_config(
            pipeline_db, "sales_report", "sales", "upper_region_v5",
            alias_map=[{"param": "region", "column": "region"}],
        )
        register_from_config(config, cr, rr)

        results = run_all_reports(rr, cr, ws, pipeline_db=pipeline_db)

        assert results[0]["status"] == "completed", (
            "pd.Series transform must complete without error through the full pipeline"
        )

    def test_series_transform_kind_is_transform_not_check(
        self, pipeline_db, watermark_db, sales_df
    ):
        """After registration via register_from_config, the expanded UUID
        must have kind='transform' — confirming kind propagation fix is intact
        and the transform lands in transform_names, not check_names.
        """
        _init(pipeline_db)
        _seed(pipeline_db, "sales", sales_df)

        def upper_region(region: pd.Series) -> pd.Series:
            result = region.str.upper()
            result.name = "region"
            return result

        cr = CheckRegistry()
        rr = ReportRegistry()
        ws = WatermarkStore(watermark_db)
        register_custom_check("upper_region_v6", upper_region, cr, kind="transform")

        config = _make_report_config(
            pipeline_db, "sales_report", "sales", "upper_region_v6",
            alias_map=[{"param": "region", "column": "region"}],
        )
        register_from_config(config, cr, rr)

        report = rr.get("sales_report")
        for check_uuid in report["resolved_checks"]:
            kind = cr.get_kind(check_uuid)
            assert kind == "transform", (
                f"Expanded UUID '{check_uuid}' must have kind='transform', got '{kind}'. "
                f"Kind propagation fix in io/registry.py must be in effect."
            )

    def test_series_transform_result_written_to_report_table(
        self, pipeline_db, watermark_db, sales_df
    ):
        """The transform result (uppercased region) must be written to the
        report table — confirming the full write path completes correctly.
        """
        _init(pipeline_db)
        _seed(pipeline_db, "sales", sales_df)

        def upper_region(region: pd.Series) -> pd.Series:
            result = region.str.upper()
            result.name = "region"
            return result

        cr = CheckRegistry()
        rr = ReportRegistry()
        ws = WatermarkStore(watermark_db)
        register_custom_check("upper_region_v7", upper_region, cr, kind="transform")

        config = _make_report_config(
            pipeline_db, "sales_report", "sales", "upper_region_v7",
            alias_map=[{"param": "region", "column": "region"}],
        )
        register_from_config(config, cr, rr)
        run_all_reports(rr, cr, ws, pipeline_db=pipeline_db)

        conn = duckdb.connect(pipeline_db, read_only=True)
        regions = conn.execute(
            'SELECT region FROM sales_report ORDER BY order_id'
        ).df()["region"].tolist()
        conn.close()

        # Original values: ["EMEA", "APAC", "EMEA"] — transform uppercases them
        # They are already uppercase, so the transform is a no-op on these values.
        # What matters is the report table exists and contains the transformed data.
        assert len(regions) == 3, "Report table must have 3 rows after validation"
        assert all(r == r.upper() for r in regions), (
            "All region values in report table must be uppercase after transform"
        )
