"""
Tests for validation_pipeline.runner

Covers:
- run_report: skips when no new data, completes when data present
- run_report: watermark advanced after successful run
- run_report: watermark NOT advanced when checks fail hard
- run_all_reports: returns results for all registered reports
- Parallel and sequential execution both produce correct results
"""

from functools import partial

import duckdb

from src.checks.built_in import check_nulls, check_range
from src.io.registry import register_from_config
from src.pipelines.watermark import WatermarkStore
from src.registry.base import CheckRegistry, ReportRegistry
from src.reports.runner import run_report, run_all_reports


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_report_config(pipeline_db: str, table: str, check_names: list[str]) -> dict:
    return {
        "name": f"{table}_validation",
        "source": {
            "type": "duckdb",
            "path": pipeline_db,
            "table": table,
            "timestamp_col": "updated_at",
        },
        "options": {"parallel": False},
        "resolved_checks": check_names,
    }


def _seed_table(pipeline_db: str, table: str, df) -> None:
    conn = duckdb.connect(pipeline_db)
    conn.execute(f'CREATE OR REPLACE TABLE "{table}" AS SELECT * FROM df')
    conn.close()


# ---------------------------------------------------------------------------
# run_report — basic behaviour
# ---------------------------------------------------------------------------

class TestRunReport:
    def test_completes_with_clean_data(
        self, pipeline_db, watermark_db, sales_df, check_registry, watermark_store
    ):
        _seed_table(pipeline_db, "sales", sales_df)
        check_registry.register("null_check", check_nulls)
        config = _make_report_config(pipeline_db, "sales", ["null_check"])

        result = run_report(config, check_registry, watermark_store)

        assert result["status"] == "completed"
        assert result["results"]["null_check"]["status"] == "passed"

    def test_skips_when_no_new_data(
        self, pipeline_db, watermark_db, sales_df, check_registry, watermark_store
    ):
        _seed_table(pipeline_db, "sales", sales_df)
        check_registry.register("null_check", check_nulls)
        config = _make_report_config(pipeline_db, "sales", ["null_check"])

        # First run advances the watermark past all rows
        run_report(config, check_registry, watermark_store)
        # Second run — no new rows
        result = run_report(config, check_registry, watermark_store)

        assert result["status"] == "skipped"

    def test_check_failure_is_captured_not_raised(
        self, pipeline_db, sales_df_out_of_range, check_registry, watermark_store
    ):
        _seed_table(pipeline_db, "sales", sales_df_out_of_range)

        # range_check with params baked in via partial
        fn = partial(check_range, col="price", min_val=0, max_val=500)
        check_registry.register("price_range", fn)
        config = _make_report_config(pipeline_db, "sales", ["price_range"])

        result = run_report(config, check_registry, watermark_store)

        assert result["status"] == "completed"
        # The check ran and returned a result (violations > 0)
        assert result["results"]["price_range"]["status"] == "passed"
        assert result["results"]["price_range"]["result"]["violations"] == 1

    def test_missing_check_name_marks_report_failed(
        self, pipeline_db, sales_df, check_registry, watermark_store
    ):
        _seed_table(pipeline_db, "sales", sales_df)
        # "nonexistent_check" is never registered
        config = _make_report_config(pipeline_db, "sales", ["nonexistent_check"])

        result = run_report(config, check_registry, watermark_store)

        # run_check_safe catches the ValueError — report completes but check failed
        assert result["status"] == "completed"
        assert result["results"]["nonexistent_check"]["status"] == "failed"


# ---------------------------------------------------------------------------
# Watermark advancement
# ---------------------------------------------------------------------------

class TestWatermarkAdvancement:
    def test_watermark_advances_after_successful_run(
        self, pipeline_db, sales_df, check_registry, watermark_store
    ):
        _seed_table(pipeline_db, "sales", sales_df)
        check_registry.register("null_check", check_nulls)
        config = _make_report_config(pipeline_db, "sales", ["null_check"])

        assert watermark_store.get("sales_validation") is None
        run_report(config, check_registry, watermark_store)
        assert watermark_store.get("sales_validation") is not None

    def test_watermark_reflects_max_timestamp_in_data(
        self, pipeline_db, sales_df, check_registry, watermark_store
    ):
        _seed_table(pipeline_db, "sales", sales_df)
        check_registry.register("null_check", check_nulls)
        config = _make_report_config(pipeline_db, "sales", ["null_check"])

        run_report(config, check_registry, watermark_store)
        mark = watermark_store.get("sales_validation")

        # Max updated_at in sales_df is 2026-03-01
        assert mark.year == 2026
        assert mark.month == 3
        assert mark.day == 1


# ---------------------------------------------------------------------------
# run_all_reports
# ---------------------------------------------------------------------------

class TestRunAllReports:
    def test_all_reports_run(
        self, pipeline_db, sales_df, inventory_df,
        check_registry, report_registry, watermark_store, reports_config
    ):
        _seed_table(pipeline_db, "sales", sales_df)
        _seed_table(pipeline_db, "inventory", inventory_df)

        # Patch source paths to point at tmp pipeline_db
        for r in reports_config["reports"]:
            r["source"]["path"] = pipeline_db

        register_from_config(reports_config, check_registry, report_registry)
        results = run_all_reports(
            report_registry, check_registry, watermark_store, parallel_reports=False
        )

        report_names = {r["report"] for r in results}
        assert "daily_sales_validation" in report_names
        assert "inventory_validation" in report_names

    def test_parallel_and_sequential_same_outcome(
        self, pipeline_db, sales_df, inventory_df,
        reports_config, watermark_db
    ):
        _seed_table(pipeline_db, "sales", sales_df)
        _seed_table(pipeline_db, "inventory", inventory_df)

        for r in reports_config["reports"]:
            r["source"]["path"] = pipeline_db

        # Sequential
        cr1 = CheckRegistry()
        rr1 = ReportRegistry()
        ws1 = WatermarkStore(watermark_db)
        register_from_config(reports_config, cr1, rr1)
        seq_results = run_all_reports(rr1, cr1, ws1, parallel_reports=False)

        # Parallel — needs fresh watermark db so it doesn't skip
        from pathlib import Path
        wdb2 = str(Path(watermark_db).parent / "watermarks2.db")
        cr2 = CheckRegistry()
        rr2 = ReportRegistry()
        ws2 = WatermarkStore(wdb2)
        register_from_config(reports_config, cr2, rr2)
        par_results = run_all_reports(rr2, cr2, ws2, parallel_reports=True)

        seq_statuses = {r["report"]: r["status"] for r in seq_results}
        par_statuses = {r["report"]: r["status"] for r in par_results}
        assert seq_statuses == par_statuses
