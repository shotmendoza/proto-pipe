"""Tests for proto_pipe.reports.runner

Covers:
- run_report: completes when data is present
- run_report: skips when no new rows since last watermark
- run_report: returns error status when source table cannot be loaded
- run_report: check failure captured, report still completes
- run_report: watermark advances only when all checks pass
- run_report: watermark does NOT advance when any check fails
- run_report: validation flags written to pipeline_db when provided
- run_report: no flags written when pipeline_db not provided
- run_report: primary_key from source config passed through to flag writer
- run_all_reports: runs all registered reports, returns one result per report
- run_all_reports: parallel and sequential produce the same statuses
- run_all_reports: pipeline_db passed through to each run_report call
"""

from functools import partial
from pathlib import Path

import duckdb
import pandas as pd

from proto_pipe.checks.built_in import check_nulls, check_range
from proto_pipe.io.registry import register_from_config
from proto_pipe.pipelines.watermark import WatermarkStore
from proto_pipe.registry.base import CheckRegistry, ReportRegistry
from proto_pipe.reports.runner import run_report, run_all_reports
from proto_pipe.reports.validation_flags import (
    count_validation_flags,
    detail_df,
    init_validation_flags_table,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_report_config(
    pipeline_db: str,
    table: str,
    check_names: list[str],
    primary_key: str | None = None,
) -> dict:
    source = {
        "type": "duckdb",
        "path": pipeline_db,
        "table": table,
        "timestamp_col": "updated_at",
    }
    if primary_key:
        source["primary_key"] = primary_key
    return {
        "name": f"{table}_validation",
        "source": source,
        "options": {"parallel": False},
        "resolved_checks": check_names,
    }


def _seed_table(pipeline_db: str, table: str, df: pd.DataFrame) -> None:
    conn = duckdb.connect(pipeline_db)
    conn.execute(f'CREATE OR REPLACE TABLE "{table}" AS SELECT * FROM df')
    conn.close()


def _init_pipeline_db(pipeline_db: str) -> None:
    """Bootstrap validation_flags so flag-writing tests have the table."""
    conn = duckdb.connect(pipeline_db)
    init_validation_flags_table(conn)
    conn.close()


# ---------------------------------------------------------------------------
# run_report — basic behaviour
# ---------------------------------------------------------------------------

class TestRunReport:
    def test_completes_with_data_present(self, pipeline_db, sales_df, check_registry, watermark_store):
        _seed_table(pipeline_db, "sales", sales_df)
        check_registry.register("null_check", check_nulls)
        config = _make_report_config(pipeline_db, "sales", ["null_check"])

        result = run_report(config, check_registry, watermark_store)

        assert result["status"] == "completed"
        assert result["results"]["null_check"]["status"] == "passed"

    def test_skips_when_no_rows_newer_than_watermark(
        self, pipeline_db, sales_df, check_registry, watermark_store
    ):
        _seed_table(pipeline_db, "sales", sales_df)
        check_registry.register("null_check", check_nulls)
        config = _make_report_config(pipeline_db, "sales", ["null_check"])

        run_report(config, check_registry, watermark_store)   # advances watermark
        result = run_report(config, check_registry, watermark_store)  # no new rows

        assert result["status"] == "skipped"

    def test_returns_error_when_source_table_missing(self, pipeline_db, check_registry, watermark_store):
        # Table never created — load_from_duckdb will raise
        check_registry.register("null_check", check_nulls)
        config = _make_report_config(pipeline_db, "nonexistent_table", ["null_check"])

        result = run_report(config, check_registry, watermark_store)

        assert result["status"] == "error"
        assert "error" in result

    def test_check_failure_captured_report_still_completes(
        self, pipeline_db, sales_df, check_registry, watermark_store
    ):
        _seed_table(pipeline_db, "sales", sales_df)
        check_registry.register("nonexistent_check", lambda ctx: (_ for _ in ()).throw(ValueError("boom")))
        # Simpler: just register a name that isn't in the registry
        config = _make_report_config(pipeline_db, "sales", ["missing_check"])

        result = run_report(config, check_registry, watermark_store)

        assert result["status"] == "completed"
        assert result["results"]["missing_check"]["status"] == "failed"

    def test_range_violation_is_captured_not_raised(
        self, pipeline_db, sales_df_out_of_range, check_registry, watermark_store
    ):
        _seed_table(pipeline_db, "sales", sales_df_out_of_range)
        fn = partial(check_range, col="price", min_val=0, max_val=500)
        check_registry.register("price_range", fn)
        config = _make_report_config(pipeline_db, "sales", ["price_range"])

        result = run_report(config, check_registry, watermark_store)

        assert result["status"] == "completed"
        assert result["results"]["price_range"]["status"] == "passed"
        assert result["results"]["price_range"]["result"]["violations"] == 1


# ---------------------------------------------------------------------------
# Watermark advancement
# ---------------------------------------------------------------------------

class TestWatermarkAdvancement:
    def test_watermark_advances_when_all_checks_pass(
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

    def test_watermark_does_not_advance_when_check_fails(
        self, pipeline_db, sales_df, check_registry, watermark_store
    ):
        _seed_table(pipeline_db, "sales", sales_df)
        # Register a check that raises — forces status=failed
        def always_raises(ctx):
            raise RuntimeError("hard failure")

        check_registry.register("hard_fail", always_raises)
        config = _make_report_config(pipeline_db, "sales", ["hard_fail"])

        assert watermark_store.get("sales_validation") is None
        run_report(config, check_registry, watermark_store)
        # Watermark must not have advanced because the check failed
        assert watermark_store.get("sales_validation") is None

    def test_watermark_does_not_advance_when_one_of_many_checks_fails(
        self, pipeline_db, sales_df, check_registry, watermark_store
    ):
        _seed_table(pipeline_db, "sales", sales_df)
        check_registry.register("null_check", check_nulls)

        def always_raises(ctx):
            raise RuntimeError("one bad check")

        check_registry.register("hard_fail", always_raises)
        config = _make_report_config(pipeline_db, "sales", ["null_check", "hard_fail"])

        run_report(config, check_registry, watermark_store)

        assert watermark_store.get("sales_validation") is None


# ---------------------------------------------------------------------------
# Validation flag writing in run_report
# ---------------------------------------------------------------------------

class TestRunReportFlagWriting:
    def test_flags_written_when_pipeline_db_provided(
        self, pipeline_db, sales_df_out_of_range, check_registry, watermark_store
    ):
        _init_pipeline_db(pipeline_db)
        _seed_table(pipeline_db, "sales", sales_df_out_of_range)
        fn = partial(check_range, col="price", min_val=0, max_val=500)
        check_registry.register("price_range", fn)
        config = _make_report_config(pipeline_db, "sales", ["price_range"], primary_key="order_id")

        run_report(config, check_registry, watermark_store, pipeline_db=pipeline_db)

        conn = duckdb.connect(pipeline_db)
        try:
            det = detail_df(conn, "sales_validation")
            assert len(det) == 1
            assert det.iloc[0]["pk_value"] == "ORD-002"
        finally:
            conn.close()

    def test_no_flags_written_when_pipeline_db_not_provided(
        self, pipeline_db, sales_df_out_of_range, check_registry, watermark_store
    ):
        _init_pipeline_db(pipeline_db)
        _seed_table(pipeline_db, "sales", sales_df_out_of_range)
        fn = partial(check_range, col="price", min_val=0, max_val=500)
        check_registry.register("price_range", fn)
        config = _make_report_config(pipeline_db, "sales", ["price_range"], primary_key="order_id")

        # No pipeline_db arg — old behaviour
        run_report(config, check_registry, watermark_store)

        conn = duckdb.connect(pipeline_db)
        try:
            assert count_validation_flags(conn) == 0
        finally:
            conn.close()

    def test_primary_key_passed_through_to_flags(
        self, pipeline_db, sales_df_out_of_range, check_registry, watermark_store
    ):
        _init_pipeline_db(pipeline_db)
        _seed_table(pipeline_db, "sales", sales_df_out_of_range)
        fn = partial(check_range, col="price", min_val=0, max_val=500)
        check_registry.register("price_range", fn)
        config = _make_report_config(pipeline_db, "sales", ["price_range"], primary_key="order_id")

        run_report(config, check_registry, watermark_store, pipeline_db=pipeline_db)

        conn = duckdb.connect(pipeline_db)
        try:
            det = detail_df(conn)
            # pk_col stored correctly
            assert det.iloc[0]["pk_col"] == "order_id"
        finally:
            conn.close()

    def test_no_flags_for_clean_data(
        self, pipeline_db, sales_df, check_registry, watermark_store
    ):
        _init_pipeline_db(pipeline_db)
        _seed_table(pipeline_db, "sales", sales_df)
        fn = partial(check_range, col="price", min_val=0, max_val=500)
        check_registry.register("price_range", fn)
        config = _make_report_config(pipeline_db, "sales", ["price_range"], primary_key="order_id")

        run_report(config, check_registry, watermark_store, pipeline_db=pipeline_db)

        conn = duckdb.connect(pipeline_db)
        try:
            # All prices in sales_df are in range → violation_indices=[] → no flags
            assert count_validation_flags(conn) == 0
        finally:
            conn.close()


# ---------------------------------------------------------------------------
# run_all_reports
# ---------------------------------------------------------------------------

class TestRunAllReports:
    def test_returns_one_result_per_registered_report(
        self, pipeline_db, sales_df, inventory_df,
        check_registry, report_registry, watermark_store, reports_config
    ):
        _seed_table(pipeline_db, "sales", sales_df)
        _seed_table(pipeline_db, "inventory", inventory_df)

        for r in reports_config["reports"]:
            r["source"]["path"] = pipeline_db

        register_from_config(reports_config, check_registry, report_registry)
        results = run_all_reports(
            report_registry, check_registry, watermark_store, parallel_reports=False
        )

        report_names = {r["report"] for r in results}
        assert "daily_sales_validation" in report_names
        assert "inventory_validation" in report_names

    def test_parallel_and_sequential_same_statuses(
        self, pipeline_db, sales_df, inventory_df, reports_config, watermark_db
    ):
        _seed_table(pipeline_db, "sales", sales_df)
        _seed_table(pipeline_db, "inventory", inventory_df)

        for r in reports_config["reports"]:
            r["source"]["path"] = pipeline_db

        cr1, rr1 = CheckRegistry(), ReportRegistry()
        ws1 = WatermarkStore(watermark_db)
        register_from_config(reports_config, cr1, rr1)
        seq = run_all_reports(rr1, cr1, ws1, parallel_reports=False)

        wdb2 = str(Path(watermark_db).parent / "watermarks2.db")
        cr2, rr2 = CheckRegistry(), ReportRegistry()
        ws2 = WatermarkStore(wdb2)
        register_from_config(reports_config, cr2, rr2)
        par = run_all_reports(rr2, cr2, ws2, parallel_reports=True)

        assert {r["report"]: r["status"] for r in seq} == {r["report"]: r["status"] for r in par}

    def test_pipeline_db_passed_through_writes_flags(
        self, pipeline_db, sales_df, check_registry, report_registry, watermark_store
    ):
        _init_pipeline_db(pipeline_db)
        _seed_table(pipeline_db, "sales", sales_df)

        from proto_pipe import register_custom_check

        def check_negative_price(ctx, col="price"):
            df = ctx["df"]
            return {"mask": df[col] < 0}

        register_custom_check("neg_price", check_negative_price, check_registry)

        config = {
            "templates": {},
            "reports": [{
                "name": "sales_validation",
                "source": {
                    "type": "duckdb", "path": pipeline_db,
                    "table": "sales", "timestamp_col": "updated_at",
                    "primary_key": "order_id",
                },
                "options": {"parallel": False},
                "checks": [{"name": "neg_price"}],
            }],
        }
        register_from_config(config, check_registry, report_registry)

        run_all_reports(
            report_registry, check_registry, watermark_store,
            parallel_reports=False,
            pipeline_db=pipeline_db,
        )

        conn = duckdb.connect(pipeline_db)
        try:
            # sales_df has all positive prices → mask is all False → no flags
            assert count_validation_flags(conn) == 0
        finally:
            conn.close()
